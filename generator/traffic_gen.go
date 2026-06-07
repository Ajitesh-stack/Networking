package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PacketJob holds the telemetry payload and its associated pacing sleep duration.
type PacketJob struct {
	Payload       string
	SleepDuration time.Duration
}

func main() {
	var dataPath string
	flag.StringVar(&dataPath, "data", "generator/data/train.csv", "Path to raw mobility CSV dataset (e.g. generator/data/train.csv or generator/data/test.csv)")
	var serverAddr string
	flag.StringVar(&serverAddr, "server", "localhost:8080", "Server endpoint address (host:port)")
	flag.Parse()

	numWorkers := 3
	packetChan := make(chan PacketJob, 100)
	var wg sync.WaitGroup

	log.Printf("Starting concurrent CSV spatial data streaming with %d workers from %s...", numWorkers, dataPath)

	// Spawn the 3 client worker goroutines
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go runWorker(i, serverAddr, packetChan, &wg)
	}

	// Read raw spatial dataset file once and feed the channel
	err := streamDataset(dataPath, packetChan)
	if err != nil {
		log.Printf("Fatal error streaming dataset: %v", err)
	}

	// Close channel to signal workers to gracefully exit
	close(packetChan)

	// Wait for all workers to shut down connection cleanly
	wg.Wait()
	log.Println("All concurrent CSV streaming workers have completed.")
}

// runWorker maintains a persistent TCP connection to the server and consumes/pipes jobs from the channel.
func runWorker(workerID int, serverAddr string, packetChan <-chan PacketJob, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("[Worker %d] Connecting to server on %s...", workerID, serverAddr)
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Printf("[Worker %d] Connection failed: %v", workerID, err)
		// Drain the channel in background to prevent deadlock of the main thread reader
		go func() {
			for range packetChan {}
		}()
		return
	}
	defer conn.Close()

	log.Printf("[Worker %d] Connected. Pipeline consumer started.", workerID)

	for job := range packetChan {
		_, err := conn.Write([]byte(job.Payload))
		if err != nil {
			log.Printf("[Worker %d] Failed to write packet: %v", workerID, err)
			// Drain the channel in background to prevent deadlock of the main thread reader
			go func() {
				for range packetChan {}
			}()
			return
		}
		// Staggered pacing simulation
		time.Sleep(job.SleepDuration)
	}

	log.Printf("[Worker %d] Stream finished. Closing TCP connection.", workerID)
}

// streamDataset reads the CSV file line-by-line, decodes geohashes, maps values, and feeds the job channel.
func streamDataset(filePath string, packetChan chan<- PacketJob) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Memory-safe line-by-line reading using encoding/csv wrapped in bufio
	bufReader := bufio.NewReader(file)
	csvReader := csv.NewReader(bufReader)

	// Read and parse headers to dynamically locate column indices
	header, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV headers: %w", err)
	}

	idxIndex, idxGeohash, idxDemand, idxWeather := -1, -1, -1, -1
	for idx, h := range header {
		switch strings.ToLower(h) {
		case "index":
			idxIndex = idx
		case "geohash":
			idxGeohash = idx
		case "demand":
			idxDemand = idx
		case "weather":
			idxWeather = idx
		}
	}

	if idxIndex == -1 || idxGeohash == -1 || idxDemand == -1 || idxWeather == -1 {
		return fmt.Errorf("essential CSV columns (Index, geohash, demand, Weather) not found in header")
	}

	rowCount := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Extract variables
		indexStr := record[idxIndex]
		geohashStr := record[idxGeohash]
		demandStr := record[idxDemand]
		weatherRaw := strings.ToLower(strings.TrimSpace(record[idxWeather]))

		// Decode geohash to get coordinates
		lat, lon := decodeGeohash(geohashStr)
		latStr := fmt.Sprintf("%.6f", lat)
		lonStr := fmt.Sprintf("%.6f", lon)

		// Map weather attributes
		weather := "clear"
		if weatherRaw == "rainy" {
			weather = "rain"
		} else if weatherRaw == "foggy" || weatherRaw == "snowy" {
			weather = "fog"
		} //Sunny or empty defaults to "clear"

		// Parse demand
		demandVal, err := strconv.ParseFloat(demandStr, 64)
		if err != nil {
			demandVal = 0.0
		}

		// Dynamic Traffic Pacing based on demand column (Accelerated 100x for benchmarking)
		sleepDuration := 1500 * time.Microsecond
		if demandVal > 0.1 {
			sleepDuration = 100 * time.Microsecond
		}

		// Index is used for sequence tracking
		indexVal, err := strconv.Atoi(indexStr)
		if err != nil {
			indexVal = rowCount
		}

		// Format into custom TCP framing protocol string
		packet := fmt.Sprintf("client=%s,seq=%d,lat=%s,lon=%s,weather=%s\n", geohashStr, indexVal, latStr, lonStr, weather)

		packetChan <- PacketJob{
			Payload:       packet,
			SleepDuration: sleepDuration,
		}

		rowCount++
		if rowCount%10000 == 0 {
			log.Printf("[Dataset Reader] Parsed and queued %d rows...", rowCount)
		}
	}

	log.Printf("[Dataset Reader] Finished scanning %d total dataset rows.", rowCount)
	return nil
}

// decodeGeohash decodes a standard geohash string into latitude and longitude coordinates.
func decodeGeohash(geohash string) (lat, lon float64) {
	const base32Alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"
	bitsMap := make(map[rune]int)
	for i, r := range base32Alphabet {
		bitsMap[r] = i
	}

	latMin, latMax := -90.0, 90.0
	lonMin, lonMax := -180.0, 180.0
	isEven := true // Even bits are longitude, odd bits are latitude

	for _, char := range geohash {
		val, ok := bitsMap[char]
		if !ok {
			continue
		}

		for mask := 16; mask > 0; mask >>= 1 {
			bit := (val & mask) != 0
			if isEven {
				mid := (lonMin + lonMax) / 2
				if bit {
					lonMin = mid
				} else {
					lonMax = mid
				}
			} else {
				mid := (latMin + latMax) / 2
				if bit {
					latMin = mid
				} else {
					latMax = mid
				}
			}
			isEven = !isEven
		}
	}

	lat = (latMin + latMax) / 2
	lon = (lonMin + lonMax) / 2
	return lat, lon
}
