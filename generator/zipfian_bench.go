package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Ajitesh-stack/spatial-ingestion-server/cache"
)

// bangaloreHotspots holds the hardcoded hotspots to map to the top Zipfian ranks
var bangaloreHotspots = []string{"td6pmz", "tdw12", "td6rs2"}

// ZipfGenerator implements a power-law request distribution generator based on standard Zipf's Law.
type ZipfGenerator struct {
	keys []string
	cdf  []float64
	rng  *rand.Rand
	mu   sync.Mutex // protects access to rng which is not thread-safe
}

// newZipfGenerator precomputes the CDF for the Zipfian distribution on the given keys.
func newZipfGenerator(keys []string, skew float64) *ZipfGenerator {
	n := len(keys)
	cdf := make([]float64, n)

	// Compute raw Zipf weights and sum them
	totalWeight := 0.0
	for i := 0; i < n; i++ {
		weight := 1.0 / math.Pow(float64(i+1), skew)
		totalWeight += weight
		cdf[i] = totalWeight
	}

	// Normalize into CDF probabilities [0.0, 1.0]
	for i := 0; i < n; i++ {
		cdf[i] /= totalWeight
	}

	// Force the last index to exactly 1.0 to prevent floating point rounding issues
	if n > 0 {
		cdf[n-1] = 1.0
	}

	return &ZipfGenerator{
		keys: keys,
		cdf:  cdf,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Draw returns a key drawn according to the precomputed Zipfian distribution.
func (z *ZipfGenerator) Draw() string {
	z.mu.Lock()
	r := z.rng.Float64()
	z.mu.Unlock()

	index := sort.Search(len(z.cdf), func(i int) bool {
		return z.cdf[i] >= r
	})

	if index >= len(z.keys) {
		index = len(z.keys) - 1
	}

	return z.keys[index]
}

// buildKeyPool parses the CSV, extracts all unique geohashes, filters out Bangalore hotspots,
// and prepends the hotspots to the front of the pool so they receive ranks 0, 1, and 2.
func buildKeyPool(csvPath string, hotspots []string) ([]string, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	csvReader := csv.NewReader(bufio.NewReader(file))

	// Create hotspot lookup map to prevent duplicates
	hotspotMap := make(map[string]bool)
	for _, h := range hotspots {
		hotspotMap[h] = true
	}

	uniqueMap := make(map[string]bool)

	// Read first line to detect header
	firstRecord, err := csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return hotspots, nil
		}
		return nil, err
	}

	isHeader := false
	if len(firstRecord) > 1 {
		if strings.ToLower(firstRecord[0]) == "index" || strings.ToLower(firstRecord[1]) == "geohash" {
			isHeader = true
		}
	}

	if !isHeader && len(firstRecord) > 1 {
		val := firstRecord[1]
		if !hotspotMap[val] {
			uniqueMap[val] = true
		}
	}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(record) > 1 {
			val := record[1]
			if !hotspotMap[val] {
				uniqueMap[val] = true
			}
		}
	}

	// Extract unique geohashes
	pool := make([]string, 0, len(uniqueMap))
	for g := range uniqueMap {
		pool = append(pool, g)
	}

	// Prepend hotspots
	finalPool := make([]string, 0, len(hotspots)+len(pool))
	finalPool = append(finalPool, hotspots...)
	finalPool = append(finalPool, pool...)

	return finalPool, nil
}

// latencyTracker provides thread-safe collection and percentile calculation of latency timings.
type latencyTracker struct {
	samples []int64
	mu      sync.Mutex
}

// record appends a latency timing to the samples in a thread-safe manner.
func (lt *latencyTracker) record(us int64) {
	lt.mu.Lock()
	lt.samples = append(lt.samples, us)
	lt.mu.Unlock()
}

// percentiles computes the p50, p95, and p99 from the collected samples.
func (lt *latencyTracker) percentiles() (p50, p95, p99 int64) {
	lt.mu.Lock()
	if len(lt.samples) == 0 {
		lt.mu.Unlock()
		return 0, 0, 0
	}
	samplesCopy := make([]int64, len(lt.samples))
	copy(samplesCopy, lt.samples)
	lt.mu.Unlock()

	sort.Slice(samplesCopy, func(i, j int) bool {
		return samplesCopy[i] < samplesCopy[j]
	})

	n := len(samplesCopy)
	p50 = samplesCopy[n*50/100]
	p95 = samplesCopy[n*95/100]
	p99 = samplesCopy[n*99/100]

	return p50, p95, p99
}

// runZipfianBenchmark runs a power-law distributed benchmark against the ingestion server.
func runZipfianBenchmark(csvPath, serverAddr string, duration time.Duration, workers int, skew float64) {
	// 1. Call buildKeyPool
	pool, err := buildKeyPool(csvPath, bangaloreHotspots)
	if err != nil {
		log.Fatalf("Failed to build key pool: %v", err)
	}

	// 2. Create ZipfGenerator
	zg := newZipfGenerator(pool, skew)

	// 3. Create latencyTracker
	var lt latencyTracker

	// 4. Declare atomic counters
	var totalRequests int64
	var hitCount int64

	// 5. Create local ShardedCache (16 shards) for hit rate simulation
	localCache := cache.NewShardedCache(16, 25)

	// 6. Create done channel and close it after duration
	done := make(chan struct{})
	time.AfterFunc(duration, func() {
		close(done)
	})

	// 7. Spawn workers goroutines
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", serverAddr)
			if err != nil {
				log.Printf("[Worker %d] Failed to connect: %v", workerID, err)
				return
			}
			defer conn.Close()

			writer := bufio.NewWriter(conn)

			for {
				select {
				case <-done:
					return
				default:
					geohash := zg.Draw()
					reqSeq := atomic.AddInt64(&totalRequests, 1)
					packet := fmt.Sprintf("client=%s,seq=%d,lat=0,lon=0,weather=clear,mode=zipfian\n", geohash, reqSeq)

					start := time.Now()
					_, err := writer.WriteString(packet)
					if err == nil {
						err = writer.Flush()
					}
					elapsed := time.Since(start).Microseconds()

					if err != nil {
						log.Printf("[Worker %d] Write failed: %v", workerID, err)
						return
					}

					lt.record(elapsed)

					// Update local cache simulation
					_, found := localCache.Get(geohash)
					if found {
						atomic.AddInt64(&hitCount, 1)
					} else {
						localCache.Set(geohash, geohash)
					}
				}
			}
		}(i)
	}

	// 8. Wait for all workers to finish after done closes
	wg.Wait()

	// 9. Compute and print metrics
	totalReqsVal := atomic.LoadInt64(&totalRequests)
	hitCountVal := atomic.LoadInt64(&hitCount)

	var hitRate float64
	if totalReqsVal > 0 {
		hitRate = float64(hitCountVal) / float64(totalReqsVal) * 100.0
	}

	rps := float64(totalReqsVal) / duration.Seconds()
	p50, p95, p99 := lt.percentiles()

	// 10. Print the results table
	fmt.Println("\n╔══════════════════════════════════════════════════╗")
	fmt.Println("║         ZIPFIAN BENCHMARK RESULTS               ║")
	fmt.Println("╠══════════════════════════════════════════════════╣")
	fmt.Printf("║  Total Requests      : %-26d║\n", totalReqsVal)
	fmt.Printf("║  Cache Hit Rate      : %-25.2f%%║\n", hitRate)
	fmt.Printf("║  Throughput          : %-23.0f rps║\n", rps)
	fmt.Printf("║  p50 Latency         : %-23d µs║\n", p50)
	fmt.Printf("║  p95 Latency         : %-23d µs║\n", p95)
	fmt.Printf("║  p99 Latency         : %-23d µs║\n", p99)
	fmt.Println("╚══════════════════════════════════════════════════╝")
}
