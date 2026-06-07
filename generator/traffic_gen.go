package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	numClients := 3

	log.Printf("Starting concurrent traffic generator with %d clients...", numClients)

	for i := 1; i <= numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runClient(clientID)
		}(i)
	}

	wg.Wait()
	log.Println("All traffic generator workers have completed.")
}

// runClient runs an independent telemetry sender.
func runClient(id int) {
	log.Printf("[Client %d] Connecting to server on localhost:8080...", id)
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Printf("[Client %d] Connection failed: %v", id, err)
		return
	}
	defer conn.Close()

	log.Printf("[Client %d] Connected successfully. Sending packets...", id)

	for p := 1; p <= 5; p++ {
		// Unique telemetry packet data including client ID, packet sequence, and timestamp
		packet := fmt.Sprintf("client=%d,seq=%d,lat=37.7749,lon=-122.4194,ts=%d\n", id, p, time.Now().UnixMilli())
		_, err := conn.Write([]byte(packet))
		if err != nil {
			log.Printf("[Client %d] Failed to write packet %d: %v", id, p, err)
			return
		}
		log.Printf("[Client %d] Sent: %q", id, packet)
		
		// Staggered sending delay to ensure interleaving is visible
		time.Sleep(150 * time.Millisecond)
	}

	log.Printf("[Client %d] Finished sending telemetry. Closing connection.", id)
}
