package main

import (
	"log"
	"net"
	"time"
)

func main() {
	log.Println("Connecting to server on localhost:8080...")
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	log.Println("Connected successfully. Sending telemetry packets...")

	telemetrySamples := []string{
		"telemetry_packet_1: latitude=37.7749, longitude=-122.4194, speed=45.2\n",
		"telemetry_packet_2: latitude=37.7750, longitude=-122.4195, speed=46.1\n",
		"telemetry_packet_3: latitude=37.7751, longitude=-122.4196, speed=47.0\n",
		"telemetry_packet_4: latitude=37.7752, longitude=-122.4197, speed=45.8\n",
		"telemetry_packet_5: latitude=37.7753, longitude=-122.4198, speed=44.9\n",
	}

	for _, sample := range telemetrySamples {
		_, err := conn.Write([]byte(sample))
		if err != nil {
			log.Fatalf("Error sending telemetry data: %v", err)
		}
		log.Printf("Sent packet: %q", sample)
		time.Sleep(100 * time.Millisecond)
	}

	log.Println("All telemetry packets sent. Closing connection gracefully.")
}
