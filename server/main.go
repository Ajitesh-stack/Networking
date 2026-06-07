package main

import (
	"bufio"
	"io"
	"log"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}
	defer listener.Close()

	log.Println("Server is listening on TCP port 8080...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		log.Printf("Accepted connection from %s", conn.RemoteAddr().String())

		// Process connection synchronously (no goroutines or caching structures yet)
		handleConnection(conn)
	}
}

// handleConnection handles a single client connection, reading packets delimited by '\n'.
func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("Connection closed by client (EOF)")
			} else {
				log.Printf("Error reading from connection: %v", err)
			}
			break
		}

		log.Printf("Received packet: %q", line)
	}
}
