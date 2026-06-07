package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"
)

var activeConnections int64

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}
	defer listener.Close()

	log.Println("Concurrent Ingestion Server is listening on TCP port 8080...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		// Track new connection atomically
		count := atomic.AddInt64(&activeConnections, 1)
		log.Printf("Accepted connection from %s. Active connections: %d", conn.RemoteAddr().String(), count)

		// Process connection concurrently using goroutines
		go handleConnection(conn)
	}
}

// handleConnection handles a single client connection, enforcing an idle read timeout.
func handleConnection(conn net.Conn) {
	defer func() {
		count := atomic.AddInt64(&activeConnections, -1)
		log.Printf("Closing connection from %s. Active connections: %d", conn.RemoteAddr().String(), count)
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	idleTimeout := 15 * time.Second

	for {
		// Set/Refresh read deadline on every read to detect abrupt client disconnects
		err := conn.SetReadDeadline(time.Now().Add(idleTimeout))
		if err != nil {
			log.Printf("Error setting read deadline for %s: %v", conn.RemoteAddr().String(), err)
			break
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("Connection closed by client %s (EOF)", conn.RemoteAddr().String())
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("Connection timed out for %s (idle for %v)", conn.RemoteAddr().String(), idleTimeout)
			} else {
				log.Printf("Error reading from connection %s: %v", conn.RemoteAddr().String(), err)
			}
			break
		}

		log.Printf("[%s] Received packet: %q", conn.RemoteAddr().String(), line)
	}
}
