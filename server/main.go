package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Ajitesh-stack/spatial-ingestion-server/cache"
	"github.com/Ajitesh-stack/spatial-ingestion-server/metrics"
	"github.com/Ajitesh-stack/spatial-ingestion-server/routing"
)

var (
	activeConnections int64
	globalCache       *cache.ShardedCache
	globalGraph       *routing.Graph
	globalMetrics     *metrics.SystemMetrics
)

func main() {
	// Initialize metrics tracking
	globalMetrics = metrics.NewSystemMetrics()
	globalMetrics.StartReporting(2 * time.Second) // Report metrics every 2 seconds
	log.Println("Internal Instrumentation Collector started (Reporting interval: 2s)")

	// Instantiate the global ShardedCache (16 shards, capacity 100 per shard)
	globalCache = cache.NewShardedCache(16, 100)
	log.Println("Global Sharded LRU Cache initialized (16 shards, capacity 100 per shard)")

	// Instantiate the static network topology graph (Read-Only under concurrency)
	globalGraph = routing.GetTestTopology()
	log.Println("Global Network Topology initialized (Nodes A to E)")

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}

	var wg sync.WaitGroup
	shutdownChan := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received. Shutting down server gracefully...")
		close(shutdownChan)
		listener.Close() // This unblocks listener.Accept() and causes it to return an error
	}()

	log.Println("Concurrent Ingestion Server is listening on TCP port 8080...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-shutdownChan:
				log.Println("Accept loop stopped. Waiting for active connections to finish...")
				goto shutdown
			default:
				log.Printf("Failed to accept connection: %v", err)
				continue
			}
		}

		// Track new connection atomically
		count := atomic.AddInt64(&activeConnections, 1)
		log.Printf("Accepted connection from %s. Active connections: %d", conn.RemoteAddr().String(), count)

		// Process connection concurrently using goroutines
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			handleConnection(c)
		}(conn)
	}

shutdown:
	wg.Wait()
	log.Println("Server gracefully stopped. All connection handlers finished.")
}

// handleConnection handles a single client connection, enforcing an idle read timeout,
// evaluating dynamic routing cost, injecting weather-induced latency, and cache logging.
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

		// Track packet processing
		globalMetrics.IncrementPackets()

		log.Printf("[%s] Received packet: %q", conn.RemoteAddr().String(), line)

		// Extract client ID and weather attributes
		clientID, okClient := extractClientID(line)
		weather, okWeather := extractWeather(line)

		if okClient && okWeather {
			// Compute shortest path and dynamic weight metrics locally (No global graph mutations)
			path, cost, latencySleep := globalGraph.FindShortestPath("A", "E", weather)
			log.Printf("[%s] Dijkstra path resolved for weather %q: %v with total cost %.2f", conn.RemoteAddr().String(), weather, path, cost)

			// Simulate environmental transmission delay if required
			if latencySleep > 0 {
				scaledSleep := latencySleep / 100
				log.Printf("[%s] Simulating adversarial link degradation: sleeping for %v (%q weather)", conn.RemoteAddr().String(), scaledSleep, weather)
				time.Sleep(scaledSleep)
				globalMetrics.AddInjectedLatency(latencySleep)
			}

			// Perform Cache operations
			globalCache.Set(clientID, line)
			log.Printf("[%s] Cache WRITE for key %q", conn.RemoteAddr().String(), clientID)

			if val, found := globalCache.Get(clientID); found {
				globalMetrics.IncrementCacheHits()
				log.Printf("[%s] Cache READ hit for key %q: %q", conn.RemoteAddr().String(), clientID, val.(string))
			} else {
				globalMetrics.IncrementCacheMisses()
				log.Printf("[%s] Cache READ miss for key %q", conn.RemoteAddr().String(), clientID)
			}
		} else {
			log.Printf("[%s] Failed to parse telemetry packet parameters: %q", conn.RemoteAddr().String(), line)
		}
	}
}

// extractClientID retrieves the value of the "client=" parameter in the telemetry packet.
func extractClientID(packet string) (string, bool) {
	const prefix = "client="
	idx := strings.Index(packet, prefix)
	if idx == -1 {
		return "", false
	}
	start := idx + len(prefix)

	end := strings.Index(packet[start:], ",")
	if end == -1 {
		end = strings.Index(packet[start:], "\n")
		if end == -1 {
			end = len(packet[start:])
		}
	}

	clientID := strings.TrimSpace(packet[start : start+end])
	if clientID == "" {
		return "", false
	}
	return clientID, true
}

// extractWeather retrieves the value of the "weather=" parameter in the telemetry packet.
func extractWeather(packet string) (string, bool) {
	const prefix = "weather="
	idx := strings.Index(packet, prefix)
	if idx == -1 {
		return "", false
	}
	start := idx + len(prefix)

	end := strings.Index(packet[start:], ",")
	if end == -1 {
		end = strings.Index(packet[start:], "\n")
		if end == -1 {
			end = len(packet[start:])
		}
	}

	weather := strings.TrimSpace(packet[start : start+end])
	if weather == "" {
		return "", false
	}
	return weather, true
}
