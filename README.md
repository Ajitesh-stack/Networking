# Spatial Ingestion Server & Routing Engine
> A high-throughput spatial data processing pipeline and sharded LRU cache built to ingest, decode, and route real-world mobility streams under simulated network degradation.

[![Go Version](https://img.shields.io/github/go-mod/go-version/Ajitesh-stack/Networking)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

This repository implements a concurrent, zero-dependency Go service designed to parse, ingest, cache, and route spatial telemetry packets at scale. The system is validated using the Bangalore Mobility dataset (`train.csv`), streaming over 77,000 spatial records concurrently with zero lock contention or data loss.

---

## ⚡ Key Features

* **Zero-Dependency base32 Geohash Decoder**: Converts alphanumeric geohashes into high-precision floating-point coordinates dynamically without importing external dependencies.
* **FNV-1a Sharded LRU Cache**: Implements an $N$-shard cache coordinator that uses FNV-1a hashing to distribute keys. This limits lock contention compared to a global lock.
* **Adversarial weather-state Dijkstra routing**: Calculates dynamic shortest paths locally across graph hubs. Latency penalties are dynamically injected without modifying the baseline graph weights, allowing lock-free concurrent path computations.
* **High-Throughput TCP pipeline**: Employs a single-reader, multi-worker client model that pipelines packets over persistent connections with natural TCP backpressure.

---

## 🏗️ Architecture Overview

The system consists of two primary components: a multi-connection raw TCP load generator and a multi-threaded ingestion server.

### Pipeline Topology

```
[generator/data/train.csv]
          │ (Single OS file descriptor)
          ▼
   [Dataset Reader] ──(Decode Geohash & Normalize)
          │
          ▼
   [packetChan] (Bounded queue, capacity: 100)
          │
      ┌───┼───┐ (Load balancing)
      ▼   ▼   ▼
    [Worker Thread 1-3]
      │   │   │ (Persistent TCP streams)
      ▼   ▼   ▼
   [TCP Port :8080]
          │
          ▼
  [Server listener]
          │ (Spawn goroutine per client)
          ▼
 [Connection Handlers] (15s idle deadlines)
    ├──► [FNV-1a Sharded LRU Cache] (16 Shards, exclusive locks)
    └──► [Dijkstra Routing Engine] (Local path relaxation)
```

* **Ingest Framing Protocol**: Custom newline-terminated frames:
  `client=<geohash>,seq=<index>,lat=<latitude>,lon=<longitude>,weather=<clear/rain/fog>`
* **Server Resilience**: Each incoming connection is wrapped with a 15-second read deadline (`net.Conn.SetReadDeadline`) that is refreshed on every packet. This prevents orphaned goroutines from building up if client edge nodes drop connection abruptly.

---

## 🧪 Testing and Benchmarks

The project is thoroughly tested using Go's standard library testing tools, achieving a high statement coverage (>80% on core library logic in `/cache`, `/routing`, and `/metrics` packages).

### Running Unit Tests

To execute all unit tests in the repository and print output logs:
```bash
make test
```
*Alternatively, run: `go test -v -cover -coverprofile="coverage.out" ./...`*

#### Example Output:
```text
=== RUN   TestLRUCacheBasic
--- PASS: TestLRUCacheBasic (0.00s)
=== RUN   TestLRUUpdateRecency
--- PASS: TestLRUUpdateRecency (0.00s)
=== RUN   TestShardingCorrectness
--- PASS: TestShardingCorrectness (0.00s)
=== RUN   TestShardedCacheConcurrency
--- PASS: TestShardedCacheConcurrency (0.01s)
PASS
ok  	github.com/Ajitesh-stack/spatial-ingestion-server/cache	1.240s	coverage: 100.0% of statements
```

### Running Performance Benchmarks

To benchmark the concurrent sharded LRU cache and Dijkstra pathfinder under high thread contention:
```bash
make bench
```
*Alternatively, run: `go test -bench="." -benchmem ./...`*

#### Example Output:
```text
goos: windows
goarch: amd64
pkg: github.com/Ajitesh-stack/spatial-ingestion-server/cache
BenchmarkShardedCacheGet-16    	34257133	        35.47 ns/op	      13 B/op	       1 allocs/op
BenchmarkShardedCacheSet-16    	36207067	        34.75 ns/op	      13 B/op	       1 allocs/op
```

---

## 📊 Performance Results

The following benchmark metrics reflect the ingestion of the full **Bangalore Mobility dataset** under the accelerated load configuration (which scales down simulated physical delays 100x while maintaining metrics accounting in milliseconds):

| Metric | Value |
| :--- | :--- |
| **Total Telemetry Packets Processed** | 77,299 |
| **LRU Cache Hits** | 77,299 |
| **LRU Cache Misses** | 0 |
| **LRU Cache Hit Rate** | 100.00% |
| **Accumulated Simulated Latency** | 4,396,520 ms |
| **Total Processing Time** | ~78 seconds |
| **Effective Ingestion Throughput** | ~991 packets/sec |

---

## 🛠️ Installation & Quick Start

Ensure Go 1.18+ is installed on your machine.

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Ajitesh-stack/Networking
   cd Networking
   ```

2. **Verify dataset location**:
   Ensure `generator/data/train.csv` exists and is formatted correctly.

3. **Build the binaries (Optional)**:
   * **Linux / macOS**:
     ```bash
     go build -o server ./server
     go build -o generator ./generator
     ```
   * **Windows**:
     ```bash
     go build -o server.exe ./server
     go build -o generator.exe ./generator
     ```

---

## 🚀 How to Run

You can run the built binaries or execute them directly using `go run`.

### Option A: Using `go run` (Recommended for quick start)

1. **Start the Ingestion Server**:
   ```bash
   go run ./server
   ```
2. **Run the Load Generator**:
   In a separate terminal session, execute:
   ```bash
   go run ./generator
   ```

### Option B: Running the Built Binaries

1. **Start the Ingestion Server**:
   * **Linux / macOS**:
     ```bash
     ./server
     ```
   * **Windows**:
     ```bash
     ./server.exe
     ```

2. **Run the Load Generator**:
   In a separate terminal session, execute:
   * **Linux / macOS**:
     ```bash
     ./generator
     ```
   * **Windows**:
     ```bash
     ./generator.exe
     ```

---

## 🧠 Design Decisions & Learnings

### LRU Read Mutations
A common mistake in caching is assuming reads are concurrent-safe under a standard read lock (`RLock()`). In an LRU cache, accessing a key (`Get`) requires moving the corresponding node to the front of the underlying doubly-linked list (`container/list`) to update its recency status. Because pointer updates constitute a write mutation, a full write lock (`Lock()`) is required for both reads and writes. To prevent bottlenecking, we mitigate this lock contention by sharding the cache namespace into 16 independent lock buckets.

### Lock-Free Dijkstra Graph
To prevent lock contention on the global routing graph under high concurrent query volumes, the graph representation remains completely read-only. When resolving dynamic costs for weather conditions (e.g. `rain` scaling edge weights by `1.5x`, `fog` by `2.0x`), neighbor relaxations and cost computations are calculated locally within the executing goroutine's frame. This approach allows concurrent routing queries without locking the graph.

### Memory Alignment & Atomic Constraints
To guarantee compatibility across architectures (such as 32-bit platforms) and prevent structural alignment panics during `sync/atomic` operations, all 64-bit metric counter fields inside the `SystemMetrics` struct are explicitly declared as the first fields in the structure, ensuring they are aligned on 8-byte boundaries:

```go
type SystemMetrics struct {
	TotalPacketsProcessed  uint64
	CacheHits              uint64
	CacheMisses            uint64
	TotalInjectedLatencyMs uint64
}
```

### Producer-Consumer Deadlock Prevention
In a concurrent producer-consumer pipeline where client workers feed from a bounded channel, a worker failure (e.g., due to TCP connection loss) can lead to a deadlock. If the worker threads exit early while the main file scanner is still writing to the channel, the scanner blocks forever once the channel buffer fills. We resolved this by implementing an asynchronous draining mechanism:

```go
_, err := conn.Write([]byte(job.Payload))
if err != nil {
    log.Printf("[Worker %d] Failed to write packet: %v", workerID, err)
    // Drain channel asynchronously to allow main reader to finish scan and exit cleanly
    go func() {
        for range packetChan {}
    }()
    return
}
```

---

## 🔮 Future Work

- [ ] **Dynamic Shard Resizing**: Dynamically adjust the number of cache shards based on real-time collision and lock contention metrics.
- [ ] **gRPC Ingestion Path**: Introduce a gRPC/Protobuf streaming path to reduce message framing and parsing overhead compared to newline-delimited protocols.
- [ ] **Persistent Cache Backing**: Implement write-ahead logging (WAL) to persist cache keys across server restarts.

---

## 🏷️ Topics
`go`, `concurrency`, `networking`, `geohash`, `dijkstra`, `systems-design`