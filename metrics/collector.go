package metrics

import (
	"log"
	"sync/atomic"
	"time"
)

// SystemMetrics tracks system performance metrics under concurrent load.
// To prevent structural alignment panics during atomic mutations on 32-bit platforms,
// all 64-bit counter fields are declared at the very beginning of the struct.
type SystemMetrics struct {
	TotalPacketsProcessed  uint64
	CacheHits              uint64
	CacheMisses            uint64
	TotalInjectedLatencyMs uint64
}

// NewSystemMetrics instantiates a new SystemMetrics collector.
func NewSystemMetrics() *SystemMetrics {
	return &SystemMetrics{}
}

// IncrementPackets atomically increments the processed packet counter.
func (m *SystemMetrics) IncrementPackets() {
	atomic.AddUint64(&m.TotalPacketsProcessed, 1)
}

// IncrementCacheHits atomically increments the cache hits counter.
func (m *SystemMetrics) IncrementCacheHits() {
	atomic.AddUint64(&m.CacheHits, 1)
}

// IncrementCacheMisses atomically increments the cache misses counter.
func (m *SystemMetrics) IncrementCacheMisses() {
	atomic.AddUint64(&m.CacheMisses, 1)
}

// AddInjectedLatency atomically adds to the total latency duration.
func (m *SystemMetrics) AddInjectedLatency(duration time.Duration) {
	atomic.AddUint64(&m.TotalInjectedLatencyMs, uint64(duration.Milliseconds()))
}

// StartReporting spawns a detached background goroutine that logs metric reports periodically.
func (m *SystemMetrics) StartReporting(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			processed := atomic.LoadUint64(&m.TotalPacketsProcessed)
			hits := atomic.LoadUint64(&m.CacheHits)
			misses := atomic.LoadUint64(&m.CacheMisses)
			latency := atomic.LoadUint64(&m.TotalInjectedLatencyMs)

			var hitRate float64
			totalReads := hits + misses
			if totalReads > 0 {
				hitRate = (float64(hits) / float64(totalReads)) * 100.0
			}

			log.Printf("[METRICS REPORT] Processed: %d | Hits: %d | Misses: %d | Hit Rate: %.2f%% | Total Latency: %dms",
				processed, hits, misses, hitRate, latency)
		}
	}()
}
