package metrics

import (
	"sync"
	"testing"
	"time"
)

// TestSystemMetricsCounters validates atomic counter increment correctness and thread-safety.
func TestSystemMetricsCounters(t *testing.T) {
	m := NewSystemMetrics()

	// Verify initial state
	if m.TotalPacketsProcessed != 0 || m.CacheHits != 0 || m.CacheMisses != 0 || m.TotalInjectedLatencyMs != 0 {
		t.Error("expected metrics to initialize to zero values")
	}

	// Test increment functions
	m.IncrementPackets()
	m.IncrementCacheHits()
	m.IncrementCacheMisses()
	m.AddInjectedLatency(50 * time.Millisecond)

	if m.TotalPacketsProcessed != 1 {
		t.Errorf("expected 1 processed packet, got %d", m.TotalPacketsProcessed)
	}
	if m.CacheHits != 1 {
		t.Errorf("expected 1 cache hit, got %d", m.CacheHits)
	}
	if m.CacheMisses != 1 {
		t.Errorf("expected 1 cache miss, got %d", m.CacheMisses)
	}
	if m.TotalInjectedLatencyMs != 50 {
		t.Errorf("expected 50ms injected latency, got %d", m.TotalInjectedLatencyMs)
	}
}

// TestSystemMetricsConcurrency verifies that collector counters remain race-free
// under heavy concurrent updates.
func TestSystemMetricsConcurrency(t *testing.T) {
	m := NewSystemMetrics()
	const workers = 50
	const ops = 1000

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				m.IncrementPackets()
				m.IncrementCacheHits()
				m.IncrementCacheMisses()
				m.AddInjectedLatency(2 * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	expectedCount := uint64(workers * ops)
	if m.TotalPacketsProcessed != expectedCount {
		t.Errorf("expected %d packets, got %d", expectedCount, m.TotalPacketsProcessed)
	}
	if m.CacheHits != expectedCount {
		t.Errorf("expected %d hits, got %d", expectedCount, m.CacheHits)
	}
	if m.CacheMisses != expectedCount {
		t.Errorf("expected %d misses, got %d", expectedCount, m.CacheMisses)
	}
	if m.TotalInjectedLatencyMs != expectedCount*2 {
		t.Errorf("expected %dms latency, got %d", expectedCount*2, m.TotalInjectedLatencyMs)
	}
}

// TestSystemMetricsReporting verifies the background reporting loop executes without panicking.
func TestSystemMetricsReporting(t *testing.T) {
	m := NewSystemMetrics()
	m.IncrementPackets()
	m.IncrementCacheHits()
	m.IncrementCacheMisses()
	m.AddInjectedLatency(10 * time.Millisecond)

	// Start reporting loop with a small interval
	m.StartReporting(20 * time.Millisecond)

	// Wait for ticker to fire at least once
	time.Sleep(50 * time.Millisecond)
}

// TestSystemMetricsZeroReadsReporting verifies the reporting loop with zero cache reads.
func TestSystemMetricsZeroReadsReporting(t *testing.T) {
	m := NewSystemMetrics()
	// Total cache hits and misses are 0, which makes totalReads = 0.
	m.StartReporting(20 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
}

