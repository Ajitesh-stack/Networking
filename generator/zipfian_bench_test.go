// Run with: go test -v ./generator/...
// For race detection: go test -race ./generator/...
package main

import (
	"fmt"
	"os"
	"sort"
	"testing"
)

// TestZipfianPowerLaw verifies that the Zipfian distribution generator produces a power-law distribution.
func TestZipfianPowerLaw(t *testing.T) {
	// 100 keys
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	zg := newZipfGenerator(keys, 1.07)

	counts := make(map[string]int)
	samples := 100000

	for i := 0; i < samples; i++ {
		key := zg.Draw()
		counts[key]++
	}

	frequencies := make([]int, 0, len(counts))
	for _, freq := range counts {
		frequencies = append(frequencies, freq)
	}

	sort.Slice(frequencies, func(i, j int) bool {
		return frequencies[i] > frequencies[j]
	})

	top20Hits := 0
	for i := 0; i < 20 && i < len(frequencies); i++ {
		top20Hits += frequencies[i]
	}

	pct := float64(top20Hits) / float64(samples) * 100.0

	if top20Hits <= 60000 {
		t.Errorf("Expected top 20%% of keys to get > 60%% of requests (60,000 hits), got %d (%.2f%%)", top20Hits, pct)
	} else {
		t.Logf("Pass: top 20%% of keys got %d hits (%.2f%%)", top20Hits, pct)
	}
}

// TestCalculatePercentiles verifies that the percentile calculation math is correct.
func TestCalculatePercentiles(t *testing.T) {
	var lt latencyTracker

	// Record values 1 through 100
	for i := int64(1); i <= 100; i++ {
		lt.record(i)
	}

	p50, p95, p99 := lt.percentiles()

	check := func(name string, got, want int64) {
		diff := got - want
		if diff < 0 {
			diff = -diff
		}
		if diff > 1 {
			t.Errorf("%s: got %d, want %d (±1)", name, got, want)
		}
	}

	check("p50", p50, 50)
	check("p95", p95, 95)
	check("p99", p99, 99)
}

// TestZipfBoundaryConditions verifies boundary scenarios in the Zipfian generator.
func TestZipfBoundaryConditions(t *testing.T) {
	// Case A — Single key pool
	t.Run("SingleKeyPool", func(t *testing.T) {
		zg := newZipfGenerator([]string{"only-key"}, 1.07)
		for i := 0; i < 500; i++ {
			if res := zg.Draw(); res != "only-key" {
				t.Errorf("Expected draw to be 'only-key', got %q", res)
			}
		}
	})

	// Case B — CDF last element exactly 1.0
	t.Run("CDFExactEnding", func(t *testing.T) {
		zg := newZipfGenerator([]string{"a", "b", "c"}, 1.07)
		if zg.cdf[2] != 1.0 {
			t.Errorf("Expected last element of CDF to be exactly 1.0, got %f", zg.cdf[2])
		}
	})

	// Case C — No draw ever returns out-of-bounds key
	t.Run("OutOfBoundsSafety", func(t *testing.T) {
		pool := make([]string, 10)
		lookup := make(map[string]bool)
		for i := 0; i < 10; i++ {
			pool[i] = fmt.Sprintf("k-%d", i)
			lookup[pool[i]] = true
		}

		zg := newZipfGenerator(pool, 1.07)
		for i := 0; i < 10000; i++ {
			res := zg.Draw()
			if !lookup[res] {
				t.Errorf("Drawn key %q not found in original pool", res)
			}
		}
	})
}

// TestHotspotDominance verifies that hotspots at ranks 0, 1, 2 get a significant proportion of requests.
func TestHotspotDominance(t *testing.T) {
	pool := []string{"hot-0", "hot-1", "hot-2"}
	for i := 0; i < 97; i++ {
		pool = append(pool, fmt.Sprintf("cold-%d", i))
	}

	zg := newZipfGenerator(pool, 1.07)

	samples := 50000
	hotCount := 0

	for i := 0; i < samples; i++ {
		res := zg.Draw()
		if res == "hot-0" || res == "hot-1" || res == "hot-2" {
			hotCount++
		}
	}

	pct := float64(hotCount) / float64(samples) * 100.0
	t.Logf("Bangalore hotspots (top 3) received %.2f%% of requests", pct)

	if pct <= 15.0 {
		t.Errorf("Expected combined hotspots percentage to be > 15.0%%, got %.2f%%", pct)
	}
}

// TestBuildKeyPoolHotspotOrdering verifies correct build order and deduplication in buildKeyPool.
func TestBuildKeyPoolHotspotOrdering(t *testing.T) {
	content := "Index,geohash,demand,Weather\n1,td6pmz,0.5,sunny\n2,abc123,0.3,rainy\n3,xyz789,0.1,foggy\n"

	tmpFile, err := os.CreateTemp("", "bench_test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write mock CSV content: %v", err)
	}
	tmpFile.Close()

	hotspots := []string{"td6pmz", "tdw12", "td6rs2"}
	pool, err := buildKeyPool(tmpFile.Name(), hotspots)
	if err != nil {
		t.Fatalf("buildKeyPool failed: %v", err)
	}

	if len(pool) < 3 {
		t.Fatalf("Pool is too short, expected at least 3 elements, got %d", len(pool))
	}

	// Assert hotspots are at rank 0, 1, 2
	if pool[0] != "td6pmz" || pool[1] != "tdw12" || pool[2] != "td6rs2" {
		t.Errorf("Expected hotspots to occupy ranks 0, 1, 2. Got ranks 0: %q, 1: %q, 2: %q", pool[0], pool[1], pool[2])
	}

	// Assert td6pmz appears exactly once in the final pool
	td6pmzCount := 0
	for _, k := range pool {
		if k == "td6pmz" {
			td6pmzCount++
		}
	}
	if td6pmzCount != 1 {
		t.Errorf("Expected td6pmz to appear exactly once, got %d times", td6pmzCount)
	}

	// Assert remaining unique geohashes are in pool[3:]
	foundAbc := false
	foundXyz := false
	for _, k := range pool[3:] {
		if k == "abc123" {
			foundAbc = true
		}
		if k == "xyz789" {
			foundXyz = true
		}
	}

	if !foundAbc {
		t.Errorf("Expected 'abc123' to be in remaining pool")
	}
	if !foundXyz {
		t.Errorf("Expected 'xyz789' to be in remaining pool")
	}
}
