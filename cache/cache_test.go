package cache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestLRUCacheBasic tests basic Set, Get, and Eviction behaviors.
func TestLRUCacheBasic(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		setup    func(c *LRUCache)
		verify   func(t *testing.T, c *LRUCache)
	}{
		{
			name:     "Set and Get single item",
			capacity: 2,
			setup: func(c *LRUCache) {
				c.Set("key1", "val1")
			},
			verify: func(t *testing.T, c *LRUCache) {
				val, found := c.Get("key1")
				if !found || val != "val1" {
					t.Errorf("expected val1, got %v (found: %v)", val, found)
				}
			},
		},
		{
			name:     "Evict oldest item when capacity reached",
			capacity: 2,
			setup: func(c *LRUCache) {
				c.Set("key1", "val1")
				c.Set("key2", "val2")
				c.Set("key3", "val3") // Should evict key1
			},
			verify: func(t *testing.T, c *LRUCache) {
				_, found1 := c.Get("key1")
				if found1 {
					t.Error("expected key1 to be evicted")
				}
				val2, found2 := c.Get("key2")
				if !found2 || val2 != "val2" {
					t.Errorf("expected key2 to remain, got %v", val2)
				}
				val3, found3 := c.Get("key3")
				if !found3 || val3 != "val3" {
					t.Errorf("expected key3 to remain, got %v", val3)
				}
			},
		},
		{
			name:     "Overwrite existing key",
			capacity: 2,
			setup: func(c *LRUCache) {
				c.Set("key1", "val1")
				c.Set("key1", "newval1")
			},
			verify: func(t *testing.T, c *LRUCache) {
				val, found := c.Get("key1")
				if !found || val != "newval1" {
					t.Errorf("expected newval1, got %v", val)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewLRUCache(tc.capacity)
			tc.setup(c)
			tc.verify(t, c)
		})
	}
}

// TestLRUUpdateRecency verifies that accessing an item via Get updates its recency status,
// preventing it from being evicted.
func TestLRUUpdateRecency(t *testing.T) {
	c := NewLRUCache(2)
	c.Set("key1", "val1")
	c.Set("key2", "val2")

	// Access key1 to update its recency
	_, found := c.Get("key1")
	if !found {
		t.Fatal("expected key1 to be found")
	}

	// Insert key3; since key2 is now the least recently used, it should be evicted
	c.Set("key3", "val3")

	_, found1 := c.Get("key1")
	if !found1 {
		t.Error("expected key1 to remain in cache due to updated recency")
	}

	_, found2 := c.Get("key2")
	if found2 {
		t.Error("expected key2 to be evicted as the least recently used item")
	}
}

// TestShardingCorrectness ensures keys are consistently routed to the correct shards
// based on their FNV-1a hash value.
func TestShardingCorrectness(t *testing.T) {
	sc := NewShardedCache(4, 10)

	// We'll assert that different keys are routed to different shard buckets based on FNV-1a hashing.
	keys := []string{"foo", "bar", "baz", "qux", "key123", "another-key"}
	shardIndexMap := make(map[int]bool)

	for _, key := range keys {
		idx := sc.getShardIndex(key)
		shardIndexMap[idx] = true
		if idx < 0 || idx >= len(sc.shards) {
			t.Errorf("key %q routed to invalid shard index %d", key, idx)
		}
	}

	// With FNV-1a and 6 distinct keys, we expect them to be distributed across multiple shards.
	if len(shardIndexMap) < 2 {
		t.Errorf("poor sharding distribution, keys fell into only %d shard(s)", len(shardIndexMap))
	}
}

// TestShardedCacheConcurrency runs a high-concurrency stress test with multiple goroutines.
func TestShardedCacheConcurrency(t *testing.T) {
	const (
		numGoroutines = 100
		numOps        = 1000
		numShards     = 16
		shardCap      = 100
	)

	sc := NewShardedCache(numShards, shardCap)
	var wg sync.WaitGroup

	// Random seed initialization (initialized locally inside goroutines)

	// Pre-populate cache to allow Hits
	for i := 0; i < 50; i++ {
		sc.Set(fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i))
	}

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(int64(id + int(time.Now().UnixNano()))))
			for j := 0; j < numOps; j++ {
				keyID := localRng.Intn(200)
				key := fmt.Sprintf("key_%d", keyID)
				if localRng.Float64() < 0.5 {
					// 50% Write
					sc.Set(key, fmt.Sprintf("val_%d_%d", id, j))
				} else {
					// 50% Read
					sc.Get(key)
				}
			}
		}(i)
	}

	wg.Wait()
	// If the test completes without panic/deadlock, RWMutex sharding and safety logic succeeded.
}

// BenchmarkShardedCacheGet measures cache read operations under heavy contention.
func BenchmarkShardedCacheGet(b *testing.B) {
	sc := NewShardedCache(16, 1000)
	for i := 0; i < 1000; i++ {
		sc.Set(fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i%1000)
			sc.Get(key)
			i++
		}
	})
}

// BenchmarkShardedCacheSet measures cache write operations under heavy contention.
func BenchmarkShardedCacheSet(b *testing.B) {
	sc := NewShardedCache(16, 1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i%1000)
			sc.Set(key, "value")
			i++
		}
	})
}
