package cache

import (
	"hash/fnv"
	"sync"
)

// Shard wraps a single non-thread-safe LRUCache with a dedicated RWMutex.
type Shard struct {
	mu    sync.RWMutex
	cache *LRUCache
}

// ShardedCache manages an array of independent LRU shards.
type ShardedCache struct {
	shards []*Shard
}

// NewShardedCache initializes a ShardedCache with numShards and capacity per shard.
func NewShardedCache(numShards int, shardCapacity int) *ShardedCache {
	shards := make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &Shard{
			cache: NewLRUCache(shardCapacity),
		}
	}
	return &ShardedCache{
		shards: shards,
	}
}

// getShardIndex computes the FNV-1a hash of the key and maps it to a shard index.
func (sc *ShardedCache) getShardIndex(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	hashVal := h.Sum32()
	return int(hashVal % uint32(len(sc.shards)))
}

// Get retrieves a key's value.
// CRITICAL: Must acquire an exclusive write Lock() because reading from LRU
// mutates internal doubly linked list pointers (Moves accessed element to front).
func (sc *ShardedCache) Get(key string) (interface{}, bool) {
	idx := sc.getShardIndex(key)
	shard := sc.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	return shard.cache.Get(key)
}

// Set adds or updates a key's value.
// Acquires an exclusive write Lock() to safely mutate the cache.
func (sc *ShardedCache) Set(key string, value interface{}) {
	idx := sc.getShardIndex(key)
	shard := sc.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.cache.Set(key, value)
}
