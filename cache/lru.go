package cache

import (
	"container/list"
)

// LRUCache represents a non-thread-safe LRU cache.
type LRUCache struct {
	capacity  int
	evictList *list.List
	items     map[string]*list.Element
}

// entry is used to store key-value pairs inside the list elements
type entry struct {
	key   string
	value interface{}
}

// NewLRUCache instantiates a new LRUCache with the given capacity.
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity:  capacity,
		evictList: list.New(),
		items:     make(map[string]*list.Element),
	}
}

// Get retrieves a key's value from the cache, moving it to the front (marking it recent).
func (c *LRUCache) Get(key string) (interface{}, bool) {
	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		return elem.Value.(*entry).value, true
	}
	return nil, false
}

// Set adds or updates a key-value pair in the cache, moving it to the front.
// If the capacity is exceeded, it evicts the least recently used element.
func (c *LRUCache) Set(key string, value interface{}) {
	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		elem.Value.(*entry).value = value
		return
	}

	// Create new entry
	ent := &entry{key: key, value: value}
	elem := c.evictList.PushFront(ent)
	c.items[key] = elem

	// Evict if capacity exceeded
	if c.evictList.Len() > c.capacity {
		c.evictOldest()
	}
}

// evictOldest removes the oldest item from the cache.
func (c *LRUCache) evictOldest() {
	elem := c.evictList.Back()
	if elem != nil {
		c.evictList.Remove(elem)
		ent := elem.Value.(*entry)
		delete(c.items, ent.key)
	}
}
