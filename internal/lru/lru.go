package lru

import (
	"container/list"
	"sync"
)

// Cache is a thread-safe LRU cache that tracks items by ID.
// When the cache is full and a new item is added, it returns the ID of the evicted item.
type Cache[K comparable] struct {
	mu       sync.Mutex
	capacity int
	cache    map[K]*list.Element
	lru      *list.List
}

type entry[K comparable] struct {
	key K
}

// New creates a new LRU cache with the specified capacity.
func New[K comparable](capacity int) *Cache[K] {
	return &Cache[K]{
		capacity: capacity,
		cache:    make(map[K]*list.Element),
		lru:      list.New(),
	}
}

// Get retrieves an item from the cache and marks it as recently used.
// Returns true if the item was found, false otherwise.
func (c *Cache[K]) Get(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.lru.MoveToFront(elem)
		return true
	}
	return false
}

// Put adds an item to the cache.
// If the cache is full, it returns the key of the evicted item and true.
// If no eviction occurred, it returns the zero value of K and false.
func (c *Cache[K]) Put(key K) (evicted K, wasEvicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If key already exists, just mark as recently used
	if elem, ok := c.cache[key]; ok {
		c.lru.MoveToFront(elem)
		return
	}

	// Add new entry
	elem := c.lru.PushFront(entry[K]{key: key})
	c.cache[key] = elem

	// Evict if over capacity
	if c.lru.Len() > c.capacity {
		oldest := c.lru.Back()
		if oldest != nil {
			c.lru.Remove(oldest)
			e := oldest.Value.(entry[K])
			delete(c.cache, e.key)
			return e.key, true
		}
	}

	return
}

// Remove removes an item from the cache.
// Returns true if the item was found and removed, false otherwise.
func (c *Cache[K]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.lru.Remove(elem)
		delete(c.cache, key)
		return true
	}
	return false
}

// Len returns the current number of items in the cache.
func (c *Cache[K]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// Clear removes all items from the cache.
func (c *Cache[K]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Init()
	c.cache = make(map[K]*list.Element)
}
