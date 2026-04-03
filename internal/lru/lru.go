package lru

import (
	"container/list"
	"sync"
)

// Replacer is a thread-safe LRU replacer that tracks items by key.
// It helps determine which item should be evicted based on least-recently-used policy.
type Replacer[K comparable] struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    map[K]*list.Element
	lru      *list.List
}

type entry[K comparable] struct {
	key K
}

// New creates a new LRU replacer
func New[K comparable]() *Replacer[K] {
	r := &Replacer[K]{
		items:    make(map[K]*list.Element),
		lru:      list.New(),
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Get marks an item as recently used.
// Returns true if the item was found, false otherwise.
func (r *Replacer[K]) Get(key K) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if elem, ok := r.items[key]; ok {
		r.lru.MoveToFront(elem)
		return true
	}
	return false
}

// Push adds an item or marks it as recently used if it already exists.
func (r *Replacer[K]) Push(key K) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If key already exists, just mark as recently used
	if elem, ok := r.items[key]; ok {
		r.lru.MoveToFront(elem)
		return
	}

	// Add new entry
	elem := r.lru.PushFront(entry[K]{key: key})
	r.items[key] = elem

	// Signal that an item is available
	r.cond.Signal()
}

// Pop removes and returns the least recently used item.
// Blocks until an item is available.
func (r *Replacer[K]) Pop() K {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Wait until there's an item available
	for r.lru.Len() == 0 {
		r.cond.Wait()
	}

	oldest := r.lru.Back()
	r.lru.Remove(oldest)
	e := oldest.Value.(entry[K])
	delete(r.items, e.key)
	return e.key
}

// Remove removes an item from the replacer.
// Returns true if the item was found and removed, false otherwise.
func (r *Replacer[K]) Remove(key K) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if elem, ok := r.items[key]; ok {
		r.lru.Remove(elem)
		delete(r.items, key)
		return true
	}
	return false
}

// Len returns the current number of items in the replacer.
func (r *Replacer[K]) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lru.Len()
}

// Clear removes all items from the replacer.
func (r *Replacer[K]) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lru.Init()
	r.items = make(map[K]*list.Element)
}
