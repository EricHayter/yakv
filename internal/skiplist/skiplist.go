package skiplist

/* The Key Value server will be using an LSM storage engine. As such, for the
 * implementation of the memtables I will be using a skiplist.
 *
 * The skiplist struct contains a pointer to a sentinel node which acts as the
 * head of the list. The sentinel node does not contain any actual key-value data
 * and serves only as a starting point for traversal. This simplifies the logic
 * by eliminating special cases for empty lists and head insertion.
 *
 * The sentinel node has maxLevel pointers (currently 32), allowing the skiplist
 * to accommodate nodes at any level without dynamic height adjustments.
 *
 * Each skiplist node contains a key-value pair and a list of pointers to the
 * next node at each level. A node's next list only contains entries up to its
 * own height. For example, if a node has a max level of 3, it will have 4
 * entries in the next list (levels 0-3), even if other nodes in the skiplist
 * have higher levels.
 */

import (
	"cmp"
	"iter"
	"math/rand"
	"sync"
)

const maxLevel = 32

type SkipList[K cmp.Ordered, V any] struct {
	mu                 sync.RWMutex
	promoteProbability float32
	head               *skipListNode[K, V] // head of list (sentinel node)
	size               int
}

type skipListNode[K cmp.Ordered, V any] struct {
	key   K
	value V
	next  []*skipListNode[K, V]
}

func (list *SkipList[K, V]) randomLevel() int {
	level := 0
	for level < maxLevel-1 && rand.Float32() <= list.promoteProbability {
		level++
	}
	return level
}

func (list *SkipList[K, V]) Size() int {
	list.mu.RLock()
	defer list.mu.RUnlock()
	return list.size
}

// Lock acquires a write lock on the skiplist
func (list *SkipList[K, V]) Lock() {
	list.mu.Lock()
}

// Unlock releases a write lock on the skiplist
func (list *SkipList[K, V]) Unlock() {
	list.mu.Unlock()
}

// RLock acquires a read lock on the skiplist
func (list *SkipList[K, V]) RLock() {
	list.mu.RLock()
}

// RUnlock releases a read lock on the skiplist
func (list *SkipList[K, V]) RUnlock() {
	list.mu.RUnlock()
}

// Insert adds or updates a key-value pair in the skiplist.
// If the key already exists, its value is updated.
// There are NO duplicate keys in the skiplist.
func (list *SkipList[K, V]) Insert(key K, value V) {
	list.mu.Lock()
	defer list.mu.Unlock()

	insertLevel := list.randomLevel()

	// Track predecessor nodes at each level
	update := make([]*skipListNode[K, V], maxLevel)
	p := list.head

	// Find insertion point and track predecessors
	for level := maxLevel - 1; level >= 0; level-- {
		for p.next[level] != nil && p.next[level].key < key {
			p = p.next[level]
		}
		update[level] = p
	}

	// Check if key already exists (p.next[0] is the potential match)
	if p.next[0] != nil && p.next[0].key == key {
		p.next[0].value = value
		return
	}

	// Create new node
	newNode := &skipListNode[K, V]{
		key:   key,
		value: value,
		next:  make([]*skipListNode[K, V], insertLevel+1),
	}

	// Insert node at each level
	for level := 0; level <= insertLevel; level++ {
		newNode.next[level] = update[level].next[level]
		update[level].next[level] = newNode
	}

	list.size++
}

func (list *SkipList[K, V]) Delete(key K) bool {
	list.mu.Lock()
	defer list.mu.Unlock()

	update := make([]*skipListNode[K, V], maxLevel)
	p := list.head

	// Find the node and track predecessors
	for level := maxLevel - 1; level >= 0; level-- {
		for p.next[level] != nil && p.next[level].key < key {
			p = p.next[level]
		}
		update[level] = p
	}

	// Check if the key exists
	target := p.next[0]
	if target == nil || target.key != key {
		return false
	}

	// Remove node from all levels
	for level := 0; level < len(target.next); level++ {
		update[level].next[level] = target.next[level]
	}

	list.size--
	return true
}

func (list *SkipList[K, V]) Get(key K) (V, bool) {
	list.mu.RLock()
	defer list.mu.RUnlock()

	p := list.head

	// Search from top level down
	for level := maxLevel - 1; level >= 0; level-- {
		for p.next[level] != nil && p.next[level].key < key {
			p = p.next[level]
		}
	}

	// Check if we found the key
	p = p.next[0]
	if p != nil && p.key == key {
		return p.value, true
	}

	var zero V
	return zero, false
}

// Items returns an iterator over all key-value pairs in the skiplist.
// The caller must hold a read lock (via RLock/RUnlock) for the duration of iteration
// to ensure thread safety.
//
// Example usage:
//
//	list.RLock()
//	defer list.RUnlock()
//	for key, value := range list.Items() {
//	    // process key, value
//	}
func (list *SkipList[K, V]) Items() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		// Start from first real node (skip sentinel)
		p := list.head.next[0]
		for p != nil {
			if !yield(p.key, p.value) {
				return
			}
			p = p.next[0]
		}
	}
}

func NewSkipList[K cmp.Ordered, V any]() *SkipList[K, V] {
	// Create sentinel node with maxLevel pointers
	sentinel := &skipListNode[K, V]{
		key:   *new(K), // Zero value of K
		value: *new(V), // Zero value of V
		next:  make([]*skipListNode[K, V], maxLevel),
	}

	return &SkipList[K, V]{
		promoteProbability: 0.5,
		head:               sentinel,
		size:               0,
	}
}
