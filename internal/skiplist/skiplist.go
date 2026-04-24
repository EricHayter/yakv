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
	"sync/atomic"
)

const maxLevel = 32

type SkipList[K cmp.Ordered, V any] struct {
	mu                 sync.RWMutex
	promoteProbability float32
	head               *skipListNode[K, V] // head of list (sentinel node)
	size               int64
}

type skipListNode[K cmp.Ordered, V any] struct {
	key   K
	value V
	next  []*skipListNode[K, V]
	mu    sync.RWMutex
}

func (list *SkipList[K, V]) randomLevel() int {
	level := 0
	for level < maxLevel-1 && rand.Float32() <= list.promoteProbability {
		level++
	}
	return level
}

func (list *SkipList[K, V]) Size() int {
	return int(atomic.LoadInt64(&list.size))
}

func lockPredecessors[K cmp.Ordered, V any](update []*skipListNode[K, V]) []*skipListNode[K, V] {
	unique_nodes := make([]*skipListNode[K, V], 0)
	seen := make(map[*skipListNode[K, V]]bool)
	for _, node := range update {
		if !seen[node] {
			unique_nodes = append(unique_nodes, node)
			node.mu.Lock()
		}
		seen[node] = true
	}
	return unique_nodes
}

func unlockPredecessors[K cmp.Ordered, V any](locked_nodes []*skipListNode[K, V]) {
	for _, node := range locked_nodes {
		node.mu.Unlock()
	}
}

func validate[K cmp.Ordered, V any](update []*skipListNode[K, V], expectedNext []*skipListNode[K, V], insertLevel int) bool {
    // Re-check that each predecessor's next pointer hasn't changed
    for level := 0; level <= insertLevel; level++ {
        if update[level].next[level] != expectedNext[level] {
            return false // Someone inserted/deleted here
        }
    }
    return true
}

// Insert adds or updates a key-value pair in the skiplist.
// If the key already exists, its value is updated.
// There are NO duplicate keys in the skiplist.
//
// Uses optimistic locking for better concurrency:
//  1. Search phase: Traverse the skiplist with read locks (lock crabbing) to find
//     insertion points at each level. Track both predecessors and their expected
//     next pointers.
//  2. Lock phase: Acquire write locks on all predecessor nodes that need updating.
//     The skiplist invariant (top-down traversal) ensures deadlock-free ordering.
//  3. Validate phase: Check that predecessor next pointers haven't changed since
//     the search. If validation fails, someone else modified the structure, so
//     unlock and retry.
//  4. Commit phase: Insert the new node and update pointers atomically while
//     holding locks.
//
// This approach provides much better parallelism than a global lock, which is
// critical for large memtables (e.g., 64 MB) in an LSM tree.
func (list *SkipList[K, V]) Insert(key K, value V) {
	for {
		insertLevel := list.randomLevel()

		// Track predecessor nodes and expected next pointers at each level
		update := make([]*skipListNode[K, V], maxLevel)
		expectedNext := make([]*skipListNode[K, V], maxLevel)

		p := list.head
		p.mu.RLock()

		// Find insertion point and track predecessors
		for level := maxLevel - 1; level >= 0; level-- {
			for p.next[level] != nil {
				next := p.next[level]
				next.mu.RLock()
				if next.key < key {
					old := p
					p = next
					old.mu.RUnlock()
				} else {
					next.mu.RUnlock()
					break
				}
			}
			update[level] = p
			expectedNext[level] = p.next[level]
		}
		p.mu.RUnlock()

		locked_nodes := lockPredecessors(update)
		if !validate(update, expectedNext, insertLevel) {
			unlockPredecessors(locked_nodes)
			continue
		}

		// Check if key already exists (p.next[0] is the potential match)
		if p.next[0] != nil && p.next[0].key == key {
			p.next[0].value = value
			unlockPredecessors(locked_nodes)
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

		atomic.AddInt64(&list.size, 1)
		unlockPredecessors(locked_nodes)
		return
	}
}

// Delete removes a key-value pair from the skiplist.
// Returns true if the key was found and deleted, false otherwise.
//
// Currently uses a global write lock for simplicity. Unlike Insert, this has not
// been converted to use optimistic locking with per-node locks.
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

	atomic.AddInt64(&list.size, -1)
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

// Items returns an iterator over all key-value pairs in the skiplist in sorted order.
//
// Uses lock crabbing (hand-over-hand locking) to allow concurrent iteration with
// inserts/deletes. This provides a "fuzzy snapshot" where you may see partial results
// of concurrent modifications, but guarantees forward progress and no crashes.
//
// Example usage:
//
//	for key, value := range list.Items() {
//	    // process key, value
//	}
func (list *SkipList[K, V]) Items() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		// Start at head with lock
		p := list.head
		p.mu.RLock()

		// Traverse level 0 with lock crabbing
		for p.next[0] != nil {
			next := p.next[0]
			next.mu.RLock()

			// Yield the next node's data while holding its lock
			if !yield(next.key, next.value) {
				next.mu.RUnlock()
				p.mu.RUnlock()
				return
			}

			// Move forward: unlock old, advance to next
			p.mu.RUnlock()
			p = next
		}

		// Unlock final node
		p.mu.RUnlock()
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
