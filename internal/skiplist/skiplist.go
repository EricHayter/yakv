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
	mu                 sync.Mutex // serializes concurrent Deletes
	promoteProbability float32
	head               *skipListNode[K, V] // head of list (sentinel node)
	size               int64
}

type skipListNode[K cmp.Ordered, V any] struct {
	key  K
	val  atomic.Pointer[V]                        // atomic so Get/Items need no locks for reads
	next []atomic.Pointer[skipListNode[K, V]]     // atomic so traversal needs no locks
	mu   sync.Mutex                               // held only during Insert commit phase
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

// lockPredecessors acquires write locks left to right (highest index first in
// update, which corresponds to the leftmost/head-side nodes). Iterating from
// update[maxLevel-1] down to update[0] ensures all concurrent Insert commits
// acquire overlapping predecessor locks in the same order, preventing deadlock.
func lockPredecessors[K cmp.Ordered, V any](update []*skipListNode[K, V]) []*skipListNode[K, V] {
	unique_nodes := make([]*skipListNode[K, V], 0)
	seen := make(map[*skipListNode[K, V]]bool)
	for i := len(update) - 1; i >= 0; i-- {
		node := update[i]
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
	for level := 0; level <= insertLevel; level++ {
		if update[level].next[level].Load() != expectedNext[level] {
			return false
		}
	}
	return true
}

// Insert adds or updates a key-value pair in the skiplist.
// If the key already exists, its value is updated.
// There are NO duplicate keys in the skiplist.
//
// Uses optimistic locking for better concurrency:
//  1. Search phase: Traverse the skiplist using atomic loads — no locks needed
//     since next pointers are atomic.Pointer. Track predecessors and their
//     expected next pointers at each level.
//  2. Lock phase: Acquire write locks on all predecessor nodes that need updating,
//     left to right (head-side first) to prevent deadlock between concurrent inserts.
//  3. Validate phase: Check that predecessor next pointers haven't changed since
//     the search. If validation fails, someone else modified the structure, so
//     unlock and retry.
//  4. Commit phase: Link in the new node with atomic stores while holding locks.
//
// This approach provides much better parallelism than a global lock, which is
// critical for large memtables (e.g., 64 MB) in an LSM tree.
func (list *SkipList[K, V]) Insert(key K, value V) {
	for {
		insertLevel := list.randomLevel()

		update := make([]*skipListNode[K, V], maxLevel)
		expectedNext := make([]*skipListNode[K, V], maxLevel)

		// Search phase: atomic loads, no locking. The validate step catches any
		// structural changes that raced with this traversal before we commit.
		p := list.head
		for level := maxLevel - 1; level >= 0; level-- {
			for {
				next := p.next[level].Load()
				if next == nil || next.key >= key {
					break
				}
				p = next
			}
			update[level] = p
			expectedNext[level] = p.next[level].Load()
		}

		locked_nodes := lockPredecessors(update)
		if !validate(update, expectedNext, insertLevel) {
			unlockPredecessors(locked_nodes)
			continue
		}

		// Check if key already exists
		existing := expectedNext[0]
		if existing != nil && existing.key == key {
			existing.val.Store(&value)
			unlockPredecessors(locked_nodes)
			return
		}

		// Create and link new node
		newNode := &skipListNode[K, V]{
			key:  key,
			next: make([]atomic.Pointer[skipListNode[K, V]], insertLevel+1),
		}
		newNode.val.Store(&value)

		for level := 0; level <= insertLevel; level++ {
			newNode.next[level].Store(expectedNext[level])
			update[level].next[level].Store(newNode)
		}

		atomic.AddInt64(&list.size, 1)
		unlockPredecessors(locked_nodes)
		return
	}
}

// Delete removes a key-value pair from the skiplist.
// Returns true if the key was found and deleted, false otherwise.
//
// Uses a global write lock to serialize concurrent deletes. The pointer
// updates use atomic stores so concurrent lock-free reads (Get, Items) remain
// safe without taking any lock.
func (list *SkipList[K, V]) Delete(key K) bool {
	list.mu.Lock()
	defer list.mu.Unlock()

	update := make([]*skipListNode[K, V], maxLevel)
	p := list.head

	for level := maxLevel - 1; level >= 0; level-- {
		for {
			next := p.next[level].Load()
			if next == nil || next.key >= key {
				break
			}
			p = next
		}
		update[level] = p
	}

	target := p.next[0].Load()
	if target == nil || target.key != key {
		return false
	}

	for level := 0; level < len(target.next); level++ {
		update[level].next[level].Store(target.next[level].Load())
	}

	atomic.AddInt64(&list.size, -1)
	return true
}

// Get is fully lock-free. next pointers and values are stored as atomic.Pointer,
// so concurrent Inserts and Deletes are safe to race with.
func (list *SkipList[K, V]) Get(key K) (V, bool) {
	p := list.head
	for level := maxLevel - 1; level >= 0; level-- {
		for {
			next := p.next[level].Load()
			if next == nil || next.key >= key {
				break
			}
			p = next
		}
	}

	next := p.next[0].Load()
	if next != nil && next.key == key {
		return *next.val.Load(), true
	}
	var zero V
	return zero, false
}

// Items returns an iterator over all key-value pairs in the skiplist in sorted order.
//
// Uses atomic loads throughout so no locks are needed. This provides a "fuzzy
// snapshot": you may see partial results of concurrent modifications, but
// forward progress and crash-safety are guaranteed.
//
// Example usage:
//
//	for key, value := range list.Items() {
//	    // process key, value
//	}
func (list *SkipList[K, V]) Items() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		p := list.head.next[0].Load()
		for p != nil {
			if !yield(p.key, *p.val.Load()) {
				return
			}
			p = p.next[0].Load()
		}
	}
}

func NewSkipList[K cmp.Ordered, V any]() *SkipList[K, V] {
	sentinel := &skipListNode[K, V]{
		next: make([]atomic.Pointer[skipListNode[K, V]], maxLevel),
	}
	return &SkipList[K, V]{
		promoteProbability: 0.5,
		head:               sentinel,
	}
}
