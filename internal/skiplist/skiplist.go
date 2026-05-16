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
	val  atomic.Pointer[V]                    // atomic so Get/Items need no locks for reads
	next []atomic.Pointer[skipListNode[K, V]] // atomic so traversal needs no locks
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

// Insert adds or updates a key-value pair in the skiplist.
// If the key already exists, its value is updated atomically.
// There are NO duplicate keys in the skiplist.
//
// Uses CAS-based lock-free insertion:
//  1. Search phase: traverse with atomic loads to find predecessors at each level.
//  2. Commit point: CAS predecessor.next[0] from expectedNext to newNode. The node
//     is logically present in the list as soon as this CAS succeeds.
//  3. Higher-level linking: CAS each level independently. A failure just means a
//     concurrent insert changed the predecessor; re-find it and retry that level.
//     These are index shortcuts — a missed link is slow but never incorrect.
func (list *SkipList[K, V]) Insert(key K, value V) {
	insertLevel := list.randomLevel()

	newNode := &skipListNode[K, V]{
		key:  key,
		next: make([]atomic.Pointer[skipListNode[K, V]], insertLevel+1),
	}
	newNode.val.Store(&value)

	for {
		var update [maxLevel]*skipListNode[K, V]
		var expectedNext [maxLevel]*skipListNode[K, V]

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

		if existing := expectedNext[0]; existing != nil && existing.key == key {
			existing.val.Store(&value)
			return
		}

		for level := 0; level <= insertLevel; level++ {
			newNode.next[level].Store(expectedNext[level])
		}

		if !update[0].next[0].CompareAndSwap(expectedNext[0], newNode) {
			continue
		}

		for level := 1; level <= insertLevel; level++ {
			for {
				newNode.next[level].Store(expectedNext[level])
				if update[level].next[level].CompareAndSwap(expectedNext[level], newNode) {
					break
				}
				p = list.head
				for lvl := maxLevel - 1; lvl >= level; lvl-- {
					for {
						next := p.next[lvl].Load()
						if next == nil || next.key >= key {
							break
						}
						p = next
					}
				}
				update[level] = p
				expectedNext[level] = p.next[level].Load()
			}
		}

		atomic.AddInt64(&list.size, 1)
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
