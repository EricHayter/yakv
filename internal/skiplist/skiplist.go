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
 * own height.
 *
 * APPEND-ONLY + FIXED-SIZE ARENA: this skiplist is append-only (Insert/update
 * only, no Delete) and fully lock-free. All node memory — the node structs, the
 * per-node next arrays, and the value boxes — is bump-allocated from fixed-size,
 * lock-free arena.Slab pools owned by the skiplist (see internal/arena).
 * Allocation is a single atomic add (no lock). When any pool is exhausted,
 * Insert reports "full" so the caller (the LSM) can seal this memtable and start
 * a fresh one. Free() drops the pools; because they are ordinary GC-managed
 * slices, a reader still holding a node pointer keeps the backing memory alive,
 * so there is no use-after-free hazard and no need for a reclamation lock.
 */

import (
	"cmp"
	"iter"
	rand "math/rand/v2"
	"sync/atomic"

	"github.com/EricHayter/yakv/internal/arena"
)

const maxLevel = 32

// defaultCapacity is the node capacity used by NewSkipList.
const defaultCapacity = 1 << 16

type SkipList[K cmp.Ordered, V any] struct {
	size atomic.Int64

	promoteProbability float32
	head               *skipListNode[K, V] // head of list (sentinel node)

	// Fixed-size, lock-free allocation pools. nodes holds the node structs,
	// links holds the per-node next arrays (sub-sliced out of one backing
	// array), and vals holds the value boxes pointed at by node.val.
	nodes *arena.Slab[skipListNode[K, V]]
	links *arena.Slab[atomic.Pointer[skipListNode[K, V]]]
	vals  *arena.Slab[V]
}

type skipListNode[K cmp.Ordered, V any] struct {
	key  K
	val  atomic.Pointer[V]                    // atomic so Get/Items need no locks for reads
	next []atomic.Pointer[skipListNode[K, V]] // atomic so traversal needs no locks
}

func (list *SkipList[K, V]) randomLevel() int {
	level := 0
	// math/rand/v2 top-level functions are safe for concurrent use and do not
	// serialize on a global mutex, unlike the v1 global source.
	for level < maxLevel-1 && rand.Float32() <= list.promoteProbability {
		level++
	}
	return level
}

func (list *SkipList[K, V]) Size() int {
	return int(list.size.Load())
}

// allocNode bump-allocates a node and its next array. Returns ok == false when
// either pool is exhausted (the skiplist is full).
func (list *SkipList[K, V]) allocNode(height int) (node *skipListNode[K, V], ok bool) {
	node = list.nodes.Get()
	if node == nil {
		return nil, false
	}
	next := list.links.GetN(height)
	if next == nil {
		return nil, false
	}
	node.next = next
	return node, true
}

// allocValue bump-allocates a value box. Returns ok == false when the value pool
// is exhausted.
func (list *SkipList[K, V]) allocValue(value V) (v *V, ok bool) {
	v = list.vals.Get()
	if v == nil {
		return nil, false
	}
	*v = value
	return v, true
}

// findPredecessors traverses the list with atomic loads, returning, for each
// level, the predecessor node of key (update) and the node currently following
// it (expectedNext).
func (list *SkipList[K, V]) findPredecessors(key K) (update, expectedNext [maxLevel]*skipListNode[K, V]) {
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
	return update, expectedNext
}

// Insert adds or updates a key-value pair in the skiplist.
// If the key already exists, its value is updated atomically.
// There are NO duplicate keys in the skiplist.
//
// Returns full == true (WITHOUT applying the change) when an allocation pool is
// exhausted, so the caller can seal this skiplist and retry against a fresh one.
//
// Fully lock-free: arena allocation is a single atomic add, and the level-0 CAS
// is the linearization point. Higher-level links are best-effort index
// shortcuts; a missed link is slow but never incorrect.
func (list *SkipList[K, V]) Insert(key K, value V) (full bool) {
	// The node is allocated lazily: only once we know this is a genuine insert
	// (not an update) do we draw from the pools, so updates never waste a node.
	var newNode *skipListNode[K, V]
	var insertLevel int

	for {
		update, expectedNext := list.findPredecessors(key)

		// Update path: key already present — no new node, just a fresh value box.
		if existing := expectedNext[0]; existing != nil && existing.key == key {
			v, ok := list.allocValue(value)
			if !ok {
				return true
			}
			existing.val.Store(v)
			return false
		}

		// First time through the insert path: allocate the node once.
		if newNode == nil {
			insertLevel = list.randomLevel()
			node, ok := list.allocNode(insertLevel + 1)
			if !ok {
				return true
			}
			v, ok := list.allocValue(value)
			if !ok {
				return true
			}
			node.key = key
			node.val.Store(v)
			newNode = node
		}

		// Point the new node at its successors at every level before committing.
		for level := 0; level <= insertLevel; level++ {
			newNode.next[level].Store(expectedNext[level])
		}

		// Commit at level 0. A failure means a concurrent insert changed the
		// predecessor; re-search and retry.
		if !update[0].next[0].CompareAndSwap(expectedNext[0], newNode) {
			continue
		}

		// Higher-level linking: best-effort index shortcuts.
		for level := 1; level <= insertLevel; level++ {
			for {
				newNode.next[level].Store(expectedNext[level])
				if update[level].next[level].CompareAndSwap(expectedNext[level], newNode) {
					break
				}
				update, expectedNext = list.findPredecessors(key)
			}
		}

		list.size.Add(1)
		return false
	}
}

// Get is fully lock-free. next pointers and values are stored as atomic.Pointer,
// so concurrent Inserts are safe to race with.
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

// Free drops the skiplist's allocation pools so the garbage collector can
// reclaim them. It is safe to call while readers race: a reader holding a node
// pointer keeps that backing array alive until it is finished, so there is no
// use-after-free. After Free the skiplist must not be inserted into again.
func (list *SkipList[K, V]) Free() {
	if list.nodes != nil {
		list.nodes.Reset()
	}
	if list.links != nil {
		list.links.Reset()
	}
	if list.vals != nil {
		list.vals.Reset()
	}
}

// NewSkipList creates an empty skiplist with the default node capacity.
func NewSkipList[K cmp.Ordered, V any]() *SkipList[K, V] {
	return NewSkipListWithCapacity[K, V](defaultCapacity)
}

// NewSkipListWithCapacity creates an empty skiplist sized to hold up to
// nodeCapacity distinct keys before Insert reports full. The link pool is sized
// for the expected total number of level pointers (plus the sentinel), and the
// value pool for one value box per node. Updates also consume value-box
// capacity, so update-heavy workloads may report full before nodeCapacity keys
// are reached — which simply seals the memtable a little earlier.
func NewSkipListWithCapacity[K cmp.Ordered, V any](nodeCapacity int) *SkipList[K, V] {
	if nodeCapacity < 1 {
		nodeCapacity = 1
	}
	list := &SkipList[K, V]{
		promoteProbability: 0.5,
		// Expected level pointers per node with p=0.5 is ~2; add maxLevel for
		// the sentinel head.
		nodes: arena.NewSlab[skipListNode[K, V]](nodeCapacity + 1),
		links: arena.NewSlab[atomic.Pointer[skipListNode[K, V]]](nodeCapacity*2 + maxLevel),
		vals:  arena.NewSlab[V](nodeCapacity),
	}
	// Allocate the sentinel head from the pools (single-threaded at construction).
	head, _ := list.allocNode(maxLevel)
	list.head = head
	return list
}
