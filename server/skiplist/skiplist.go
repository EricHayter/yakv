package main

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
 * Each skiplist node contains a key-value pair of strings and a list of
 * pointers to the next node at each level. A node's next list only contains
 * entries up to its own height. For example, if a node has a max level of 3,
 * it will have 4 entries in the next list (levels 0-3), even if other nodes
 * in the skiplist have higher levels.
 */

import (
	"iter"
	"math/rand"
)

const maxLevel = 32

type SkipList struct {
	promoteProbability float32
	head           	   *skipListNode // head of list (sentinel node)
	size               int
}

type skipListNode struct {
	key, value string
	next       []*skipListNode
}

func (list *SkipList) randomLevel() int {
	level := 0
	for level < maxLevel-1 && rand.Float32() <= list.promoteProbability {
		level++
	}
	return level
}

func (list *SkipList) Size() int {
	return list.size
}

// Insert adds or updates a key-value pair in the skiplist.
// If the key already exists, its value is updated.
// There are NO duplicate keys in the skiplist.
func (list *SkipList) Insert(key, value string) {
	insertLevel := list.randomLevel()

	// Track predecessor nodes at each level
	update := make([]*skipListNode, maxLevel)
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
	newNode := &skipListNode{
		key:   key,
		value: value,
		next:  make([]*skipListNode, insertLevel+1),
	}

	// Insert node at each level
	for level := 0; level <= insertLevel; level++ {
		newNode.next[level] = update[level].next[level]
		update[level].next[level] = newNode
	}

	list.size++
}

func (list *SkipList) Delete(key string) bool {
	update := make([]*skipListNode, maxLevel)
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

func (list *SkipList) Get(key string) (string, bool) {
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

	return "", false
}

func (list *SkipList) Items() iter.Seq[*skipListNode] {
	return func(yield func(*skipListNode) bool) {
		// Start from first real node (skip sentinel)
		p := list.head.next[0]
		for p != nil {
			if !yield(p) {
				return
			}
			p = p.next[0]
		}
	}
}

func NewSkipList() *SkipList {
	// Create sentinel node with maxLevel pointers
	sentinel := &skipListNode{
		key:   "", // Sentinel has no meaningful key
		value: "",
		next:  make([]*skipListNode, maxLevel),
	}

	return &SkipList{
		promoteProbability: 0.5,
		head:           sentinel,
		size:               0,
	}
}
