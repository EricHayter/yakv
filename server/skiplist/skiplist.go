package main

/* The Key Value server will be using an LSM storage engine. As such, for the
 * implementation of the memtables I will be using a skiplist.
 *
 * the skiplist struct itself is pretty simple. It's a pointer to the first
 * key-value (kv) pair in the skiplist and a probability which is used in the
 * insert method to determine which level a new value should be inserted at.
 *
 * the first node in the list (the head) will always have the maximimum
 * (non-strict) height of the skiplist. Therefore to determine the height
 * of the skiplist the first node can be used and the following nodes can be
 * assumed to have >= its height.
 *
 * Each skiplist node contains a key, value pair of strings, and a list of
 * pointers to the next node at that given level. Therefore the next list
 * will only contain entries for pointers up to it's own respective height.
 * for example if a node has a max level of say 3 it will have 4 entries in the
 * next list even if the skiplist has a global max height of say 7.
 */

import (
	"iter"
	"math/rand"
)

const maxLevel = 32

type SkipList struct {
	promoteProbability float32
	head               *skipListNode
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

/* NOTE: this function WILL update the value of nodes with the same key. i.e.
 * there are NO duplicates keys in the skiplist.
 */
func (list *SkipList) Insert(key, value string) {
	insertLevel := list.randomLevel()

	insertedNode := &skipListNode{
		key:   key,
		value: value,
		next:  make([]*skipListNode, insertLevel+1),
	}

	// Empty list case
	if list.head == nil {
		list.head = insertedNode
		list.size++
		return
	}

	// Update head if key matches
	if list.head.key == key {
		list.head.value = value
		return
	}

	// New head of list
	if key < list.head.key {
		oldListHeight := len(list.head.next)
		maxHeight := max(insertLevel+1, oldListHeight)

		// Reallocate next array to accommodate max height
		insertedNode.next = make([]*skipListNode, maxHeight)

		// Point all levels to the old head
		for height := range oldListHeight {
			insertedNode.next[height] = list.head
		}

		list.head = insertedNode
		list.size++
		return
	}

	numLevels := len(list.head.next)
	// height is not big enough to accomodate for that height. e.g. level 0
	// requires a height of ast least 1.
	for numLevels <= insertLevel {
		list.head.next = append(list.head.next, nil)
		numLevels++
	}

	// Find the location of where the key should be placed
	p := list.head

	level := numLevels - 1
	for level >= 0 {
		// Find the node at this level that we will insert the new value after
		// (or update)
		for p.next[level] != nil && key >= p.next[level].key {
			p = p.next[level]
		}

		// Update if the key already exists
		if p.key == key {
			p.value = value
			return
		}

		if level <= insertLevel {
			insertedNode.next[level] = p.next[level]
			p.next[level] = insertedNode
		}

		level -= 1
	}

	list.size++
}

func (list *SkipList) Delete(key string) bool {
	if list.head == nil {
		return false
	}

	numLevels := len(list.head.next)
	level := numLevels - 1
	p := list.head

	// Special case: first kv pair in list is the node to delete
	if p.key == key {
		secondElement := p.next[0]
		if secondElement != nil {
			for height := len(secondElement.next); height < len(p.next); height++ {
				secondElement.next = append(secondElement.next, p.next[height])
			}
		}
		list.head = secondElement
		list.size--
		return true
	}

	deleted := false
	for level >= 0 {
		// Move p right until the next pointer is null OR the next pointer
		// is larger than the deletion key
		for p.next[level] != nil && key > p.next[level].key {
			p = p.next[level]
		}

		// Three cases for stopping the above loop:
		// 1. next is nil (in which case we just move down)
		// 2. next is strictly bigger than our key (move down as well)
		// 3. next is our kv pair we want to delete

		// Handle the matched case
		if p.next[level] != nil && p.next[level].key == key {
			deleted = true
			p.next[level] = p.next[level].next[level]
		}

		level -= 1
	}
	if deleted {
		list.size--
	}
	return deleted
}

func (list *SkipList) Get(key string) (string, bool) {
	if list.head == nil {
		return "", false
	}

	if list.head.key == key {
		return list.head.value, true
	}

	numLevels := len(list.head.next)
	p := list.head
	height := numLevels - 1
	for height >= 0 {
		// Move p right until the next pointer is null OR the next pointer
		// is larger than the search key
		for p.next[height] != nil && key >= p.next[height].key {
			p = p.next[height]
		}

		if p.key == key {
			return p.value, true
		}

		// Check the level below
		height -= 1
	}
	return "", false
}

func (list *SkipList) Items() iter.Seq[*skipListNode] {
	return func(yield func(*skipListNode) bool) {
		p := list.head
		for p != nil {
			if !yield(p) {
				return
			}
			p = p.next[0]
		}
	}
}

func NewSkipList() *SkipList {
	return &SkipList{
		promoteProbability: 0.5,
		head:               nil,
		size:               0,
	}
}
