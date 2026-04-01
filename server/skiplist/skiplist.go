package main

/* The Key Value server will be using an LSM storage engine. As such, for the
 * implementation of the memtables I will be using a skiplist.
 *
 * This file will contain the implementation of the skiplist
 */

import (
	"math/rand"
	"iter"
)

type SkipList struct {
	promoteProbability float32
	head *skipListNode
}

type skipListNode struct {
	key, value string
	next []*skipListNode
}

func (list *SkipList) increaseHeight() {
	if list.head != nil {
		list.head.next = append(list.head.next, nil)
	}
}

// This should update not insert a new one too btw
func (list *SkipList) Insert(key, value string) {
	insertLevel := 0
	for rand.Float32() <= list.promoteProbability {
		insertLevel++
	}

	insertedNode := &skipListNode{
		key: key,
		value: value,
		next: make([]*skipListNode, insertLevel + 1),
	}

	// Empty list case
	if list.head == nil {
		list.head = insertedNode
		return
	}

	if list.head.key == key {
		list.head.value = value
		return
	}

	// new head of list
	if key < list.head.key {
		oldListHeight := len(list.head.next)
		insertedNode.next = make([]*skipListNode, max(insertLevel, oldListHeight))
		for height := range oldListHeight {
			insertedNode.next[height] = list.head
		}
		list.head = insertedNode
		return
	}

	numLevels := len(list.head.next)
	// height is not big enough to accomodate for that height. e.g. level 0
	// requires a height of ast least 1.
	for numLevels <= insertLevel {
		list.increaseHeight()
		numLevels++
	}

	// find the location of where the key should be placed
	p := list.head

	level := numLevels - 1
	for level >= 0 {
		// find the node at this level that we will insert the new value into
		// after (or update)
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
		second_element := p.next[0]
		if second_element != nil {
			for height := len(second_element.next); height < len(p.next); height++ {
				second_element.next = append(second_element.next, p.next[height])
			}
		}
		list.head = second_element
		return true
	}

	for level >= 0 {
		// move p right until the next pointer is null OR the next pointer
		// is larger than the insertion key
		for p.next[level] != nil && key > p.next[level].key {
			p = p.next[level]
		}

		// three cases for stopping the above loop:
		// 1. next is nil (in which case we just move down)
		// 2. next is strictly bigger than our key (move down as well)
		// 3. next is our kv pair we want to delete.

		// we Handle the last case here
		if p.next[level] != nil && p.next[level].key == key {
			p.next[level] = p.next[level].next[level]
		}

		level -= 1
	}
	return false
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
		// move p right until the next pointer is null OR the next pointer
		// is larger than the insertion key
		for p.next[height] != nil && key >= p.next[height].key {
			p = p.next[height]
		}

		if p.key == key {
			return p.value, true
		}

		// check the level bellow
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
		head: nil,
	}
}
