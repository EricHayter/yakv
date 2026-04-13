package lsm

/* When memtables are entirely filled up and ready to flush we must replace
 * them with a new memtable. There are two options for handling this:
 *
 * 1. a naive approach: once we are ready to flush the memtable to disk (in an
 * sstable), we block ALL transactions on the LSM until the memtable is flushed
 * to disk.
 *
 * This solution is simple but this would require creating a new file and
 * performing lots of page writes which would take a considerable amount of
 * time.
 *
 * 2. async flush approach: create a new memtable immediately and maintain the
 * old memtables as read-only while the memtable is asynchronously flushed via
 * a go routine.
 *
 * we will be implementing the second option. This file in particular will
 * design the queue used to handle memtables in the transitory state of being
 * in the process of being flushed.
 *
 * we first create a list of nodes containing the memtables that are to are
 * being flushed.
 */

import (
	"sync"
	"github.com/EricHayter/yakv/server/lsm/types"
	"github.com/EricHayter/yakv/server/lsm/sstable"
	"github.com/EricHayter/yakv/server/storage_manager"
)

type flushCallback = func (storage_manager.FileId, error)

type flushQueue struct {
	cb flushCallback
	storageManager  *storage_manager.StorageManager
	mut sync.RWMutex
	head *flushQueueElement
	tail *flushQueueElement
}

type flushQueueElement struct {
	mut sync.RWMutex
	memtable *types.Memtable
	next, prev *flushQueueElement
}


func newFlushQueue(storageManager *storage_manager.StorageManager, cb flushCallback) *flushQueue {
	return &flushQueue{
		cb: cb,
		storageManager: storageManager,
	}
}

func (e *flushQueueElement) Next() *flushQueueElement { return e.next }
func (e *flushQueueElement) Lock() { e.mut.Lock() }
func (e *flushQueueElement) Unlock() { e.mut.Unlock() }
func (e *flushQueueElement) Data() *types.Memtable { return e.memtable }

func (fq *flushQueue) PushBack(memtable *types.Memtable) {
	e := &flushQueueElement{
		memtable: memtable,
		next: fq.tail,
		prev: nil,
	}

	fq.mut.Lock()
	if fq.tail != nil {
		fq.tail.Lock()
		fq.tail.prev = e
		fq.tail.Unlock()
	} else {
		// First element - also becomes head
		fq.head = e
	}
	fq.tail = e
	fq.mut.Unlock()

	go func() {
		// don't think I need a lock for reads of the memtable.
		// since those are read only.
		// pointers will update though so we can't access those.
		fileId, err := sstable.CreateNew(fq.storageManager, e.Data())
		if err != nil {
			fq.cb(0, err)
			return
		}

		// Remove self from the list
		// Lock in traversal order: prev -> self -> next
		if e.prev != nil {
			e.prev.Lock()
			defer e.prev.Unlock()
		}
		e.Lock()
		defer e.Unlock()
		if e.next != nil {
			e.next.Lock()
			defer e.next.Unlock()
		}

		// Update links
		if e.prev != nil {
			e.prev.next = e.next
		} else {
			// This was head, update queue
			fq.mut.Lock()
			fq.head = e.next
			fq.mut.Unlock()
		}

		if e.next != nil {
			e.next.prev = e.prev
		} else {
			// This was tail, update queue
			fq.mut.Lock()
			fq.tail = e.prev
			fq.mut.Unlock()
		}

		fq.cb(fileId, nil)
	}()
}

// Returns the tail end of the qeue in a readlock state
func (fq *flushQueue) Tail() *flushQueueElement {
	fq.mut.RLock()
	defer fq.mut.RUnlock()
	return fq.tail
}

// Returns the head end of the qeue in a readlock state
func (fq *flushQueue) Head() *flushQueueElement {
	fq.mut.RLock()
	defer fq.mut.RUnlock()
	return fq.head
}
