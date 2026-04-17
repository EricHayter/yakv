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
	"fmt"
	"github.com/EricHayter/yakv/server/lsm/sstable"
	"github.com/EricHayter/yakv/server/lsm/types"
	"github.com/EricHayter/yakv/server/storage_manager"
)

type flushCallback = func(storage_manager.FileId, error)

type flushQueue struct {
	cb             flushCallback
	storageManager *storage_manager.StorageManager
	head           *flushQueueElement
	tail           *flushQueueElement
}

type flushQueueElement struct {
	memtable   *types.Memtable
	next, prev *flushQueueElement
}

func newFlushQueue(storageManager *storage_manager.StorageManager, cb flushCallback) *flushQueue {
	return &flushQueue{
		cb:             cb,
		storageManager: storageManager,
	}
}

func (fq *flushQueue) PushBack(memtable *types.Memtable) {
	e := &flushQueueElement{
		memtable: memtable,
		next:     fq.tail,
		prev:     nil,
	}

	if fq.tail != nil {
		fq.tail.prev = e
	} else {
		fq.head = e
	}
	fq.tail = e

	go func() {
		// don't think I need a lock for reads of the memtable.
		// since those are read only.
		// pointers will update though so we can't access those.
		fileId, err := sstable.CreateNew(fq.storageManager, e.memtable)
		if err != nil {
			fq.cb(0, fmt.Errorf("failed to create sstable from memtable: %w", err))
			return
		}

		// Update links
		if e.prev != nil {
			e.prev.next = e.next
		} else {
			// This was head, update queue
			fq.head = e.next
		}

		if e.next != nil {
			e.next.prev = e.prev
		} else {
			// This was tail, update queue
			fq.tail = e.prev
		}

		fq.cb(fileId, nil)
	}()
}
