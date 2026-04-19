package lsm

/* Flush Queue - Sequential Memtable Flushing
 *
 * When memtables are full, we need to flush them to disk as SSTables. This
 * module implements the async flush approach:
 *
 * 1. When a memtable is full, replace it immediately with a new one
 * 2. Add the old memtable to the flush queue as read-only
 * 3. A single worker goroutine processes memtables sequentially in FIFO order
 *
 * Design:
 * - Slice holds the FIFO queue (single source of truth)
 * - Signal channel wakes up worker when work is available
 * - Single worker ensures deterministic ordering of SSTables
 * - LSM Get() can search the slice for in-flight memtables
 */

import (
	"fmt"
	"sync"

	"github.com/EricHayter/yakv/server/lsm/sstable"
	"github.com/EricHayter/yakv/server/lsm/types"
	"github.com/EricHayter/yakv/server/storage_manager"
)

type flushCallback = func(storage_manager.FileId, error)

type flushQueue struct {
	cb             flushCallback
	storageManager *storage_manager.StorageManager
	mu             sync.Mutex
	queue          []*types.Memtable // FIFO queue: append to end, process from front
	workChan       chan struct{}     // Signal that work is available
	quit           chan struct{}
	done           chan struct{}
}

func newFlushQueue(storageManager *storage_manager.StorageManager, cb flushCallback) *flushQueue {
	fq := &flushQueue{
		cb:             cb,
		storageManager: storageManager,
		queue:          make([]*types.Memtable, 0),
		workChan:       make(chan struct{}, 10), // Buffered to avoid blocking PushBack
		quit:           make(chan struct{}),
		done:           make(chan struct{}),
	}

	// Start single worker goroutine
	go fq.worker()

	return fq
}

// worker is the single goroutine that processes memtables sequentially in FIFO order
func (fq *flushQueue) worker() {
	defer close(fq.done)

	for {
		select {
		case <-fq.workChan:
			// Get the first memtable from queue (FIFO)
			fq.mu.Lock()
			if len(fq.queue) == 0 {
				fq.mu.Unlock()
				continue
			}
			memtable := fq.queue[0]
			fq.mu.Unlock()

			// Process memtable (don't hold lock during expensive I/O)
			fileId, err := sstable.CreateNew(fq.storageManager, memtable)
			if err != nil {
				fq.cb(0, fmt.Errorf("failed to create sstable from memtable: %w", err))
			} else {
				fq.cb(fileId, nil)
			}

			// Remove from queue
			fq.mu.Lock()
			fq.queue = fq.queue[1:]
			fq.mu.Unlock()

		case <-fq.quit:
			// Drain remaining work before shutting down
			for {
				fq.mu.Lock()
				if len(fq.queue) == 0 {
					fq.mu.Unlock()
					return
				}
				memtable := fq.queue[0]
				fq.queue = fq.queue[1:]
				fq.mu.Unlock()

				fileId, err := sstable.CreateNew(fq.storageManager, memtable)
				if err != nil {
					fq.cb(0, fmt.Errorf("failed to create sstable from memtable: %w", err))
				} else {
					fq.cb(fileId, nil)
				}
			}
		}
	}
}

func (fq *flushQueue) PushBack(memtable *types.Memtable) {
	fq.mu.Lock()
	fq.queue = append(fq.queue, memtable)
	fq.mu.Unlock()

	// Signal worker that there's work to do
	select {
	case fq.workChan <- struct{}{}:
	default:
		// Channel full, but that's ok - worker will keep processing
	}
}

func (fq *flushQueue) Close() {
	close(fq.quit)
	<-fq.done
}

// GetMemtables returns a snapshot of all memtables in the flush queue (newest to oldest)
func (fq *flushQueue) GetMemtables() []*types.Memtable {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	// Return reversed copy (newest to oldest for LSM Get() search order)
	memtables := make([]*types.Memtable, len(fq.queue))
	for i := range fq.queue {
		memtables[i] = fq.queue[len(fq.queue)-1-i]
	}
	return memtables
}
