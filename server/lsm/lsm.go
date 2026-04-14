package lsm

import (
	"sync"
	"github.com/EricHayter/yakv/server/lsm/sstable"
	"github.com/EricHayter/yakv/server/lsm/types"
	"github.com/EricHayter/yakv/server/storage_manager"
)

const (
	memtableSizeThreshold = 1 << 26
)

type LogStructuredMergeTree struct {
	mu sync.RWMutex // Protects memtable, sstables, memtableSize, and lastTimestamp
	memtableSize uint64
	memtable     *types.Memtable
	sstables     [][]storage_manager.FileId
	storageManager *storage_manager.StorageManager
	lastTimestamp uint64
	flushQueue flushQueue
}

func (lsm *LogStructuredMergeTree) onMemtableFlush(fileId storage_manager.FileId, err error) {
	if err != nil {
		// TODO: log error, possibly retry
		return
	}

	// Need lock to safely modify sstables slice
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	lsm.sstables[0] = append(lsm.sstables[0], fileId)
}

func New(storageManager *storage_manager.StorageManager) *LogStructuredMergeTree {
	lsm := &LogStructuredMergeTree{
		memtable: types.NewMemtable(),
		sstables: make([][]storage_manager.FileId, 1),
		storageManager: storageManager,
		lastTimestamp: 0,
	}

	lsm.flushQueue = *newFlushQueue(lsm.storageManager, lsm.onMemtableFlush)
	return lsm
}

func (lsm *LogStructuredMergeTree) Put(key, value string) {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	lsm.lastTimestamp++
	newEntry := types.LsmEntry{
		Timestamp: lsm.lastTimestamp,
		Deleted: false,
		Value: value,
	}
	lsm.memtable.Insert(key, newEntry)
	lsm.memtableSize += uint64(len(key) + len(value) + 8 + 1)

	// If memtable is full, flush it
	if lsm.memtableSize >= memtableSizeThreshold {
		lsm.flushQueue.PushBack(lsm.memtable)
		lsm.memtable = types.NewMemtable()
		lsm.memtableSize = 0
	}
}

func (lsm *LogStructuredMergeTree) Delete(key string) {
	/* deleting from the skiplist is not enough to delete from the LSM since
	 * if we only remove it from the memtable (skiplist) the LSM will search
	 * for other logs that contain this key which will show previous data
	 *
	 * because of this, we need to explcitly create a new log that states the
	 * key has been deleted/removed.
	 */
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	lsm.lastTimestamp++
	newEntry := types.LsmEntry{
		Timestamp: lsm.lastTimestamp,
		Deleted: true,
		Value: "",
	}
	lsm.memtable.Insert(key, newEntry)
	// Tombstones also count toward memtable size
	lsm.memtableSize += uint64(len(key) + 8 + 1)

	// Check if we need to flush
	if lsm.memtableSize >= memtableSizeThreshold {
		lsm.flushQueue.PushBack(lsm.memtable)
		lsm.memtable = types.NewMemtable()
		lsm.memtableSize = 0
	}
}

func (lsm *LogStructuredMergeTree) Get(key string) (string, bool) {
	// Simplest approach: hold read lock for entire operation
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	// Check current memtable first
	entry, found := lsm.memtable.Get(key)
	if found {
		if entry.Deleted {
			return "", false
		}
		return entry.Value, true
	}

	// Search flush queue (newest to oldest)
	// Memtables in flush queue are read-only, safe to access
	for node := lsm.flushQueue.tail; node != nil; node = node.next {
		entry, found := node.memtable.Get(key)
		if found {
			if entry.Deleted {
				return "", false
			}
			return entry.Value, true
		}
	}

	for level := 0; level < len(lsm.sstables); level++ {
	    for i := len(lsm.sstables[level]) - 1; i >= 0; i-- {
	        fileId := lsm.sstables[level][i]
	        sstable, err := sstable.Open(lsm.storageManager, fileId)
			if err != nil {
				// TODO print something here soon.
			}
	        entry, err := sstable.Get(key)
	        if entry != nil {
	            if entry.Deleted {
	                return "", false
	            }
	            return entry.Value, true
	        }
	    }
	}

	return "", false
}
