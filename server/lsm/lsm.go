package lsm

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/EricHayter/yakv/server/lsm/sstable"
	"github.com/EricHayter/yakv/server/lsm/types"
	"github.com/EricHayter/yakv/server/storage_manager"
	"github.com/EricHayter/yakv/server/wal"
)

const (
	memtableSizeThreshold = 1 << 26
)

type LogStructuredMergeTree struct {
	mu sync.RWMutex // RLock for reads/writes, Lock for memtable flush and sstable modifications

	// Accessed atomically - must be 64-bit aligned (keep at top of struct)
	memtableSize uint64

	memtable   *types.Memtable
	flushQueue flushQueue
	sstables   [][]storage_manager.FileId

	storageManager *storage_manager.StorageManager
	manifest       *manifest
	wal            *wal.WriteAheadLog
	flushSignaler  chan<- struct{}
}

func (lsm *LogStructuredMergeTree) onMemtableFlush(fileId storage_manager.FileId, err error) {
	if err != nil {
		// TODO: log error, possibly retry
		return
	}

	// Need lock to safely modify sstables slice
	lsm.mu.Lock()
	lsm.sstables[0] = append(lsm.sstables[0], fileId)
	lsm.mu.Unlock()

	// Checkpoint WAL to mark this as a safe recovery point
	if err := lsm.wal.Checkpoint(); err != nil {
		// Log error - checkpoint failure is serious but not fatal
		slog.Error("failed to checkpoint WAL", "error", err)
	}

	// Signal manifest to flush (non-blocking)
	select {
	case lsm.flushSignaler <- struct{}{}:
	default:
		// Channel already has a pending signal, skip
	}
}

func New(storageManager *storage_manager.StorageManager) (*LogStructuredMergeTree, error) {
	// Try to load existing version from disk
	v, err := loadVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to load LSM manifest: %w", err)
	}

	// Initialize LSM with loaded data or defaults
	var sstables [][]storage_manager.FileId

	if v != nil {
		// Restore from persisted state
		sstables = v.sstables
	} else {
		// Fresh start
		sstables = make([][]storage_manager.FileId, 1)
	}

	// Create separate flush signaler channels
	manifestFlushSignaler := make(chan struct{}, 1)

	// Initialize WAL and recover logs
	writeAheadLog, recoveredLogs, err := wal.NewWriteAheadLog()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	lsm := &LogStructuredMergeTree{
		memtable:       types.NewMemtable(),
		sstables:       sstables,
		storageManager: storageManager,
		wal:            writeAheadLog,
		flushSignaler:  manifestFlushSignaler,
	}

	// Rebuild memtable from recovered WAL logs
	if err := lsm.initMemtable(recoveredLogs); err != nil {
		return nil, fmt.Errorf("failed to rebuild memtable from WAL: %w", err)
	}

	lsm.flushQueue = *newFlushQueue(lsm.storageManager, lsm.onMemtableFlush)
	lsm.manifest = newManifest(lsm, manifestFlushSignaler)

	return lsm, nil
}

func (lsm *LogStructuredMergeTree) initMemtable(logs []wal.Log) error {
	for _, log := range logs {
		switch l := log.(type) {
		case *wal.WriteLog:
			entry := types.LsmEntry{
				Timestamp: l.Timestamp(),
				Deleted:   false,
				Value:     l.Value(),
			}
			lsm.memtable.Insert(l.Key(), entry)
		case *wal.DeleteLog:
			entry := types.LsmEntry{
				Timestamp: l.Timestamp(),
				Deleted:   true,
				Value:     "",
			}
			lsm.memtable.Insert(l.Key(), entry)
		}
	}
	return nil
}

func (lsm *LogStructuredMergeTree) Put(key, value string) {
	// Log to WAL first and get LSN (timestamp will be set by Push)
	lsn := lsm.wal.Push(wal.NewWriteLog(key, value, 0))

	// Hot path: use read lock since memtable is thread-safe
	lsm.mu.RLock()

	newEntry := types.LsmEntry{
		Timestamp: lsn,
		Deleted:   false,
		Value:     value,
	}
	lsm.memtable.Insert(key, newEntry)
	newSize := atomic.AddUint64(&lsm.memtableSize, uint64(len(key)+len(value)+8+1))

	// Check if we need to flush
	if newSize >= memtableSizeThreshold {
		lsm.mu.RUnlock()
		// Acquire write lock for flush
		lsm.mu.Lock()
		// Double-check - another thread might have flushed already
		if atomic.LoadUint64(&lsm.memtableSize) >= memtableSizeThreshold {
			lsm.flushQueue.PushBack(lsm.memtable)
			lsm.memtable = types.NewMemtable()
			atomic.StoreUint64(&lsm.memtableSize, 0)
		}
		lsm.mu.Unlock()
	} else {
		lsm.mu.RUnlock()
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
	// Log to WAL first and get LSN (timestamp will be set by Push)
	lsn := lsm.wal.Push(wal.NewDeleteLog(key, 0))

	// Hot path: use read lock since memtable is thread-safe
	lsm.mu.RLock()

	newEntry := types.LsmEntry{
		Timestamp: lsn,
		Deleted:   true,
		Value:     "",
	}
	lsm.memtable.Insert(key, newEntry)
	// Tombstones also count toward memtable size
	newSize := atomic.AddUint64(&lsm.memtableSize, uint64(len(key)+8+1))

	// Check if we need to flush
	if newSize >= memtableSizeThreshold {
		lsm.mu.RUnlock()
		// Acquire write lock for flush
		lsm.mu.Lock()
		// Double-check - another thread might have flushed already
		if atomic.LoadUint64(&lsm.memtableSize) >= memtableSizeThreshold {
			lsm.flushQueue.PushBack(lsm.memtable)
			lsm.memtable = types.NewMemtable()
			atomic.StoreUint64(&lsm.memtableSize, 0)
		}
		lsm.mu.Unlock()
	} else {
		lsm.mu.RUnlock()
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
	for _, memtable := range lsm.flushQueue.GetMemtables() {
		entry, found := memtable.Get(key)
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

// getVersion creates a snapshot of the current LSM state for persistence.
// This method is called by the manifest to get the current state to flush.
func (lsm *LogStructuredMergeTree) getVersion() version {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	// Deep copy sstables to avoid data races
	sstablesCopy := make([][]storage_manager.FileId, len(lsm.sstables))
	for i := range lsm.sstables {
		sstablesCopy[i] = append([]storage_manager.FileId{}, lsm.sstables[i]...)
	}

	return version{
		sstables: sstablesCopy,
	}
}

// Close stops background goroutines and closes the storage manager
func (lsm *LogStructuredMergeTree) Close() error {
	// Stop flush queue worker (waits for pending flushes to complete)
	lsm.flushQueue.Close()

	// Stop WAL flusher (waits for pending flushes to complete)
	if lsm.wal != nil {
		lsm.wal.Close()
	}

	// Stop manifest flusher
	if lsm.manifest != nil {
		lsm.manifest.Close()
	}

	// Close storage manager
	if lsm.storageManager != nil {
		return lsm.storageManager.Close()
	}
	return nil
}
