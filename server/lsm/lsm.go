package lsm

import (
	"github.com/EricHayter/yakv/internal/skiplist"
	"github.com/EricHayter/yakv/server/storage_manager"
)

// LsmEntry represents a key-value entry with metadata
type LsmEntry struct {
	Timestamp uint64
	Deleted   bool
	Value     string
}

type LogStructuredMergeTree struct {
	memtable skiplist.SkipList[string, LsmEntry]
	sstables [][]storage_manager.FileId
	storageManager *storage_manager.StorageManager
	lastTimestamp uint64
}

func (lsm *LogStructuredMergeTree) Put(key, value string) {
	lsm.lastTimestamp++
	newEntry := LsmEntry{
		Timestamp: lsm.lastTimestamp,
		Deleted: false,
		Value: value,
	}
	lsm.memtable.Insert(key, newEntry)
}

func (lsm *LogStructuredMergeTree) Delete(key string) {
	/* deleting from the skiplist is not enough to delete from the LSM since
	 * if we only remove it from the memtable (skiplist) the LSM will search
	 * for other logs that contain this key which will show previous data
	 *
	 * because of this, we need to explcitly create a new log that states the
	 * key has been deleted/removed.
	 */
	lsm.lastTimestamp++
	newEntry := LsmEntry{
		Timestamp: lsm.lastTimestamp,
		Deleted: true,
		Value: "",
	}
	lsm.memtable.Insert(key, newEntry)
}

func (lsm *LogStructuredMergeTree) Get(key string) (string, bool) {
	entry, pres := lsm.memtable.Get(key)
	if !pres {
		return "", false
	}
	return entry.Value, true
}
