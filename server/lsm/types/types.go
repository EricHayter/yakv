package types

import "github.com/EricHayter/yakv/internal/skiplist"

// The memtable skiplist bump-allocates from fixed-size pools. We size those
// pools to a target arena footprint of ~64 MiB (preserving the previous flush
// cadence). approxBytesPerEntry is the rough per-entry cost across the node,
// link, and value pools for [string, LsmEntry].
const (
	memtableBudgetBytes  = 1 << 26 // 64 MiB target footprint
	approxBytesPerEntry  = 96
	memtableNodeCapacity = memtableBudgetBytes / approxBytesPerEntry
)

// LsmEntry represents a key-value entry with metadata
type LsmEntry struct {
	Timestamp uint64
	Deleted   bool
	Value     string
}

// Memtable is a type alias for the skiplist-based in-memory table
type Memtable = skiplist.SkipList[string, LsmEntry]

// NewMemtable creates a new empty memtable backed by fixed-size, lock-free
// allocation pools. When the pools fill, Insert reports full and the LSM seals
// the memtable and starts a fresh one.
func NewMemtable() *Memtable {
	return skiplist.NewSkipListWithCapacity[string, LsmEntry](memtableNodeCapacity)
}
