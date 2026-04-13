package types

import "github.com/EricHayter/yakv/internal/skiplist"

// LsmEntry represents a key-value entry with metadata
type LsmEntry struct {
	Timestamp uint64
	Deleted   bool
	Value     string
}

// Memtable is a type alias for the skiplist-based in-memory table
type Memtable = skiplist.SkipList[string, LsmEntry]
