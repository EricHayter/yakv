package sstable

import (
	"fmt"
	"github.com/EricHayter/yakv/server/lsm/types"
	"github.com/EricHayter/yakv/server/storage_manager"
)

/* This file contains the main SSTable struct and high-level operations.
 * Reading implementation is in sstable_reader.go
 * Writing implementation is in sstable_writer.go
 */

type SSTable struct {
	storageManager *storage_manager.StorageManager
	fileId         storage_manager.FileId

	// Total number of entries in the SSTable
	numTuples uint32

	// Bloom filter configuration
	numBits    uint
	numFilters uint

	// Track page IDs (header=0, data starts at 1)
	bloomPageId storage_manager.PageId
	rangePageId storage_manager.PageId

	// Global range for the entire SSTable
	globalFirstKey string
	globalLastKey  string
}

// CreateNew creates a level 0 sstable (effectively just a direct dump of a memtable)
func CreateNew(storageManager *storage_manager.StorageManager, memtable *types.Memtable) (storage_manager.FileId, error) {
	writer, err := NewTableWriter(storageManager)
	if err != nil {
		return 0, fmt.Errorf("failed to create sstable writer: %w", err)
	}
	fileId, err := writer.Write(memtable)
	if err != nil {
		return 0, fmt.Errorf("failed to write memtable to sstable: %w", err)
	}
	return fileId, nil
}

// Open opens an existing SSTable by reading its header
func Open(sm *storage_manager.StorageManager, fileId storage_manager.FileId) (*SSTable, error) {
	// Header is always at page 0
	pageId := storage_manager.PageId(0)
	page, err := sm.GetPage(fileId, pageId)
	if err != nil {
		return nil, fmt.Errorf("failed to get header page for sstable %d: %w", fileId, err)
	}

	r := page.NewReader()
	header, err := deserializeHeader(r)
	if err != nil {
		page.Close()
		return nil, fmt.Errorf("failed to deserialize sstable header: %w", err)
	}
	page.Close()

	sstable := &SSTable{
		storageManager: sm,
		fileId:         fileId,
		numTuples:      header.numTuples,
		numBits:        uint(header.numBits),
		numFilters:     uint(header.numFilters),
		bloomPageId:    header.bloomPageId,
		rangePageId:    header.rangePageId,
		globalFirstKey: header.globalFirstKey,
		globalLastKey:  header.globalLastKey,
	}

	return sstable, nil
}

// Get retrieves a value from the SSTable
func (sstable *SSTable) Get(key string) (*types.LsmEntry, error) {
	// Step 1: Find which data block might contain the key by checking ranges
	dataBlockNum, found, err := sstable.findBlockForKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to find block for key %q: %w", key, err)
	}
	if !found {
		// Key is outside all ranges
		return nil, nil
	}

	// Step 2: Check the bloom filter for that block
	mightBePresent, err := sstable.checkBloomFilter(dataBlockNum, key)
	if err != nil {
		return nil, fmt.Errorf("failed to check bloom filter for key %q in block %d: %w", key, dataBlockNum, err)
	}
	if !mightBePresent {
		// Definitely not present
		return nil, nil
	}

	// Step 3: Actually search the data block
	entry, err := sstable.searchDataBlock(dataBlockNum, key)
	if err != nil {
		return nil, fmt.Errorf("failed to search data block %d for key %q: %w", dataBlockNum, key, err)
	}
	return entry, nil
}
