package sstable

import (
	"encoding/binary"
	"io"

	"github.com/EricHayter/yakv/internal/bitpack"
	"github.com/EricHayter/yakv/internal/bloom_filter"
	"github.com/EricHayter/yakv/server/lsm/types"
	"github.com/EricHayter/yakv/server/storage_manager"
)

// findBlockForKey searches through range blocks to find which data block might contain the key
func (sstable *SSTable) findBlockForKey(key string) (blockNum int, found bool, err error) {
	// Calculate total number of data blocks
	// Data blocks are pages 1 through (bloomPageId - 1)
	numDataBlocks := int(sstable.bloomPageId - 1)

	pageId := sstable.rangePageId
	blockNum = 0

	// Iterate through range pages until we've checked all data blocks
	for blockNum < numDataBlocks {
		page, err := sstable.storageManager.GetPage(sstable.fileId, pageId)
		if err != nil {
			return 0, false, err
		}

		r := page.NewReader()
		var numRanges uint8
		err = binary.Read(r, binary.LittleEndian, &numRanges)
		if err != nil {
			page.Close()
			return 0, false, err
		}

		// Read each range on this page
		for i := 0; i < int(numRanges) && blockNum < numDataBlocks; i++ {
			firstKey, lastKey, err := deserializeRange(r)
			if err != nil {
				page.Close()
				return 0, false, err
			}

			// Check if key is in this range [firstKey, lastKey]
			if key >= firstKey && key <= lastKey {
				page.Close()
				return blockNum, true, nil
			}

			blockNum++
		}

		page.Close()

		// Move to next range page
		pageId++
	}

	// Key not found in any range
	return 0, false, nil
}

// checkBloomFilter checks if a key might be present in the specified data block
func (sstable *SSTable) checkBloomFilter(blockNum int, key string) (bool, error) {
	// Bloom filters are stored sequentially, one per data block
	// Each bloom filter is numBits bits = numBits/8 bytes
	filterSizeBytes := int(sstable.numBits / 8)
	pageSize := 4096

	// Calculate which page and offset within the page
	totalOffset := blockNum * filterSizeBytes
	pageOffset := totalOffset / pageSize
	byteOffsetInPage := totalOffset % pageSize

	pageId := storage_manager.PageId(int(sstable.bloomPageId) + pageOffset)

	page, err := sstable.storageManager.GetPage(sstable.fileId, pageId)
	if err != nil {
		return false, err
	}
	defer page.Close()

	r := page.NewReader()

	// Skip to the right offset within this page
	if byteOffsetInPage > 0 {
		skipBuf := make([]byte, byteOffsetInPage)
		_, err = io.ReadFull(r, skipBuf)
		if err != nil {
			return false, err
		}
	}

	// Read the bloom filter (might span page boundary)
	filterBytes := make([]byte, filterSizeBytes)
	bytesRead := 0

	// Read from current page
	n, err := io.ReadFull(r, filterBytes[bytesRead:])
	bytesRead += n

	// If we need more bytes and hit EOF/page boundary, read from next page
	if bytesRead < filterSizeBytes {
		page.Close()

		pageId++
		page, err = sstable.storageManager.GetPage(sstable.fileId, pageId)
		if err != nil {
			return false, err
		}
		defer page.Close()

		r = page.NewReader()
		_, err = io.ReadFull(r, filterBytes[bytesRead:])
		if err != nil {
			return false, err
		}
	}

	// Unpack and check
	bits := bitpack.Unpack(filterBytes, int(sstable.numBits))
	bf, err := bloom_filter.New(sstable.numBits, sstable.numFilters)
	if err != nil {
		return false, err
	}
	bf.Bits = bits

	return bf.Present([]byte(key)), nil
}

// searchDataBlock performs a linear search through a data block for the key
func (sstable *SSTable) searchDataBlock(blockNum int, key string) (*types.LsmEntry, error) {
	// Data blocks start at page 1 (page 0 is header)
	// Each data block is one page
	dataPageId := storage_manager.PageId(1 + blockNum)

	page, err := sstable.storageManager.GetPage(sstable.fileId, dataPageId)
	if err != nil {
		return nil, err
	}
	defer page.Close()

	r := page.NewReader()

	// Read number of tuples
	var numTuples uint8
	err = binary.Read(r, binary.LittleEndian, &numTuples)
	if err != nil {
		return nil, err
	}

	// Search through the tuples
	for i := 0; i < int(numTuples); i++ {
		entryKey, entry, err := DeserializeKeyValue(r)
		if err != nil {
			return nil, err
		}

		if entryKey == key {
			return &entry, nil
		}

		// Since data is sorted, if we've passed the key, it's not here
		if entryKey > key {
			return nil, nil
		}
	}

	return nil, nil
}

// deserializeHeader reads the SSTable header from a reader
func deserializeHeader(r io.Reader) (*sstableHeader, error) {
	h := &sstableHeader{}

	// Read numTuples (4 bytes)
	err := binary.Read(r, binary.LittleEndian, &h.numTuples)
	if err != nil {
		return nil, err
	}

	// Read numFilters (2 bytes)
	err = binary.Read(r, binary.LittleEndian, &h.numFilters)
	if err != nil {
		return nil, err
	}

	// Read numBits (2 bytes)
	err = binary.Read(r, binary.LittleEndian, &h.numBits)
	if err != nil {
		return nil, err
	}

	// Read bloomPageId (4 bytes)
	err = binary.Read(r, binary.LittleEndian, &h.bloomPageId)
	if err != nil {
		return nil, err
	}

	// Read rangePageId (4 bytes)
	err = binary.Read(r, binary.LittleEndian, &h.rangePageId)
	if err != nil {
		return nil, err
	}

	// Read globalFirstKeyLen (2 bytes)
	var globalFirstKeyLen uint16
	err = binary.Read(r, binary.LittleEndian, &globalFirstKeyLen)
	if err != nil {
		return nil, err
	}

	// Read globalLastKeyLen (2 bytes)
	var globalLastKeyLen uint16
	err = binary.Read(r, binary.LittleEndian, &globalLastKeyLen)
	if err != nil {
		return nil, err
	}

	// Read globalFirstKey (variable bytes)
	globalFirstKeyBytes := make([]byte, globalFirstKeyLen)
	_, err = io.ReadFull(r, globalFirstKeyBytes)
	if err != nil {
		return nil, err
	}
	h.globalFirstKey = string(globalFirstKeyBytes)

	// Read globalLastKey (variable bytes)
	globalLastKeyBytes := make([]byte, globalLastKeyLen)
	_, err = io.ReadFull(r, globalLastKeyBytes)
	if err != nil {
		return nil, err
	}
	h.globalLastKey = string(globalLastKeyBytes)

	return h, nil
}

// deserializeRange reads a range (firstKey, lastKey) from a reader
// Format: firstKeyLen, lastKeyLen, firstKey, lastKey
func deserializeRange(r io.Reader) (firstKey, lastKey string, err error) {
	var firstKeyLen uint16
	var lastKeyLen uint16

	// Read firstKeyLen (2 bytes)
	err = binary.Read(r, binary.LittleEndian, &firstKeyLen)
	if err != nil {
		return
	}

	// Read lastKeyLen (2 bytes)
	err = binary.Read(r, binary.LittleEndian, &lastKeyLen)
	if err != nil {
		return
	}

	// Read firstKey (variable bytes)
	firstKeyBytes := make([]byte, firstKeyLen)
	_, err = io.ReadFull(r, firstKeyBytes)
	if err != nil {
		return
	}
	firstKey = string(firstKeyBytes)

	// Read lastKey (variable bytes)
	lastKeyBytes := make([]byte, lastKeyLen)
	_, err = io.ReadFull(r, lastKeyBytes)
	if err != nil {
		return
	}
	lastKey = string(lastKeyBytes)

	return
}

// DeserializeKeyValue reads a key-value pair from r.
// Format: timestamp, deleted, keyLen, valueLen, key, value
// Returns the key and types.LsmEntry, or an error if the data is malformed.
func DeserializeKeyValue(r io.Reader) (string, types.LsmEntry, error) {
	var entry types.LsmEntry

	// Read timestamp (8 bytes)
	if err := binary.Read(r, binary.LittleEndian, &entry.Timestamp); err != nil {
		return "", types.LsmEntry{}, err
	}

	// Read deleted flag (1 byte)
	var deletedByte byte
	if err := binary.Read(r, binary.LittleEndian, &deletedByte); err != nil {
		return "", types.LsmEntry{}, err
	}
	entry.Deleted = deletedByte != 0

	// Read key length (4 bytes)
	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return "", types.LsmEntry{}, err
	}

	// Read value length (4 bytes)
	var valueLen uint32
	if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
		return "", types.LsmEntry{}, err
	}

	// Read key bytes
	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyBytes); err != nil {
		return "", types.LsmEntry{}, err
	}
	key := string(keyBytes)

	// Read value bytes
	valueBytes := make([]byte, valueLen)
	if _, err := io.ReadFull(r, valueBytes); err != nil {
		return "", types.LsmEntry{}, err
	}
	entry.Value = string(valueBytes)

	return key, entry, nil
}
