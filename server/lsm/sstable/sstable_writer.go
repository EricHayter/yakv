package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"iter"

	"github.com/EricHayter/yakv/internal/bitpack"
	"github.com/EricHayter/yakv/internal/bloom_filter"
	"github.com/EricHayter/yakv/server/lsm/types"
	"github.com/EricHayter/yakv/server/storage_manager"
)

/*
SSTable file format

The SSTable will be composed of blocks (pages) of size 4kb using pages from
the buffer manager.

The SSTable contains 4 different types of blocks:
1. a header block: a metadata tracking block, in particular this contains
   the number of entries inside the entire SSTable, bloom filter configuration
   parameters (i.e. number of bits, number of hash functions), and lastly
   page number offsets into the first filter and range blocks respectively as
   they are not at a fixed block #. And a global range
2. data blocks: data blocks contain the main data of the SSTable (as the name
   suggests). In particular, we contain key, value pairs from the memtable,
   AND metadata including the timestamp of the entry and a tombstone to
   indicate if the entry was deleted. serialized strings i.e. keys, and values
   are serialized with length-value pairs.
3. filter blocks: filter blocks store bloom filter data per data block. In
   particular, pages have bloom filters written out in the order of datablocks
   and allow for relatively fast checks for the existence of values inside of
   datablocks. All bloom filters in filter blocks follow the configuration as
   specified in the header block, this reduces overhead of storing information
   per block and allows for maximum utilization of the 4kb as the bloom
   filters are a power of 2 as well leading to 0 fragmentation.
4. range blocks: range blocks, are used to indicate what range of key values
   are in each data block. Using this + the floom filters we can get a fairly
   probable estimation whether or not a key is the a given block without
   getting false positives.
*/

/*
Some math here to figure out rough parameters for this
*
* pages have 4096 bytes, and key value pairs have 17 + m + n bytes in data
* with n and m being the lengths of the key and value respectively.
*
* assuming 8 bytes for key and value, say the user is using ids that are
* 64 bit ints and keys are also some 8 byte number, the we can assume
* entries are 33 bytes long.
*
* 4096 / 33 is roughly 124 entries per block/page
*
* We are generating bloom filters per page.
*
* from here I'm going to use the facts listed in this wikipedia article
* https://en.wikipedia.org/wiki/Bloom_filter
* to pick out some parameters that are close to optimal for this
* hypothetical (but hopefully close to reality) setup.

* We figure out roughly the size of bloom filters we want:
* to determine the number of bits we need we can use the following formula
* give a desired false positive rate:
*
* 		m = -2.08ln(E)n
*
* where m is the number of bits, E is the desired false positive rate,
* and n is the number of elements in the filter.
*
* so with a our 124 entries we have the following bit counts per false
* positive rate:
*
* 1%: around 1187 bits (around 148 bytes)
* 5%: around 772 bits (around 97 bytes)
*
* Or more practically since I have the constraint that the size of our
* bloom filter must be a power of 2-bytes long, we can determine the
* expected false positive rate with m bits, asuuming ideal hashing.
*
* 256 bytes: 0.35% false positive
* 128 bytes: 1.9% false positive
*
* Given these numbers it would seem that going with a bloom filter of size
* 256 bytes seems best giving a ratio of 16:1 of data blocks to filter
* blocks which I belive is reasonable, and a relatively low false potiive
* rate.
*
* also, it's almost certain that a false positive rate of 0.35% will not be
* achieved in real scenarios, so being slightly conservative with
* acceptable false positive rates here seems like a good choice.
*
* optinal number of hashes is a lot more simple :) (thank goodness).
* optimal number of hashes is achieved by m/n*ln(2) where m is the number
* of bits, and n is the number of entries.
*
* Thus, assuming the number of entries per page is matching closely to
* our prediction, of 124 entries, the optimal number of hash functions is
* 11.
*/
const (
	numFilters = 11
	numBits    = 2048
)

type sstableHeader struct {
	numTuples      uint32
	numFilters     uint16
	numBits        uint16
	bloomPageId    storage_manager.PageId
	rangePageId    storage_manager.PageId
	globalFirstKey string
	globalLastKey  string
}

func (h *sstableHeader) serialize(w io.Writer) error {
	// Write numTuples (4 bytes)
	err := binary.Write(w, binary.LittleEndian, h.numTuples)
	if err != nil {
		return err
	}

	// Write numFilters (2 bytes)
	err = binary.Write(w, binary.LittleEndian, h.numFilters)
	if err != nil {
		return err
	}

	// Write numBits (2 bytes)
	err = binary.Write(w, binary.LittleEndian, h.numBits)
	if err != nil {
		return err
	}

	// Write bloomPageId (4 bytes)
	err = binary.Write(w, binary.LittleEndian, h.bloomPageId)
	if err != nil {
		return err
	}

	// Write rangePageId (4 bytes)
	err = binary.Write(w, binary.LittleEndian, h.rangePageId)
	if err != nil {
		return err
	}

	// Write globalFirstKeyLen (2 bytes)
	globalFirstKeyLen := uint16(len(h.globalFirstKey))
	err = binary.Write(w, binary.LittleEndian, globalFirstKeyLen)
	if err != nil {
		return err
	}

	// Write globalLastKeyLen (2 bytes)
	globalLastKeyLen := uint16(len(h.globalLastKey))
	err = binary.Write(w, binary.LittleEndian, globalLastKeyLen)
	if err != nil {
		return err
	}

	// Write globalFirstKey (variable bytes)
	_, err = w.Write([]byte(h.globalFirstKey))
	if err != nil {
		return err
	}

	// Write globalLastKey (variable bytes)
	_, err = w.Write([]byte(h.globalLastKey))
	if err != nil {
		return err
	}

	return nil
}

type lsmTableWriter struct {
	storageManager *storage_manager.StorageManager
	fileId         storage_manager.FileId

	// Bloom filter configuration
	numBits    uint
	numFilters uint

	// Track page IDs as we write (header=0, data=1, then these)
	bloomPageId storage_manager.PageId
	rangePageId storage_manager.PageId

	// Global metadata
	numTuples      uint32
	globalFirstKey string
	globalLastKey  string
}

func NewTableWriter(sm *storage_manager.StorageManager) (*lsmTableWriter, error) {
	fileId, err := sm.CreateFile()
	if err != nil {
		return nil, err
	}

	return &lsmTableWriter{
		storageManager: sm,
		fileId:         fileId,
		numBits:        numBits,
		numFilters:     numFilters,
	}, nil
}

func (w *lsmTableWriter) Write(memtable *types.Memtable) (storage_manager.FileId, error) {
	// Write data blocks (starts at page 1)
	firstKeys, err := w.writeDataBlocks(memtable.Items())
	if err != nil {
		return 0, fmt.Errorf("failed to write data blocks: %w", err)
	}

	// Write bloom filters
	w.bloomPageId, err = w.writeBloomFilters(firstKeys, memtable.Items())
	if err != nil {
		return 0, fmt.Errorf("failed to write bloom filters: %w", err)
	}

	// Write ranges
	w.rangePageId, err = w.writeRangeBlocks(firstKeys, memtable.Items())
	if err != nil {
		return 0, fmt.Errorf("failed to write range blocks: %w", err)
	}

	// Write header (at page 0)
	err = w.writeHeaderBlock()
	if err != nil {
		return 0, fmt.Errorf("failed to write header block: %w", err)
	}

	return w.fileId, nil
}

func (w *lsmTableWriter) writeDataBlocks(it iter.Seq2[string, types.LsmEntry]) (firstKeys []string, err error) {
	// Initialize empty slice - we'll append first key of each block as we create them
	firstKeys = make([]string, 0)
	tuplesWritten := uint8(0)
	w.numTuples = 0

	// Track whether we're starting a new block/page
	isFirstKeyInBlock := true
	isFirstKeyOverall := true

	// Create the initial page
	pageId, err := w.storageManager.AddPage(w.fileId)
	if err != nil {
		return nil, err
	}
	page, err := w.storageManager.GetPage(w.fileId, pageId)
	if err != nil {
		return nil, err
	}
	pageWriter := page.NewWriter()

	// Reserve byte 0 for tuple count (will be filled in later)
	pageWriter.Write([]byte{0})

	// Single iteration through all entries
	for key, entry := range it {
		// Try to serialize the entry to the current page
		err = SerializeKeyValue(pageWriter, key, entry)

		if err != nil {
			// Page is full - finalize current page and create a new one

			// 1. Write the tuple count to byte 0 of the current page
			pageWriter.WriteAt([]byte{tuplesWritten}, 0)
			page.MarkDirty()
			page.Close()

			// 2. Create a new page
			pageId, err = w.storageManager.AddPage(w.fileId)
			if err != nil {
				return nil, err
			}
			page, err = w.storageManager.GetPage(w.fileId, pageId)
			if err != nil {
				return nil, err
			}
			pageWriter = page.NewWriter()

			// 3. Reserve byte 0 for tuple count in the new page
			pageWriter.Write([]byte{0})

			// 4. Reset counters and flags for the new block
			tuplesWritten = 0
			isFirstKeyInBlock = true

			// 5. Retry serializing the current entry on the new page
			err = SerializeKeyValue(pageWriter, key, entry)
			if err != nil {
				// If we can't serialize to a fresh page, something is seriously wrong
				return nil, err
			}
		}

		// If this is the first key in the current block, record it
		if isFirstKeyInBlock {
			firstKeys = append(firstKeys, key)
			isFirstKeyInBlock = false
		}

		// Track global first and last keys
		if isFirstKeyOverall {
			w.globalFirstKey = key
			isFirstKeyOverall = false
		}
		w.globalLastKey = key

		tuplesWritten++
		w.numTuples++
	}

	// Finalize the last page
	pageWriter.WriteAt([]byte{tuplesWritten}, 0)
	page.MarkDirty()
	page.Close()

	return firstKeys, nil
}

func (w *lsmTableWriter) writeBloomFilters(firstKeys []string, it iter.Seq2[string, types.LsmEntry]) (storage_manager.PageId, error) {
	pageId, err := w.storageManager.AddPage(w.fileId)
	if err != nil {
		return 0, err
	}
	page, err := w.storageManager.GetPage(w.fileId, pageId)
	if err != nil {
		return 0, err
	}
	firstPageId := storage_manager.PageId(pageId)
	pageWriter := page.NewWriter()

	bf, err := bloom_filter.New(w.numBits, w.numFilters)
	if err != nil {
		return 0, err
	}

	numDataBlocks := len(firstKeys)
	dataBlockNum := 0
	for key := range it {
		// This is the last block so everything here can be added to the
		// bloom filter without checking that we have entered into the next
		// block's data.
		if dataBlockNum == numDataBlocks-1 {
			bf.Insert([]byte(key))
			// This is still data in the current block
		} else if key < firstKeys[dataBlockNum+1] {
			bf.Insert([]byte(key))
			// we've reached the next block's data, so we must flush the data
			// to disk
		} else {
			// attempt to write the bloom filters data to the thing
			_, err := pageWriter.Write(bitpack.Pack(bf.Bits))
			if err != nil {
				// if we failed to write (page full) create a new page and page
				// writer
				page.MarkDirty()
				page.Close()

				// Create new page
				pageId, err = w.storageManager.AddPage(w.fileId)
				if err != nil {
					return 0, err
				}
				page, err = w.storageManager.GetPage(w.fileId, pageId)
				if err != nil {
					return 0, err
				}
				pageWriter = page.NewWriter()
				_, err = pageWriter.Write(bitpack.Pack(bf.Bits))
				if err != nil {
					return 0, err
				}
			}

			// Reset bloom filter for next block
			bf, err = bloom_filter.New(w.numBits, w.numFilters)
			if err != nil {
				return 0, err
			}

			// Insert current key into new bloom filter
			bf.Insert([]byte(key))

			// Move to next data block
			dataBlockNum++
		}
	}

	// Write the final bloom filter
	_, err = pageWriter.Write(bitpack.Pack(bf.Bits))
	if err != nil {
		// Page full - create new page and retry
		page.MarkDirty()
		page.Close()

		pageId, err = w.storageManager.AddPage(w.fileId)
		if err != nil {
			return 0, err
		}
		page, err = w.storageManager.GetPage(w.fileId, pageId)
		if err != nil {
			return 0, err
		}
		pageWriter = page.NewWriter()
		_, err = pageWriter.Write(bitpack.Pack(bf.Bits))
		if err != nil {
			return 0, err
		}
	}

	page.MarkDirty()
	page.Close()
	return firstPageId, nil
}

func (w *lsmTableWriter) writeRangeBlocks(firstKeys []string, it iter.Seq2[string, types.LsmEntry]) (storage_manager.PageId, error) {
	tuplesWritten := uint8(0)
	pageId, err := w.storageManager.AddPage(w.fileId)
	if err != nil {
		return 0, err
	}
	firstPageId := pageId
	page, err := w.storageManager.GetPage(w.fileId, pageId)
	if err != nil {
		return 0, err
	}
	pageWriter := page.NewWriter()

	// Reserve byte 0 for tuple count
	pageWriter.Write([]byte{0})

	var previousKey string
	dataBlockNum := 0
	numDataBlocks := len(firstKeys)
	for key := range it {
		// still in current block's range
		if dataBlockNum == numDataBlocks-1 || key < firstKeys[dataBlockNum+1] {
			previousKey = key
			continue
		}

		// this is the start of the new block
		err = serializeRange(pageWriter, firstKeys[dataBlockNum], previousKey)
		if err != nil {
			// Page full - write count, mark dirty, close, get new page
			pageWriter.WriteAt([]byte{tuplesWritten}, 0)
			page.MarkDirty()
			page.Close()
			tuplesWritten = 0

			// Create new page
			pageId, err = w.storageManager.AddPage(w.fileId)
			if err != nil {
				return 0, err
			}
			page, err = w.storageManager.GetPage(w.fileId, pageId)
			if err != nil {
				return 0, err
			}
			pageWriter = page.NewWriter()

			// Reserve byte 0 for tuple count
			pageWriter.Write([]byte{0})

			// Retry serializing on new page
			err = serializeRange(pageWriter, firstKeys[dataBlockNum], previousKey)
			if err != nil {
				return 0, err
			}
		}
		dataBlockNum++
		previousKey = key
		tuplesWritten++
	}

	// Write the final block's range
	err = serializeRange(pageWriter, firstKeys[dataBlockNum], previousKey)
	if err != nil {
		// Page full - write count, mark dirty, close, get new page
		pageWriter.WriteAt([]byte{tuplesWritten}, 0)
		page.MarkDirty()
		page.Close()
		tuplesWritten = 0

		// Create new page
		pageId, err = w.storageManager.AddPage(w.fileId)
		if err != nil {
			return 0, err
		}
		page, err = w.storageManager.GetPage(w.fileId, pageId)
		if err != nil {
			return 0, err
		}
		pageWriter = page.NewWriter()

		// Reserve byte 0 for tuple count
		pageWriter.Write([]byte{0})

		// Retry serializing on new page
		err = serializeRange(pageWriter, firstKeys[dataBlockNum], previousKey)
		if err != nil {
			return 0, err
		}
	}
	tuplesWritten++

	// Write final count, mark dirty, and close last page
	pageWriter.WriteAt([]byte{tuplesWritten}, 0)
	page.MarkDirty()
	page.Close()
	return firstPageId, nil
}

func (w *lsmTableWriter) writeHeaderBlock() error {
	// Header is always at page 0
	// We need to create it at the beginning, but we write it at the end when we have all the metadata
	pageId := storage_manager.PageId(0)
	page, err := w.storageManager.GetPage(w.fileId, pageId)
	if err != nil {
		// Page doesn't exist yet, create it
		pageId, err = w.storageManager.AddPage(w.fileId)
		if err != nil {
			return err
		}
		page, err = w.storageManager.GetPage(w.fileId, pageId)
		if err != nil {
			return err
		}
	}

	pageWriter := page.NewWriter()

	header := &sstableHeader{
		numTuples:      w.numTuples,
		numFilters:     uint16(w.numFilters),
		numBits:        uint16(w.numBits),
		bloomPageId:    w.bloomPageId,
		rangePageId:    w.rangePageId,
		globalFirstKey: w.globalFirstKey,
		globalLastKey:  w.globalLastKey,
	}

	err = header.serialize(pageWriter)
	if err != nil {
		return err
	}

	page.MarkDirty()
	page.Close()
	return nil
}

func serializeRange(w io.Writer, firstKey, lastKey string) error {
	firstKeyLen := uint16(len(firstKey))
	lastKeyLen := uint16(len(lastKey))

	// Write firstKeyLen (2 bytes)
	err := binary.Write(w, binary.LittleEndian, firstKeyLen)
	if err != nil {
		return err
	}

	// Write lastKeyLen (2 bytes)
	err = binary.Write(w, binary.LittleEndian, lastKeyLen)
	if err != nil {
		return err
	}

	// Write firstKey (variable bytes)
	_, err = w.Write([]byte(firstKey))
	if err != nil {
		return err
	}

	// Write lastKey (variable bytes)
	_, err = w.Write([]byte(lastKey))
	if err != nil {
		return err
	}

	return nil
}

// SerializeKeyValue writes a key-value pair to w in binary format.
// Format: timestamp, deleted, keyLen, valueLen, key, value
func SerializeKeyValue(w io.Writer, key string, entry types.LsmEntry) error {
	// Write timestamp (8 bytes, little-endian)
	if err := binary.Write(w, binary.LittleEndian, entry.Timestamp); err != nil {
		return err
	}

	// Write deleted flag (1 byte)
	deletedByte := byte(0)
	if entry.Deleted {
		deletedByte = 1
	}
	if err := binary.Write(w, binary.LittleEndian, deletedByte); err != nil {
		return err
	}

	// Write key length (4 bytes, little-endian)
	keyBytes := []byte(key)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
		return err
	}

	// Write value length (4 bytes, little-endian)
	valueBytes := []byte(entry.Value)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(valueBytes))); err != nil {
		return err
	}

	// Write key bytes
	if _, err := w.Write(keyBytes); err != nil {
		return err
	}

	// Write value bytes
	_, err := w.Write(valueBytes)
	return err
}
