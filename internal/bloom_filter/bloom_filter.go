package bloom_filter

import (
	"hash"
	"errors"
	"github.com/twmb/murmur3"
	"io"
	"encoding/binary"
)

type BloomFilter struct {
	filters []hash.Hash32 // k hash functions
	bits []byte	// m bits
}

// Maybe do this auto scaling with a minimum acceptable rate?
func New(numBits uint, numHashFunctions uint) (*BloomFilter, error) {
	if numBits == 0 {
		return nil, errors.New("numBits must be a positive number (> 0)")
	}

	if numHashFunctions == 0 {
		return nil, errors.New("numHashFunctions must be a positive number (> 0)")
	}

	// Ensure that the number of bits is a power of 2 otherwise we will skew
	// our insertions.
	if numBits & (numBits - 1) != 0 {
		return nil, errors.New("numBits must be a power of 2")
	}

	bloomFilter := &BloomFilter{
		filters: make([]hash.Hash32, numHashFunctions),
		bits: make([]byte, numBits),
	}

	for i := range numHashFunctions {
		bloomFilter.filters[i] = murmur3.SeedNew32(uint32(i))
	}

	return bloomFilter, nil
}

func (bf *BloomFilter) Insert(value []byte) {
	for _, filter := range bf.filters {
		filter.Write(value)
		index := filter.Sum32() % uint32(len(bf.bits))
		filter.Reset()
		bf.bits[index] = 1
	}
}

// maybe rename this since it obvisouly isn't true. Can give false positives
func (bf *BloomFilter) Present(value []byte) bool {
	for _, filter := range bf.filters {
		filter.Write(value)
		index := filter.Sum32() % uint32(len(bf.bits))
		filter.Reset()
		if bf.bits[index] != 1 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Serialize(w io.Writer) error {
	// Write number of hash functions
	if err := binary.Write(w, binary.LittleEndian, uint16(len(bf.filters))); err != nil {
		return err
	}

	// Write number of bits
	if err := binary.Write(w, binary.LittleEndian, uint16(len(bf.bits))); err != nil {
		return err
	}


	// write out bit array
	if err := binary.Write(w, binary.LittleEndian, bf.bits); err != nil {
		return err
	}

	return nil
}

func DeserializeBloomFilter(r io.Reader) (*BloomFilter, error) {
	var numHashFunctions uint16
	if err := binary.Read(r, binary.LittleEndian, &numHashFunctions); err != nil {
		return nil, err
	}

	var numBits uint16
	if err := binary.Read(r, binary.LittleEndian, &numBits); err != nil {
		return nil, err
	}

	bf, err := New(uint(numBits), uint(numHashFunctions))
	if err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &bf.bits); err != nil {
		return nil, err
	}

	return bf, nil
}
