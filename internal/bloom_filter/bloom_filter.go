package bloom_filter

import (
	"errors"
	"github.com/twmb/murmur3"
	"hash"
)

type BloomFilter struct {
	filters []hash.Hash32 // k hash functions
	Bits    []bool        // m bits
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
	if numBits&(numBits-1) != 0 {
		return nil, errors.New("numBits must be a power of 2")
	}

	bloomFilter := &BloomFilter{
		filters: make([]hash.Hash32, numHashFunctions),
		Bits:    make([]bool, numBits),
	}

	for i := range numHashFunctions {
		bloomFilter.filters[i] = murmur3.SeedNew32(uint32(i))
	}

	return bloomFilter, nil
}

func (bf *BloomFilter) Insert(value []byte) {
	for _, filter := range bf.filters {
		filter.Write(value)
		index := filter.Sum32() % uint32(len(bf.Bits))
		filter.Reset()
		bf.Bits[index] = true
	}
}

// maybe rename this since it obvisouly isn't true. Can give false positives
func (bf *BloomFilter) Present(value []byte) bool {
	for _, filter := range bf.filters {
		filter.Write(value)
		index := filter.Sum32() % uint32(len(bf.Bits))
		filter.Reset()
		if !bf.Bits[index] {
			return false
		}
	}
	return true
}
