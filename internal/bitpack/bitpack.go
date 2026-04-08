package bitpack

/* Implement functions for bit packing, i.e. condensing booleans down into bits
 * inside of bytes. Mainly for serialization benefits
 */

// Pack packs a slice of bools into a slice of bytes (8 bools per byte)
func Pack(bools []bool) []byte {
	n := len(bools)
	// Calculate number of bytes needed: ceil(n / 8)
	numBytes := (n + 7) / 8
	packed := make([]byte, numBytes)

	for i := range n {
		if bools[i] {
			byteIndex := i / 8
			bitIndex := i % 8
			packed[byteIndex] |= 1 << bitIndex
		}
	}

	return packed
}

// Unpack unpacks a slice of bytes into a slice of bools
// numBools specifies how many bools to extract (needed because the last byte may be partially filled)
func Unpack(packed []byte, numBools int) []bool {
	bools := make([]bool, numBools)

	for i := range numBools {
		byteIndex := i / 8
		bitIndex := i % 8
		if packed[byteIndex]&(1<<bitIndex) != 0 {
			bools[i] = true
		}
	}

	return bools
}
