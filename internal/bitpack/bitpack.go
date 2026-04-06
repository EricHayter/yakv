package bitpack

/* Implement functions for bit packing, i.e. codensing booleans down into bits
 * inside of bytes. Mainly for serialization benefits
 */

import (
	"io"
	"encoding/binary"
)

func Serialize(writer io.Writer, bools []bool) error {
	n := len(bools)
	err := binary.Write(writer, binary.LittleEndian, uint16(n))
	if err != nil {
		return err
	}
	for i := 0; i < n; i += 8 {
		var packedVal uint8
		for j := 0; j < 8 && i + j < n; j++ {
			if bools[i + j] {
				packedVal |= 1 << j
			}
		}
		err = binary.Write(writer, binary.LittleEndian, packedVal)
		if err != nil {
			return err
		}
	}

	return nil
}

func Deserialize(reader io.Reader) ([]bool, error) {
	var numBools uint16
	if err := binary.Read(reader, binary.LittleEndian, &numBools); err != nil {
		return nil, err
	}
	n := int(numBools)

	bools := make([]bool, n)
	for i := 0; i < int(n); i += 8 {
		var packedVal uint8
		if err := binary.Read(reader, binary.LittleEndian, &packedVal); err != nil {
			return nil, err
		}

		for j := 0; j < 8 && i + j < n; j++ {
			if packedVal & (1 << j) != 0 {
				bools[i + j] = true
			}
		}
	}

	return bools, nil
}
