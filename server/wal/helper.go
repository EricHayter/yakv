package wal

import (
	"io"
	"encoding/binary"
	"fmt"
)

func writeStringTo(w io.Writer, s string) (n int64, err error) {
	err = binary.Write(w, binary.LittleEndian, uint16(len(s)))
	if err != nil {
		return 0, fmt.Errorf("Failed to serialize string: %w", err)
	}
	_, err = w.Write([]byte(s))
	if err != nil {
		return 2, fmt.Errorf("Failed to serialize string: %w", err)
	}
	return 2 + int64(len(s)),nil
}

func readStringFrom(r io.Reader, s *string) error {
	var strLen uint16
	err := binary.Read(r, binary.LittleEndian, &strLen)
	if err != nil {
		return fmt.Errorf("Failed to deserialize string: %w", err)
	}
	strBuf := make([]byte, strLen)
	_, err = io.ReadFull(r, strBuf)
	if err != nil {
		return fmt.Errorf("Failed to deserialize string: %w", err)
	}
	*s = string(strBuf)
	return nil
}
