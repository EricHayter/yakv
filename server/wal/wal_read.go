package wal

import (
	"encoding/binary"
	"fmt"
	"io"
)

func (l *WriteLog) ReadFrom(r io.Reader) (n int64, err error) {
	err = binary.Read(r, binary.LittleEndian, &l.timestamp)
	if err != nil {
		return n, fmt.Errorf("Failed to read WriteLog: %w", err)
	}
	n += 8 // uint64 is 8 bytes

	err = readStringFrom(r, &l.key)
	if err != nil {
		return n, fmt.Errorf("Failed to read WriteLog: %w", err)
	}
	n += 2 + int64(len(l.key)) // uint16 length prefix + string bytes

	err = readStringFrom(r, &l.value)
	if err != nil {
		return n, fmt.Errorf("Failed to read WriteLog: %w", err)
	}
	n += 2 + int64(len(l.value)) // uint16 length prefix + string bytes

	return
}

func (l *DeleteLog) ReadFrom(r io.Reader) (n int64, err error) {
	err = binary.Read(r, binary.LittleEndian, &l.timestamp)
	if err != nil {
		return n, fmt.Errorf("Failed to read DeleteLog: %w", err)
	}
	n += 8 // uint64 is 8 bytes

	err = readStringFrom(r, &l.key)
	if err != nil {
		return n, fmt.Errorf("Failed to read DeleteLog: %w", err)
	}
	n += 2 + int64(len(l.key)) // uint16 length prefix + string bytes

	return
}

func (l *CheckpointLog) ReadFrom(r io.Reader) (n int64, err error) {
	err = binary.Read(r, binary.LittleEndian, &l.timestamp)
	if err != nil {
		return n, fmt.Errorf("Failed to read CheckpointLog: %w", err)
	}
	n += 8 // uint64 is 8 bytes

	return
}
