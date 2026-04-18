package wal

import (
	"io"
	"encoding/binary"
	"fmt"
)

func (l *WriteLog) WriteTo(w io.Writer) (n int64, err error) {
	err = binary.Write(w, binary.LittleEndian, WriteType)
	if err != nil {
		return n, fmt.Errorf("Failed to write WriteLog: %w", err)
	}
	n += 1

	err = binary.Write(w, binary.LittleEndian, l.timestamp)
	if err != nil {
		return n, fmt.Errorf("Failed to write WriteLog: %w", err)
	}

	bytesWritten, err := writeStringTo(w, l.key)
	if err != nil {
		return n, fmt.Errorf("Failed to write WriteLog: %w", err)
	}
	n += bytesWritten

	bytesWritten, err = writeStringTo(w, l.value)
	if err != nil {
		return n, fmt.Errorf("Failed to write WriteLog: %w", err)
	}
	n += bytesWritten
	return
}

func (l *DeleteLog) WriteTo(w io.Writer) (n int64, err error) {
	err = binary.Write(w, binary.LittleEndian, DeleteType)
	if err != nil {
		return n, fmt.Errorf("Failed to write DeleteLog: %w", err)
	}
	n += 1

	err = binary.Write(w, binary.LittleEndian, l.timestamp)
	if err != nil {
		return n, fmt.Errorf("Failed to write DeleteLog: %w", err)
	}

	bytesWritten, err := writeStringTo(w, l.key)
	if err != nil {
		return n, fmt.Errorf("Failed to write DeleteLog: %w", err)
	}
	n += bytesWritten
	return
}

func (l *CheckpointLog) WriteTo(w io.Writer) (n int64, err error) {
	err = binary.Write(w, binary.LittleEndian, CheckpointType)
	if err != nil {
		return n, fmt.Errorf("Failed to write CheckpointLog: %w", err)
	}
	n += 1

	err = binary.Write(w, binary.LittleEndian, l.timestamp)
	if err != nil {
		return n, fmt.Errorf("Failed to write CheckpointLog: %w", err)
	}
	n += 8

	return
}
