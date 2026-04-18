package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	return 2 + int64(len(s)), nil
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

// atomicWriteFile writes data to a file atomically using temp-file-then-rename
func atomicWriteFile(targetPath string, writeFn func(io.Writer) error) error {
	dir := filepath.Dir(targetPath)
	base := filepath.Base(targetPath)

	// Create temp file in same directory (required for atomic rename)
	f, err := os.CreateTemp(dir, base+"-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := f.Name()

	// Setup cleanup on error
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(tempPath)
		}
	}()

	// Write data using provided function
	if err := writeFn(f); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Sync to ensure durability
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Close before rename
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	f = nil // Prevent defer from double-closing

	// Atomic rename
	if err := os.Rename(tempPath, targetPath); err != nil {
		os.Remove(tempPath) // Cleanup on rename failure
		return fmt.Errorf("failed to rename file atomically: %w", err)
	}

	return nil
}
