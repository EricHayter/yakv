package wal

/* Write Ahead Logging
 */

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

type LogType uint8

const (
	CheckpointType LogType = iota
	WriteType
	DeleteType
)

const (
	WriteAheadLogFileName = "wal"
)

type WriteAheadLog struct {
	flushSignaler  <-chan struct{} // Triggers explicit flushes
	quit           chan struct{}   // Signals: "please stop"
	done           chan struct{}   // Signals: "I've stopped"
	mu             sync.Mutex
	buffer         []Log
}

// NewWriteAheadLog creates a new WriteAheadLog and starts the background flusher
func NewWriteAheadLog(flushSignaler <-chan struct{}) *WriteAheadLog {
	wal := &WriteAheadLog{
		flushSignaler: flushSignaler,
		quit:          make(chan struct{}),
		done:          make(chan struct{}),
		buffer:        make([]Log, 0),
	}

	// Start background flusher
	go wal.flusher()
	return wal
}

// Close stops the background flusher and waits for it to finish
func (wal *WriteAheadLog) Close() {
	close(wal.quit) // Signal shutdown
	<-wal.done      // Wait for flusher to stop
}

func (wal *WriteAheadLog) Push(log Log) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	wal.buffer = append(wal.buffer, log)
}

// goroutine for flushing log files
func (wal *WriteAheadLog) flusher() {
	defer close(wal.done) // Signal when we're done
	for {
		select {
		case <-wal.flushSignaler:
			// Explicit signal from LSM (e.g., sstables changed)
			err := wal.flushWALBuffer()
			if err != nil {
				slog.Error(err.Error())
			}
		case <-time.After(50 * time.Millisecond):
			// Periodic flush to catch timestamp updates
			err := wal.flushWALBuffer()
			if err != nil {
				slog.Error(err.Error())
			}
		case <-wal.quit:
			return // Clean shutdown
		}
	}
}

func (wal *WriteAheadLog) flushWALBuffer() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	writeAheadLogPath := "TODOFIX THIS LATER"
	f, err := os.OpenFile(writeAheadLogPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("Failed to open WAL file: %s", writeAheadLogPath)
	}
	defer f.Close()

	for _, log := range wal.buffer {
		_, err = log.WriteTo(f)
		if err != nil {
			return fmt.Errorf("Failed write log to WAL file: %s", writeAheadLogPath)
		}
	}

	return nil
}

type Log interface {
	io.WriterTo
	io.ReaderFrom
}

type WriteLog struct {
	timestamp uint64
	key, value string
}

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

type DeleteLog struct {
	timestamp uint64
	key string
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

type CheckpointLog struct {}

func (l *CheckpointLog) WriteTo(w io.Writer) (n int64, err error) {
	err = binary.Write(w, binary.LittleEndian, CheckpointType)
	if err != nil {
		return n, fmt.Errorf("Failed to write CheckpointLog: %w", err)
	}
	n += 1
	return
}
