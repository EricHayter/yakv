package wal

/* Write Ahead Logging
 */

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/EricHayter/yakv/server/common"
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
	lastLSN       uint64
	walPath       string
	flushSignaler chan struct{} // Triggers explicit flushes
	quit          chan struct{} // Signals: "please stop"
	done          chan struct{} // Signals: "I've stopped"

	// mu guards lastLSN and buffer only — held briefly by Push and by the
	// buffer swap in flushWALBuffer. It is NOT held during file I/O.
	mu     sync.Mutex
	buffer []Log

	// flushMu serializes flushers and is held across the swap + I/O so batches
	// are written to the file in LSN order by a single writer at a time. Push
	// never acquires it, so writers do not block on fsync.
	flushMu sync.Mutex
	file    *os.File // kept open across flushes (append mode)
	scratch []byte   // reusable serialization buffer; only touched under flushMu
}

// NewWriteAheadLog creates a new WriteAheadLog and starts the background flusher
func NewWriteAheadLog() (*WriteAheadLog, []Log, error) {
	walPath := filepath.Join(common.YakvDirectory, WriteAheadLogFileName)

	// Try to read existing WAL
	logs, err := ReadWALToCheckpoint(walPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read WAL during recovery: %w", err)
	}

	wal := &WriteAheadLog{
		lastLSN:       uint64(len(logs)),
		walPath:       walPath,
		flushSignaler: make(chan struct{}, 1), // Create channel internally
		quit:          make(chan struct{}),
		done:          make(chan struct{}),
		buffer:        make([]Log, 0),
	}

	// If we have logs, compact the WAL (remove checkpointed data)
	if len(logs) > 0 {
		if err := wal.compactWAL(logs); err != nil {
			return nil, nil, fmt.Errorf("failed to compact WAL: %w", err)
		}
	} else {
		// No existing data - create empty WAL file
		if err := wal.initializeEmptyWAL(); err != nil {
			return nil, nil, fmt.Errorf("failed to initialize empty WAL: %w", err)
		}
	}

	// Open the persistent append handle once, after any compaction/rename above,
	// and reuse it for every flush (no per-flush open/close).
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	wal.file = f

	// Start background flusher
	go wal.flusher()
	return wal, logs, nil
}

// Close stops the background flusher and waits for it to finish
func (wal *WriteAheadLog) Close() {
	close(wal.quit) // Signal shutdown
	<-wal.done      // Wait for flusher to stop

	// Flush anything buffered since the last periodic flush (the flusher has
	// stopped, so this is uncontended), then close the file handle.
	if err := wal.flushWALBuffer(); err != nil {
		slog.Error("failed to flush WAL on close", "error", err)
	}
	if wal.file != nil {
		if err := wal.file.Close(); err != nil {
			slog.Error("failed to close WAL file", "error", err)
		}
	}
}

// compactWAL creates a new WAL file containing only uncheckpointed logs
// and atomically replaces the old WAL file
func (wal *WriteAheadLog) compactWAL(logs []Log) error {
	return atomicWriteFile(wal.walPath, func(w io.Writer) error {
		for _, log := range logs {
			if _, err := log.WriteTo(w); err != nil {
				return fmt.Errorf("failed to write log during compaction: %w", err)
			}
		}
		return nil
	})
}

// initializeEmptyWAL creates a new empty WAL file
func (wal *WriteAheadLog) initializeEmptyWAL() error {
	f, err := os.OpenFile(wal.walPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create empty WAL: %w", err)
	}
	defer f.Close()

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync empty WAL: %w", err)
	}

	return nil
}

func (wal *WriteAheadLog) Push(log Log) uint64 {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	lsn := wal.lastLSN + 1
	wal.lastLSN++

	// Set timestamp on the log
	switch l := log.(type) {
	case *WriteLog:
		l.SetTimestamp(lsn)
	case *DeleteLog:
		l.SetTimestamp(lsn)
	}

	wal.buffer = append(wal.buffer, log)
	return lsn
}

func (wal *WriteAheadLog) Checkpoint() error {
	wal.mu.Lock()
	log := checkpointLog{
		timestamp: wal.lastLSN,
	}
	wal.buffer = append(wal.buffer, &log)
	wal.mu.Unlock()

	// Synchronously flush to ensure durability
	return wal.flushWALBuffer()
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
	// Serialize flushers and hold across the swap + I/O so batches are written
	// in LSN order by a single writer. Push does not take this lock.
	wal.flushMu.Lock()
	defer wal.flushMu.Unlock()

	// Swap the pending batch out under mu, then release mu before any I/O so
	// concurrent Push calls are never blocked on fsync. A fresh backing slice is
	// installed so Push cannot overwrite the batch we are about to serialize.
	wal.mu.Lock()
	if len(wal.buffer) == 0 {
		wal.mu.Unlock()
		return nil
	}
	batch := wal.buffer
	wal.buffer = make([]Log, 0, cap(batch))
	wal.mu.Unlock()

	// Serialize the batch into the reusable scratch buffer (no per-field
	// allocation), then write and fsync — all with wal.mu released.
	wal.scratch = wal.scratch[:0]
	for _, log := range batch {
		wal.scratch = log.AppendTo(wal.scratch)
	}

	if _, err := wal.file.Write(wal.scratch); err != nil {
		return fmt.Errorf("failed to write logs to WAL: %w", err)
	}
	if err := wal.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	return nil
}

// ReadWALToCheckpoint reads the WAL file and returns all logs with timestamps after the first checkpoint
func ReadWALToCheckpoint(walPath string) ([]Log, error) {
	f, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []Log{}, nil
		}
		return nil, fmt.Errorf("Failed to open WAL file: %w", err)
	}
	defer f.Close()

	var logs []Log
	var checkpointTimestamp *uint64

	for {
		var logType LogType
		err := binary.Read(f, binary.LittleEndian, &logType)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("Failed to read log type: %w", err)
		}

		switch logType {
		case CheckpointType:
			var log checkpointLog
			_, err = log.ReadFrom(f)
			if err != nil {
				return nil, fmt.Errorf("Failed to read CheckpointLog: %w", err)
			}
			// Only use the first checkpoint
			if checkpointTimestamp == nil {
				checkpointTimestamp = &log.timestamp
				// Clear any logs collected before the checkpoint
				logs = make([]Log, 0)
			}
		case WriteType:
			var log WriteLog
			_, err = log.ReadFrom(f)
			if err != nil {
				return nil, fmt.Errorf("Failed to read WriteLog: %w", err)
			}
			// If we have a checkpoint, check if we can stop
			if checkpointTimestamp != nil {
				if log.timestamp <= *checkpointTimestamp {
					// Timestamps are monotonically increasing, so we're done
					return logs, nil
				}
			}
			logs = append(logs, &log)
		case DeleteType:
			var log DeleteLog
			_, err = log.ReadFrom(f)
			if err != nil {
				return nil, fmt.Errorf("Failed to read DeleteLog: %w", err)
			}
			// If we have a checkpoint, check if we can stop
			if checkpointTimestamp != nil {
				if log.timestamp <= *checkpointTimestamp {
					// Timestamps are monotonically increasing, so we're done
					return logs, nil
				}
			}
			logs = append(logs, &log)
		default:
			return nil, fmt.Errorf("Unknown log type: %d", logType)
		}
	}

	return logs, nil
}

type Log interface {
	io.WriterTo
	io.ReaderFrom
	// AppendTo serializes the log onto dst and returns the extended slice.
	AppendTo(dst []byte) []byte
}

type WriteLog struct {
	timestamp  uint64
	key, value string
}

func NewWriteLog(key, value string, timestamp uint64) *WriteLog {
	return &WriteLog{
		timestamp: timestamp,
		key:       key,
		value:     value,
	}
}

type DeleteLog struct {
	timestamp uint64
	key       string
}

func NewDeleteLog(key string, timestamp uint64) *DeleteLog {
	return &DeleteLog{
		timestamp: timestamp,
		key:       key,
	}
}

type checkpointLog struct {
	timestamp uint64
}

func NewCheckpointLog(timestamp uint64) *checkpointLog {
	return &checkpointLog{
		timestamp: timestamp,
	}
}

// Getter methods for log fields
func (w *WriteLog) Timestamp() uint64 {
	return w.timestamp
}

func (w *WriteLog) Key() string {
	return w.key
}

func (w *WriteLog) Value() string {
	return w.value
}

func (d *DeleteLog) Timestamp() uint64 {
	return d.timestamp
}

func (d *DeleteLog) Key() string {
	return d.key
}

func (c *checkpointLog) Timestamp() uint64 {
	return c.timestamp
}

// Setter for timestamp (used by WAL.Push)
func (w *WriteLog) SetTimestamp(ts uint64) {
	w.timestamp = ts
}

func (d *DeleteLog) SetTimestamp(ts uint64) {
	d.timestamp = ts
}

func (c *checkpointLog) SetTimestamp(ts uint64) {
	c.timestamp = ts
}
