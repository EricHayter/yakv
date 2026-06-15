package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/EricHayter/yakv/server/common"
)

func walSetup(t *testing.T) string {
	t.Helper()
	os.RemoveAll(common.YakvDirectory)
	if err := os.MkdirAll(common.YakvDirectory, 0755); err != nil {
		t.Fatalf("failed to create yakv dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(common.YakvDirectory) })
	return filepath.Join(common.YakvDirectory, WriteAheadLogFileName)
}

// Writes go through the new AppendTo encoding and the decoupled flush path; the
// existing reader must decode them unchanged.
func TestWALRoundTrip(t *testing.T) {
	path := walSetup(t)

	w, _, err := NewWriteAheadLog()
	if err != nil {
		t.Fatalf("NewWriteAheadLog: %v", err)
	}
	w.Push(NewWriteLog("k1", "v1", 0))
	w.Push(NewWriteLog("k2", "v2", 0))
	w.Push(NewDeleteLog("k1", 0))
	if err := w.flushWALBuffer(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	w.Close()

	logs, err := ReadWALToCheckpoint(path)
	if err != nil {
		t.Fatalf("ReadWALToCheckpoint: %v", err)
	}
	if len(logs) != 3 {
		t.Fatalf("recovered %d logs, want 3", len(logs))
	}
	if wl, ok := logs[0].(*WriteLog); !ok || wl.Key() != "k1" || wl.Value() != "v1" {
		t.Errorf("log[0] = %+v, want WriteLog{k1,v1}", logs[0])
	}
	if wl, ok := logs[1].(*WriteLog); !ok || wl.Key() != "k2" || wl.Value() != "v2" {
		t.Errorf("log[1] = %+v, want WriteLog{k2,v2}", logs[1])
	}
	if dl, ok := logs[2].(*DeleteLog); !ok || dl.Key() != "k1" {
		t.Errorf("log[2] = %+v, want DeleteLog{k1}", logs[2])
	}
}

// Close must flush logs buffered since the last periodic flush.
func TestWALCloseFlushesBuffer(t *testing.T) {
	path := walSetup(t)

	w, _, err := NewWriteAheadLog()
	if err != nil {
		t.Fatalf("NewWriteAheadLog: %v", err)
	}
	w.Push(NewWriteLog("only", "value", 0))
	w.Close() // no explicit flush; Close must persist the buffer

	logs, err := ReadWALToCheckpoint(path)
	if err != nil {
		t.Fatalf("ReadWALToCheckpoint: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("recovered %d logs, want 1 (Close should flush)", len(logs))
	}
}

// Push must remain safe and lossless while flushes run concurrently, and the
// WAL file must record logs in LSN (timestamp) order.
func TestWALConcurrentPushDuringFlush(t *testing.T) {
	path := walSetup(t)

	w, _, err := NewWriteAheadLog()
	if err != nil {
		t.Fatalf("NewWriteAheadLog: %v", err)
	}

	const goroutines, perG = 8, 2000

	// Background flush pressure concurrent with the writers.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = w.flushWALBuffer()
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				w.Push(NewWriteLog(fmt.Sprintf("k%d-%d", id, i), "v", 0))
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	w.Close()

	logs, err := ReadWALToCheckpoint(path)
	if err != nil {
		t.Fatalf("ReadWALToCheckpoint: %v", err)
	}
	if len(logs) != goroutines*perG {
		t.Fatalf("recovered %d logs, want %d (no losses)", len(logs), goroutines*perG)
	}

	// LSNs are assigned and buffered under the same lock, and batches are flushed
	// in order, so the file must be in strictly ascending timestamp order.
	var prev uint64
	for i, lg := range logs {
		ts := lg.(*WriteLog).Timestamp()
		if i > 0 && ts <= prev {
			t.Fatalf("LSN order violated at index %d: %d <= %d", i, ts, prev)
		}
		prev = ts
	}
}
