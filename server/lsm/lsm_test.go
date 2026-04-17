package lsm

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/EricHayter/yakv/server/common"
	"github.com/EricHayter/yakv/server/storage_manager"
)

func setupTestLSM(t *testing.T) (*LogStructuredMergeTree, func()) {
	t.Helper()

	// Clean up yakv directory before test
	os.RemoveAll(common.YakvDirectory)

	sm, err := storage_manager.New(100)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}

	lsm, err := New(sm)
	if err != nil {
		t.Fatalf("Failed to create LSM: %v", err)
	}

	cleanup := func() {
		lsm.Close()
		os.RemoveAll(common.YakvDirectory)
	}

	return lsm, cleanup
}

func TestBasicPutGet(t *testing.T) {
	lsm, cleanup := setupTestLSM(t)
	defer cleanup()

	// Put a value
	lsm.Put("key1", "value1")

	// Get it back
	value, found := lsm.Get("key1")
	if !found {
		t.Error("Expected to find key1")
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}
}

func TestGetNonExistent(t *testing.T) {
	lsm, cleanup := setupTestLSM(t)
	defer cleanup()

	_, found := lsm.Get("nonexistent")
	if found {
		t.Error("Should not find nonexistent key")
	}
}

func TestDelete(t *testing.T) {
	lsm, cleanup := setupTestLSM(t)
	defer cleanup()

	// Put a value
	lsm.Put("key1", "value1")

	// Verify it exists
	value, found := lsm.Get("key1")
	if !found || value != "value1" {
		t.Error("Key should exist before delete")
	}

	// Delete it
	lsm.Delete("key1")

	// Verify it's gone
	_, found = lsm.Get("key1")
	if found {
		t.Error("Key should not exist after delete")
	}
}

func TestUpdateValue(t *testing.T) {
	lsm, cleanup := setupTestLSM(t)
	defer cleanup()

	// Put initial value
	lsm.Put("key1", "value1")

	// Update it
	lsm.Put("key1", "value2")

	// Get updated value
	value, found := lsm.Get("key1")
	if !found {
		t.Error("Expected to find key1")
	}
	if value != "value2" {
		t.Errorf("Expected value2, got %s", value)
	}
}

func TestConcurrentWrites(t *testing.T) {
	lsm, cleanup := setupTestLSM(t)
	defer cleanup()

	const numGoroutines = 10
	const writesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range writesPerGoroutine {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := fmt.Sprintf("value-%d-%d", id, j)
				lsm.Put(key, value)
			}
		}(i)
	}

	wg.Wait()

	// Verify some writes succeeded
	value, found := lsm.Get("key-0-0")
	if !found || value != "value-0-0" {
		t.Error("Expected to find key-0-0")
	}
}

func TestConcurrentReadsWrites(t *testing.T) {
	lsm, cleanup := setupTestLSM(t)
	defer cleanup()

	// Pre-populate some data
	for i := range 100 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		lsm.Put(key, value)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer goroutine
	go func() {
		defer wg.Done()
		for i := 100; i < 200; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			lsm.Put(key, value)
		}
	}()

	// Reader goroutine
	go func() {
		defer wg.Done()
		for i := range 100 {
			key := fmt.Sprintf("key-%d", i)
			lsm.Get(key)
		}
	}()

	wg.Wait()
}

func TestDeletedFlagRespected(t *testing.T) {
	lsm, cleanup := setupTestLSM(t)
	defer cleanup()

	// Put, delete, then verify deleted flag works
	lsm.Put("key1", "value1")
	lsm.Delete("key1")

	// Should not find deleted key
	_, found := lsm.Get("key1")
	if found {
		t.Error("Deleted key should not be found")
	}
}

func TestMemtableSizeTracking(t *testing.T) {
	lsm, cleanup := setupTestLSM(t)
	defer cleanup()

	initialSize := atomic.LoadUint64(&lsm.memtableSize)

	// Add an entry
	lsm.Put("key", "value")

	// Size should have increased
	currentSize := atomic.LoadUint64(&lsm.memtableSize)
	if currentSize <= initialSize {
		t.Error("Memtable size should increase after Put")
	}

	// Delete an entry
	lsm.Delete("key2")

	// Size should increase even for deletes (tombstones take space)
	expectedMinSize := initialSize + uint64(len("key")+len("value")+8+1) + uint64(len("key2")+8+1)
	finalSize := atomic.LoadUint64(&lsm.memtableSize)
	if finalSize < expectedMinSize {
		t.Errorf("Memtable size should be at least %d, got %d", expectedMinSize, finalSize)
	}
}

func TestTimestampIncreases(t *testing.T) {
	lsm, cleanup := setupTestLSM(t)
	defer cleanup()

	lsm.Put("key1", "value1")
	ts1 := atomic.LoadUint64(&lsm.lastTimestamp)

	lsm.Put("key2", "value2")
	ts2 := atomic.LoadUint64(&lsm.lastTimestamp)

	if ts2 <= ts1 {
		t.Error("Timestamp should increase with each operation")
	}

	lsm.Delete("key3")
	ts3 := atomic.LoadUint64(&lsm.lastTimestamp)

	if ts3 <= ts2 {
		t.Error("Timestamp should increase for deletes too")
	}
}
