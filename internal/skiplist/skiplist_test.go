package skiplist

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Helper function to dump skiplist contents using the iterator
func dumpSkipList(t *testing.T, list *SkipList[string, string]) {
	t.Helper()
	var items []string
	for key, value := range list.Items() {
		items = append(items, fmt.Sprintf("(%s: %s)", key, value))
	}
	t.Logf("SkipList contents: [%s]", strings.Join(items, ", "))
}

// Helper function to visualize the entire skip list hierarchy
func dumpSkipListHierarchy(t *testing.T, list *SkipList[string, string]) {
	t.Helper()

	if list.head == nil {
		t.Log("SkipList is empty")
		return
	}

	maxLevel := len(list.head.next) - 1
	t.Logf("SkipList height: %d levels (0-%d)", maxLevel+1, maxLevel)

	// Print each level from top to bottom
	for level := maxLevel; level >= 0; level-- {
		var levelNodes []string
		p := list.head

		// Traverse the current level
		for p != nil {
			levelNodes = append(levelNodes, fmt.Sprintf("%s", p.key))
			if level < len(p.next) {
				p = p.next[level].Load()
			} else {
				break
			}
		}

		t.Logf("Level %d: %s", level, strings.Join(levelNodes, " -> "))
	}
}

func TestSkipListInsert(t *testing.T) {
	skipList := NewSkipList[string, string]()
	const key string = "Key"
	const value string = "Value"
	skipList.Insert(key, value)
	res, pres := skipList.Get(key)

	if !pres {
		t.Errorf("Couldn't find key in map")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}

	if res != value {
		t.Errorf(`Get("Key") = %s, want "%s"`, res, value)
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListGetEmpty(t *testing.T) {
	skipList := NewSkipList[string, string]()
	_, found := skipList.Get("nonexistent")

	if found {
		t.Error("Expected Get on empty skiplist to return false")
		dumpSkipList(t, skipList)
	}
}

func TestSkipListGetNonExistent(t *testing.T) {
	skipList := NewSkipList[string, string]()
	skipList.Insert("a", "1")
	skipList.Insert("c", "3")

	_, found := skipList.Get("b")
	if found {
		t.Error("Expected Get for non-existent key to return false")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListMultipleInserts(t *testing.T) {
	skipList := NewSkipList[string, string]()
	testData := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "red",
		"date":   "brown",
	}

	// Insert all items
	for k, v := range testData {
		skipList.Insert(k, v)
	}

	// Verify all items can be retrieved
	failed := false
	for k, expectedV := range testData {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != expectedV {
			t.Errorf("Get(%s) = %s, want %s", k, v, expectedV)
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListInsertAscending(t *testing.T) {
	skipList := NewSkipList[string, string]()
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	for _, k := range keys {
		skipList.Insert(k, k+"_value")
	}

	failed := false
	for _, k := range keys {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != k+"_value" {
			t.Errorf("Get(%s) = %s, want %s", k, v, k+"_value")
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListInsertDescending(t *testing.T) {
	skipList := NewSkipList[string, string]()
	keys := []string{"j", "i", "h", "g", "f", "e", "d", "c", "b", "a"}

	for _, k := range keys {
		skipList.Insert(k, k+"_value")
	}

	failed := false
	for _, k := range keys {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != k+"_value" {
			t.Errorf("Get(%s) = %s, want %s", k, v, k+"_value")
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListUpdateValue(t *testing.T) {
	skipList := NewSkipList[string, string]()
	key := "key1"

	// Insert initial value
	skipList.Insert(key, "value1")
	v, found := skipList.Get(key)
	if !found || v != "value1" {
		t.Errorf("Initial insert failed")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}

	// Update with new value
	skipList.Insert(key, "value2")
	v, found = skipList.Get(key)
	if !found {
		t.Error("Key not found after update")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}

	// Note: This test documents current behavior - may need adjustment
	// if skiplist should prevent duplicates
	t.Logf("After duplicate insert, Get(%s) = %s", key, v)
}

func TestSkipListEdgeCases(t *testing.T) {
	skipList := NewSkipList[string, string]()

	// Empty string keys
	skipList.Insert("", "empty_key")
	v, found := skipList.Get("")
	if !found || v != "empty_key" {
		t.Error("Failed to handle empty string key")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}

	// Very long keys
	longKey := string(make([]byte, 1000))
	skipList.Insert(longKey, "long")
	v, found = skipList.Get(longKey)
	if !found || v != "long" {
		t.Error("Failed to handle long key")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListKeyOrdering(t *testing.T) {
	skipList := NewSkipList[string, string]()

	// Test lexicographic ordering
	keys := []string{"a", "aa", "aaa", "b", "ab", "abc"}
	for _, k := range keys {
		skipList.Insert(k, k)
	}

	failed := false
	for _, k := range keys {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != k {
			t.Errorf("Get(%s) = %s, want %s", k, v, k)
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListNumericStringKeys(t *testing.T) {
	skipList := NewSkipList[string, string]()

	// Note: These are lexicographic, not numeric ordering
	// "10" < "2" in lexicographic order
	keys := []string{"1", "10", "2", "20", "3"}
	for _, k := range keys {
		skipList.Insert(k, "val_"+k)
	}

	failed := false
	for _, k := range keys {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != "val_"+k {
			t.Errorf("Get(%s) = %s, want %s", k, v, "val_"+k)
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListLargeDataset(t *testing.T) {
	skipList := NewSkipList[string, string]()
	n := 1000

	// Insert many items
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%d", i)
		skipList.Insert(key, value)
	}

	// Verify all items
	failed := false
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		v, found := skipList.Get(key)
		if !found {
			t.Errorf("Key %s not found", key)
			failed = true
		}
		if v != expectedValue {
			t.Errorf("Get(%s) = %s, want %s", key, v, expectedValue)
			failed = true
		}
	}

	if failed {
		// Only dump contents for large dataset, hierarchy would be too big
		dumpSkipList(t, skipList)
	}
}

// Append-only / arena tests

func TestArenaFullSignalsFull(t *testing.T) {
	// Small capacity so a handful of inserts exhausts the pools.
	list := NewSkipListWithCapacity[string, string](64)

	full := false
	inserted := 0
	for i := 0; i < 1_000_000 && !full; i++ {
		full = list.Insert(fmt.Sprintf("key%06d", i), "value")
		if !full {
			inserted++
		}
	}

	if !full {
		t.Fatal("expected Insert to eventually report full with a small budget")
	}
	if inserted == 0 {
		t.Fatal("expected at least one successful insert before the arena filled (BR-6)")
	}
	if list.Size() != inserted {
		t.Errorf("Size() = %d, want %d (full inserts must not count)", list.Size(), inserted)
	}
	// Entries that were inserted before full must still be readable.
	if _, ok := list.Get("key000000"); !ok {
		t.Error("first inserted key not found after arena filled")
	}
}

func TestUpdateDoesNotConsumeNodeOrGrowSize(t *testing.T) {
	list := NewSkipList[string, string]()
	if full := list.Insert("k", "v1"); full {
		t.Fatal("unexpected full on first insert")
	}
	if list.Size() != 1 {
		t.Fatalf("Size() = %d, want 1", list.Size())
	}

	list.Insert("k", "v2")
	list.Insert("k", "v3")

	if list.Size() != 1 {
		t.Errorf("Size() = %d, want 1 after updates", list.Size())
	}
	if v, ok := list.Get("k"); !ok || v != "v3" {
		t.Errorf("Get(k) = %q, %v; want v3, true", v, ok)
	}
}

func TestFreeReleasesArena(t *testing.T) {
	list := NewSkipList[string, string]()
	for i := 0; i < 1000; i++ {
		list.Insert(fmt.Sprintf("key%04d", i), "value")
	}
	// Free must not panic, and a second Free must be a no-op.
	list.Free()
	list.Free()
}

// Benchmark tests
func BenchmarkSkipListInsert(b *testing.B) {
	// Size the pools to b.N so the arena never reports full during the run.
	skipList := NewSkipListWithCapacity[string, string](b.N + 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		skipList.Insert(key, "value")
	}
}

func BenchmarkSkipListInsertParallel(b *testing.B) {
	skipList := NewSkipListWithCapacity[string, string](b.N + 1)
	var ctr int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&ctr, 1)
			skipList.Insert(fmt.Sprintf("key%d", i), "value")
		}
	})
}

func BenchmarkSkipListGet(b *testing.B) {
	skipList := NewSkipList[string, string]()
	// Pre-populate with data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%04d", i)
		skipList.Insert(key, "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%04d", i%10000)
		skipList.Get(key)
	}
}

// Concurrency stress tests

func TestConcurrentInserts(t *testing.T) {
	skipList := NewSkipList[int, int]()
	numGoroutines := 10
	insertsPerGoroutine := 1000

	done := make(chan bool, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < insertsPerGoroutine; i++ {
				key := goroutineID*insertsPerGoroutine + i
				skipList.Insert(key, key*10)
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines
	for g := 0; g < numGoroutines; g++ {
		<-done
	}

	// Verify all keys were inserted
	expectedCount := numGoroutines * insertsPerGoroutine
	if skipList.Size() != expectedCount {
		t.Errorf("Expected size %d, got %d", expectedCount, skipList.Size())
	}

	// Verify all keys are retrievable
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < insertsPerGoroutine; i++ {
			key := g*insertsPerGoroutine + i
			val, found := skipList.Get(key)
			if !found {
				t.Errorf("Key %d not found", key)
			}
			if val != key*10 {
				t.Errorf("Key %d has value %d, expected %d", key, val, key*10)
			}
		}
	}
}

func TestConcurrentInsertsOverlapping(t *testing.T) {
	skipList := NewSkipList[int, int]()
	numGoroutines := 20
	insertsPerGoroutine := 500
	keyRange := 1000 // Smaller than total inserts to force overlap

	done := make(chan bool, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < insertsPerGoroutine; i++ {
				key := (goroutineID*insertsPerGoroutine + i) % keyRange
				skipList.Insert(key, goroutineID*1000+i)
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines
	for g := 0; g < numGoroutines; g++ {
		<-done
	}

	// Verify size is at most keyRange (due to overlaps)
	if skipList.Size() > keyRange {
		t.Errorf("Size should not exceed %d, got %d", keyRange, skipList.Size())
	}

	// Verify all keys in range exist
	for i := 0; i < keyRange; i++ {
		_, found := skipList.Get(i)
		if !found {
			t.Errorf("Key %d not found", i)
		}
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	skipList := NewSkipList[int, int]()
	numReaders := 10
	numWriters := 10
	duration := 2 // seconds
	keyRange := 1000

	// Pre-populate some data
	for i := 0; i < keyRange/2; i++ {
		skipList.Insert(i, i)
	}

	done := make(chan bool)

	// Start writers
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			start := time.Now()
			count := 0
			for time.Since(start).Seconds() < float64(duration) {
				key := (writerID*1000 + count) % keyRange
				skipList.Insert(key, writerID*10000+count)
				count++
			}
			done <- true
		}(w)
	}

	// Start readers
	for r := 0; r < numReaders; r++ {
		go func(readerID int) {
			start := time.Now()
			count := 0
			for time.Since(start).Seconds() < float64(duration) {
				key := (readerID*100 + count) % keyRange
				skipList.Get(key)
				count++
			}
			done <- true
		}(r)
	}

	// Wait for all goroutines
	for i := 0; i < numReaders+numWriters; i++ {
		<-done
	}

	t.Logf("Final size: %d", skipList.Size())
}

func TestConcurrentIteratorWithWrites(t *testing.T) {
	skipList := NewSkipList[int, int]()
	keyRange := 1000
	duration := 2 // seconds

	// Pre-populate
	for i := 0; i < keyRange; i++ {
		skipList.Insert(i, i)
	}

	done := make(chan bool)

	// Start a writer
	go func() {
		start := time.Now()
		count := 0
		for time.Since(start).Seconds() < float64(duration) {
			key := count % keyRange
			skipList.Insert(key, count)
			count++
		}
		done <- true
	}()

	// Start multiple iterators
	numIterators := 5
	for i := 0; i < numIterators; i++ {
		go func() {
			start := time.Now()
			iterCount := 0
			for time.Since(start).Seconds() < float64(duration) {
				count := 0
				for range skipList.Items() {
					count++
				}
				iterCount++
			}
			done <- true
		}()
	}

	// Wait for all (1 writer + iterators)
	for i := 0; i < 1+numIterators; i++ {
		<-done
	}

	t.Logf("Test completed without deadlock")
}

func TestHighContentionInserts(t *testing.T) {
	skipList := NewSkipList[int, int]()
	numGoroutines := 50
	insertsPerGoroutine := 200
	hotKeyRange := 100 // Small range to force high contention

	done := make(chan bool, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < insertsPerGoroutine; i++ {
				key := i % hotKeyRange
				skipList.Insert(key, goroutineID*10000+i)
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines
	for g := 0; g < numGoroutines; g++ {
		<-done
	}

	// Size should be at most hotKeyRange
	if skipList.Size() > hotKeyRange {
		t.Errorf("Size should not exceed %d, got %d", hotKeyRange, skipList.Size())
	}

	// All keys in hot range should exist
	for i := 0; i < hotKeyRange; i++ {
		_, found := skipList.Get(i)
		if !found {
			t.Errorf("Key %d not found", i)
		}
	}
}

func TestStressTestMixedWorkload(t *testing.T) {
	skipList := NewSkipList[int, int]()
	duration := 3 // seconds
	keyRange := 2000

	// Pre-populate
	for i := 0; i < keyRange/2; i++ {
		skipList.Insert(i, i)
	}

	done := make(chan bool)

	// Heavy inserters
	for i := 0; i < 15; i++ {
		go func(id int) {
			start := time.Now()
			count := 0
			for time.Since(start).Seconds() < float64(duration) {
				key := (id*10000 + count) % keyRange
				skipList.Insert(key, count)
				count++
			}
			done <- true
		}(i)
	}

	// Readers
	for i := 0; i < 10; i++ {
		go func(id int) {
			start := time.Now()
			count := 0
			for time.Since(start).Seconds() < float64(duration) {
				key := (id*5000 + count) % keyRange
				skipList.Get(key)
				count++
			}
			done <- true
		}(i)
	}

	// Iterators
	for i := 0; i < 3; i++ {
		go func() {
			start := time.Now()
			for time.Since(start).Seconds() < float64(duration) {
				count := 0
				for range skipList.Items() {
					count++
					if count > keyRange*2 {
						// Safety check to prevent infinite loops
						break
					}
				}
			}
			done <- true
		}()
	}

	// Wait for all (15 inserters + 10 readers + 3 iterators = 28 goroutines)
	for i := 0; i < 28; i++ {
		<-done
	}

	t.Logf("Stress test completed. Final size: %d", skipList.Size())
}

func TestDeadlockDetection(t *testing.T) {
	skipList := NewSkipList[int, int]()
	numGoroutines := 100
	insertsPerGoroutine := 100
	timeout := 10 // seconds

	done := make(chan bool, numGoroutines)
	completed := make(chan bool)

	// Start goroutines
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < insertsPerGoroutine; i++ {
				// Insert with some overlap to increase contention
				key := (goroutineID*10 + i) % 500
				skipList.Insert(key, goroutineID*1000+i)
			}
			done <- true
		}(g)
	}

	// Monitor completion
	go func() {
		for g := 0; g < numGoroutines; g++ {
			<-done
		}
		completed <- true
	}()

	// Wait with timeout
	select {
	case <-completed:
		t.Logf("All goroutines completed successfully")
	case <-time.After(time.Duration(timeout) * time.Second):
		t.Fatalf("Test timed out after %d seconds - likely deadlock or livelock detected", timeout)
	}
}

func TestLivelockDetection(t *testing.T) {
	skipList := NewSkipList[int, int]()
	numGoroutines := 2
	duration := 5 // seconds

	successCounts := make([]int64, numGoroutines)
	done := make(chan bool, numGoroutines)

	// Create high contention scenario
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			count := int64(0)
			start := time.Now()
			for time.Since(start).Seconds() < float64(duration) {
				// All goroutines try to insert into the same small key range
				key := count % 10
				skipList.Insert(int(key), int(count))
				count++
			}
			successCounts[goroutineID] = count
			done <- true
		}(g)
	}

	// Wait for all
	for g := 0; g < numGoroutines; g++ {
		<-done
	}

	// Check if any goroutine made very little progress (potential livelock)
	totalOps := int64(0)
	minOps := successCounts[0]
	for _, count := range successCounts {
		totalOps += count
		if count < minOps {
			minOps = count
		}
	}

	avgOps := totalOps / int64(numGoroutines)
	t.Logf("Total operations: %d, Average per goroutine: %d, Min: %d", totalOps, avgOps, minOps)

	// If minimum is less than 10% of average, might indicate livelock
	if minOps < avgOps/10 {
		t.Logf("WARNING: Possible livelock detected - goroutine made minimal progress (min: %d, avg: %d)", minOps, avgOps)
	}
}
