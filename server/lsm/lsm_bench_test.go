package lsm

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/EricHayter/yakv/server/disk_manager"
	"github.com/EricHayter/yakv/server/storage_manager"
)

// setupBenchLSM creates a new LSM tree for benchmarking
func setupBenchLSM(b *testing.B) *LogStructuredMergeTree {
	b.Helper()

	// Clean up yakv directory before benchmark
	os.RemoveAll(disk_manager.YakvDirectory)

	// Create storage manager with reasonable buffer capacity
	sm, err := storage_manager.New(1000)
	if err != nil {
		b.Fatalf("Failed to create storage manager: %v", err)
	}

	lsm, err := New(sm)
	if err != nil {
		b.Fatalf("Failed to create LSM: %v", err)
	}

	return lsm
}

// cleanupBenchLSM cleans up after benchmarks
func cleanupBenchLSM(b *testing.B, lsm *LogStructuredMergeTree) {
	b.Helper()
	lsm.Close()
	os.RemoveAll(disk_manager.YakvDirectory)
}

// =============================================================================
// Sequential Write Benchmarks
// =============================================================================

func BenchmarkSequentialWrites(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}
}

func BenchmarkSequentialWritesSmallValues(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%d", i)
		value := "v"
		lsm.Put(key, value)
	}
}

func BenchmarkSequentialWritesLargeValues(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// 1KB value
	largeValue := string(make([]byte, 1024))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i)
		lsm.Put(key, largeValue)
	}
}

// =============================================================================
// Random Write Benchmarks
// =============================================================================

func BenchmarkRandomWrites(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyNum := rng.Intn(1000000)
		key := fmt.Sprintf("key%010d", keyNum)
		value := fmt.Sprintf("value%010d", keyNum)
		lsm.Put(key, value)
	}
}

func BenchmarkRandomWritesHighCardinality(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyNum := rng.Intn(10000000)
		key := fmt.Sprintf("key%010d", keyNum)
		value := fmt.Sprintf("value%010d", keyNum)
		lsm.Put(key, value)
	}
}

// =============================================================================
// Read Benchmarks
// =============================================================================

func BenchmarkReadsMemtableHit(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Pre-populate with some data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyNum := rng.Intn(10000)
		key := fmt.Sprintf("key%010d", keyNum)
		lsm.Get(key)
	}
}

func BenchmarkReadsMiss(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Pre-populate with some data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Keys that don't exist
		key := fmt.Sprintf("miss%010d", i)
		lsm.Get(key)
	}
}

// =============================================================================
// Delete Benchmarks
// =============================================================================

func BenchmarkDeletes(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Pre-populate with data
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i)
		lsm.Delete(key)
	}
}

// =============================================================================
// Mixed Workload Benchmarks
// =============================================================================

func BenchmarkMixedWorkload_90Read_10Write(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Pre-populate with data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rng.Intn(100) < 90 {
			// Read
			keyNum := rng.Intn(10000)
			key := fmt.Sprintf("key%010d", keyNum)
			lsm.Get(key)
		} else {
			// Write
			keyNum := rng.Intn(10000)
			key := fmt.Sprintf("key%010d", keyNum)
			value := fmt.Sprintf("value%010d", keyNum)
			lsm.Put(key, value)
		}
	}
}

func BenchmarkMixedWorkload_50Read_50Write(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Pre-populate with data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rng.Intn(100) < 50 {
			// Read
			keyNum := rng.Intn(10000)
			key := fmt.Sprintf("key%010d", keyNum)
			lsm.Get(key)
		} else {
			// Write
			keyNum := rng.Intn(10000)
			key := fmt.Sprintf("key%010d", keyNum)
			value := fmt.Sprintf("value%010d", keyNum)
			lsm.Put(key, value)
		}
	}
}

func BenchmarkMixedWorkload_70Read_25Write_5Delete(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Pre-populate with data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roll := rng.Intn(100)
		keyNum := rng.Intn(10000)
		key := fmt.Sprintf("key%010d", keyNum)

		if roll < 70 {
			// Read
			lsm.Get(key)
		} else if roll < 95 {
			// Write
			value := fmt.Sprintf("value%010d", keyNum)
			lsm.Put(key, value)
		} else {
			// Delete
			lsm.Delete(key)
		}
	}
}

// =============================================================================
// Write-Heavy with Flushes
// =============================================================================

func BenchmarkWriteHeavyWithFlushes(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Write enough data to trigger flushes
	// memtableSizeThreshold is 64MB, with ~30 byte entries we need ~2M entries

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}
}

// =============================================================================
// Update-Heavy Benchmarks (overwriting existing keys)
// =============================================================================

func BenchmarkUpdateHeavy(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Pre-populate with 1000 keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Keep updating the same 1000 keys
		keyNum := rng.Intn(numKeys)
		key := fmt.Sprintf("key%010d", keyNum)
		value := fmt.Sprintf("updated%010d", i)
		lsm.Put(key, value)
	}
}

// =============================================================================
// Realistic OLTP-Style Workload
// =============================================================================

func BenchmarkOLTPWorkload(b *testing.B) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Pre-populate database with 100k records
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("user:%010d", i)
		value := fmt.Sprintf(`{"name":"User %d","email":"user%d@example.com","balance":%.2f"}`, i, i, float64(i)*1.5)
		lsm.Put(key, value)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roll := rng.Intn(100)
		userID := rng.Intn(100000)
		key := fmt.Sprintf("user:%010d", userID)

		if roll < 60 {
			// Point read (60%)
			lsm.Get(key)
		} else if roll < 85 {
			// Update (25%)
			value := fmt.Sprintf(`{"name":"User %d","email":"user%d@example.com","balance":%.2f"}`, userID, userID, float64(i)*2.0)
			lsm.Put(key, value)
		} else if roll < 95 {
			// Insert new record (10%)
			newUserID := 100000 + i
			key := fmt.Sprintf("user:%010d", newUserID)
			value := fmt.Sprintf(`{"name":"User %d","email":"user%d@example.com","balance":0.00"}`, newUserID, newUserID)
			lsm.Put(key, value)
		} else {
			// Delete (5%)
			lsm.Delete(key)
		}
	}
}

// =============================================================================
// Concurrent Write Benchmarks
// =============================================================================

func benchmarkConcurrentWrites(b *testing.B, numGoroutines int) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%010d", i)
			value := fmt.Sprintf("value%010d", i)
			lsm.Put(key, value)
			i++
		}
	})
}

func BenchmarkConcurrentWrites_1Thread(b *testing.B) {
	b.SetParallelism(1)
	benchmarkConcurrentWrites(b, 1)
}

func BenchmarkConcurrentWrites_2Threads(b *testing.B) {
	b.SetParallelism(2)
	benchmarkConcurrentWrites(b, 2)
}

func BenchmarkConcurrentWrites_4Threads(b *testing.B) {
	b.SetParallelism(4)
	benchmarkConcurrentWrites(b, 4)
}

func BenchmarkConcurrentWrites_8Threads(b *testing.B) {
	b.SetParallelism(8)
	benchmarkConcurrentWrites(b, 8)
}

// =============================================================================
// Concurrent Mixed Workload Benchmarks
// =============================================================================

func benchmarkConcurrentMixed(b *testing.B, readPct int) {
	lsm := setupBenchLSM(b)
	defer cleanupBenchLSM(b, lsm)

	// Pre-populate with data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		lsm.Put(key, value)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		i := 0
		for pb.Next() {
			if rng.Intn(100) < readPct {
				// Read
				keyNum := rng.Intn(10000)
				key := fmt.Sprintf("key%010d", keyNum)
				lsm.Get(key)
			} else {
				// Write
				key := fmt.Sprintf("key%010d", i)
				value := fmt.Sprintf("value%010d", i)
				lsm.Put(key, value)
				i++
			}
		}
	})
}

func BenchmarkConcurrentMixed_90Read_10Write_4Threads(b *testing.B) {
	b.SetParallelism(4)
	benchmarkConcurrentMixed(b, 90)
}

func BenchmarkConcurrentMixed_50Read_50Write_4Threads(b *testing.B) {
	b.SetParallelism(4)
	benchmarkConcurrentMixed(b, 50)
}

func BenchmarkConcurrentMixed_10Read_90Write_4Threads(b *testing.B) {
	b.SetParallelism(4)
	benchmarkConcurrentMixed(b, 10)
}
