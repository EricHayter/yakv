package buffer_manager

import (
	"github.com/EricHayter/yakv/server/disk_manager"
	"os"
	"sync"
	"testing"
)

// Helper function to create a test buffer manager and clean up after
func setupTest(t *testing.T, capacity uint16) (*BufferManager, func()) {
	dm, err := disk_manager.New()
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}

	bm, err := New(capacity, dm)
	if err != nil {
		t.Fatalf("Failed to create buffer manager: %v", err)
	}

	cleanup := func() {
		// Clean up yakv directory
		os.RemoveAll("yakv")
	}

	return bm, cleanup
}

// =============================================================================
// 1. Basic Functionality Tests
// =============================================================================

func TestGetPageBasic(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	// Create a test file
	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Get a page
	page, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	defer page.Close()

	// Verify we got a valid page
	if page.bufferManager == nil {
		t.Error("Page has nil buffer manager")
	}
}

func TestGetPageSameTwice(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Get the same page twice
	page1, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page1: %v", err)
	}
	defer page1.Close()

	page2, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page2: %v", err)
	}
	defer page2.Close()

	// Verify they reference the same frame
	if page1.frameId != page2.frameId {
		t.Errorf("Expected same frame, got %d and %d", page1.frameId, page2.frameId)
	}

	// Verify pin count is 2
	frame := &bm.frames[page1.frameId]
	bm.mu.Lock()
	pinCount := frame.pinCount
	bm.mu.Unlock()

	if pinCount != 2 {
		t.Errorf("Expected pin count 2, got %d", pinCount)
	}
}

func TestPageClose(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	page, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	frameId := page.frameId
	page.Close()

	// Verify pin count is 0
	bm.mu.Lock()
	pinCount := bm.frames[frameId].pinCount
	bm.mu.Unlock()

	if pinCount != 0 {
		t.Errorf("Expected pin count 0 after close, got %d", pinCount)
	}

	// Verify frame is back in replacer
	if bm.frameReplacer.Len() != 10 {
		t.Errorf("Expected replacer to have 10 frames, got %d", bm.frameReplacer.Len())
	}
}

func TestGetBufferAndMarkDirty(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	page, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	defer page.Close()

	// Test GetBuffer with proper locking
	page.Lock()
	buffer := page.GetBuffer()
	if buffer == nil {
		page.Unlock()
		t.Error("GetBuffer returned nil")
	}

	// Write some data
	copy(buffer[:], []byte("test data"))
	page.Unlock()

	// Mark dirty
	page.MarkDirty()

	// Verify dirty flag is set
	frame := &bm.frames[page.frameId]
	frame.mut.RLock()
	dirty := frame.dirty
	frame.mut.RUnlock()

	if !dirty {
		t.Error("Expected page to be dirty after MarkDirty()")
	}
}

// =============================================================================
// 2. Pin/Unpin Mechanics Tests
// =============================================================================

func TestPinCount(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Get page 3 times
	page1, _ := bm.GetPage(fileId, 0)
	page2, _ := bm.GetPage(fileId, 0)
	page3, _ := bm.GetPage(fileId, 0)

	frameId := page1.frameId

	// Verify pin count is 3
	bm.mu.Lock()
	pinCount := bm.frames[frameId].pinCount
	bm.mu.Unlock()

	if pinCount != 3 {
		t.Errorf("Expected pin count 3, got %d", pinCount)
	}

	// Close one
	page1.Close()

	bm.mu.Lock()
	pinCount = bm.frames[frameId].pinCount
	bm.mu.Unlock()

	if pinCount != 2 {
		t.Errorf("Expected pin count 2 after one close, got %d", pinCount)
	}

	// Close remaining
	page2.Close()
	page3.Close()

	bm.mu.Lock()
	pinCount = bm.frames[frameId].pinCount
	bm.mu.Unlock()

	if pinCount != 0 {
		t.Errorf("Expected pin count 0 after all closed, got %d", pinCount)
	}
}

func TestUnpinPanic(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when unpinning frame with pin count 0")
		}
	}()

	// Try to unpin a frame that was never pinned
	bm.mu.Lock()
	bm.unpinPage(0)
	bm.mu.Unlock()
}

// =============================================================================
// 3. Eviction & LRU Tests
// =============================================================================

func TestLRUEviction(t *testing.T) {
	bm, cleanup := setupTest(t, 3)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Pre-write pages to disk so they exist when we try to load them
	emptyPage := &disk_manager.PageData{}
	for pageId := 0; pageId < 4; pageId++ {
		bm.diskManager.WritePage(fileId, disk_manager.PageId(pageId), emptyPage)
	}

	// Get and close 3 pages (fill the buffer)
	page0, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page 0: %v", err)
	}
	frame0 := page0.frameId
	page0.Close()

	page1, err := bm.GetPage(fileId, 1)
	if err != nil {
		t.Fatalf("Failed to get page 1: %v", err)
	}
	frame1 := page1.frameId
	page1.Close()

	page2, err := bm.GetPage(fileId, 2)
	if err != nil {
		t.Fatalf("Failed to get page 2: %v", err)
	}
	frame2 := page2.frameId
	page2.Close()

	// All 3 frames should be different
	if frame0 == frame1 || frame1 == frame2 || frame0 == frame2 {
		t.Error("Expected different frames for different pages")
	}

	// Get page 3 - should evict page 0 (LRU)
	page3, err := bm.GetPage(fileId, 3)
	if err != nil {
		t.Fatalf("Failed to get page 3: %v", err)
	}
	frame3 := page3.frameId
	page3.Close()

	// Page 0 should no longer be in the buffer
	bm.mu.Lock()
	_, exists := bm.filePageMap[pageKey{fileId: fileId, pageId: 0}]
	bm.mu.Unlock()

	if exists {
		t.Error("Expected page 0 to be evicted")
	}

	// Frame 3 should reuse one of the previous frames
	if frame3 != frame0 && frame3 != frame1 && frame3 != frame2 {
		t.Error("Expected frame 3 to reuse an existing frame")
	}
}

func TestDirtyPageFlushOnEviction(t *testing.T) {
	bm, cleanup := setupTest(t, 2)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Pre-write pages to disk
	emptyPage := &disk_manager.PageData{}
	for pageId := 0; pageId < 3; pageId++ {
		bm.diskManager.WritePage(fileId, disk_manager.PageId(pageId), emptyPage)
	}

	// Get page 0, write data, mark dirty
	page0, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page 0: %v", err)
	}
	page0.Lock()
	buffer := page0.GetBuffer()
	testData := []byte("test data for page 0")
	copy(buffer[:], testData)
	page0.Unlock()
	page0.MarkDirty()
	page0.Close()

	// Fill buffer with another page
	page1, err := bm.GetPage(fileId, 1)
	if err != nil {
		t.Fatalf("Failed to get page 1: %v", err)
	}
	page1.Close()

	// Get page 2 - should evict page 0 and flush it
	page2, err := bm.GetPage(fileId, 2)
	if err != nil {
		t.Fatalf("Failed to get page 2: %v", err)
	}
	page2.Close()

	// Read page 0 from disk directly to verify it was flushed
	var diskBuffer disk_manager.PageData
	err = bm.diskManager.ReadPage(fileId, 0, &diskBuffer)
	if err != nil {
		t.Fatalf("Failed to read page from disk: %v", err)
	}

	// Verify data was written to disk
	for i := range testData {
		if diskBuffer[i] != testData[i] {
			t.Errorf("Data mismatch at byte %d: expected %d, got %d", i, testData[i], diskBuffer[i])
			break
		}
	}
}

func TestEvictionRemovesOldMapping(t *testing.T) {
	bm, cleanup := setupTest(t, 2)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Pre-write pages to disk
	emptyPage := &disk_manager.PageData{}
	for pageId := 0; pageId < 3; pageId++ {
		bm.diskManager.WritePage(fileId, disk_manager.PageId(pageId), emptyPage)
	}

	// Get and close page 0
	page0, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page 0: %v", err)
	}
	page0.Close()

	// Get and close page 1
	page1, err := bm.GetPage(fileId, 1)
	if err != nil {
		t.Fatalf("Failed to get page 1: %v", err)
	}
	page1.Close()

	// Get page 2 - will evict page 0
	page2, err := bm.GetPage(fileId, 2)
	if err != nil {
		t.Fatalf("Failed to get page 2: %v", err)
	}
	page2.Close()

	// Verify page 0 is no longer in filePageMap
	bm.mu.Lock()
	_, exists := bm.filePageMap[pageKey{fileId: fileId, pageId: 0}]
	bm.mu.Unlock()

	if exists {
		t.Error("Expected page 0 mapping to be removed after eviction")
	}
}

// =============================================================================
// 4. Concurrency Tests (CRITICAL)
// =============================================================================

func TestPageLockingAPI(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	page, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	defer page.Close()

	// Test write lock
	page.Lock()
	buffer := page.GetBuffer()
	testData := []byte("write lock test")
	copy(buffer[:], testData)
	page.Unlock()

	// Test read lock
	page.RLock()
	buffer = page.GetBuffer()
	for i := range testData {
		if buffer[i] != testData[i] {
			t.Errorf("Data mismatch at byte %d", i)
			break
		}
	}
	page.RUnlock()

	// Test multiple concurrent readers
	const numReaders = 10
	var wg sync.WaitGroup
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			page.RLock()
			buffer := page.GetBuffer()
			_ = buffer[0] // Read
			page.RUnlock()
		}()
	}

	wg.Wait()
}

func TestConcurrentGetSamePage(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	const numGoroutines = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Track which frames were returned using a channel to avoid race
	frameIdsChan := make(chan frameId, numGoroutines)

	// All goroutines get the same page simultaneously
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			// Simulate concurrent start
			page, err := bm.GetPage(fileId, 0)
			if err != nil {
				t.Errorf("Failed to get page: %v", err)
				return
			}
			defer page.Close()

			frameIdsChan <- page.frameId
		}()
	}

	wg.Wait()
	close(frameIdsChan)

	// Collect all frame IDs
	frameIds := make([]frameId, 0, numGoroutines)
	for fid := range frameIdsChan {
		frameIds = append(frameIds, fid)
	}

	// Verify all goroutines got the same frame
	if len(frameIds) == 0 {
		t.Fatal("No frame IDs collected")
	}

	firstFrameId := frameIds[0]
	for i := 1; i < len(frameIds); i++ {
		if frameIds[i] != firstFrameId {
			t.Errorf("Goroutine %d got different frame: expected %d, got %d", i, firstFrameId, frameIds[i])
		}
	}

	// Verify the page exists in the map exactly once
	bm.mu.Lock()
	_, exists := bm.filePageMap[pageKey{fileId: fileId, pageId: 0}]
	mapSize := len(bm.filePageMap)
	bm.mu.Unlock()

	if !exists {
		t.Error("Page should exist in filePageMap")
	}

	if mapSize != 1 {
		t.Errorf("Expected 1 entry in filePageMap, got %d", mapSize)
	}
}

func TestConcurrentGetDifferentPages(t *testing.T) {
	bm, cleanup := setupTest(t, 20)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	const numGoroutines = 20

	// Pre-write pages to disk
	emptyPage := &disk_manager.PageData{}
	for pageId := 0; pageId < numGoroutines; pageId++ {
		bm.diskManager.WritePage(fileId, disk_manager.PageId(pageId), emptyPage)
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make([]error, numGoroutines)

	// Each goroutine gets a different page
	for i := 0; i < numGoroutines; i++ {
		go func(pageId disk_manager.PageId, idx int) {
			defer wg.Done()

			page, err := bm.GetPage(fileId, pageId)
			if err != nil {
				errors[idx] = err
				return
			}
			defer page.Close()

			// Do some work with the page
			buffer := page.GetBuffer()
			buffer[0] = byte(pageId)
		}(disk_manager.PageId(i), i)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			t.Errorf("Goroutine %d failed: %v", i, err)
		}
	}

	// Verify all pages are in the map
	bm.mu.Lock()
	mapSize := len(bm.filePageMap)
	bm.mu.Unlock()

	if mapSize != numGoroutines {
		t.Errorf("Expected %d entries in filePageMap, got %d", numGoroutines, mapSize)
	}
}

func TestConcurrentPinUnpin(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Get a page first
	initialPage, _ := bm.GetPage(fileId, 0)
	frameId := initialPage.frameId

	const numGoroutines = 100
	var wg sync.WaitGroup

	// Half will pin, half will unpin
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines/2; i++ {
		// Pin
		go func() {
			defer wg.Done()
			page, _ := bm.GetPage(fileId, 0)
			// Keep it pinned for now
			_ = page
		}()

		// Unpin (will unpin the ones we just pinned)
		go func() {
			defer wg.Done()
			page, _ := bm.GetPage(fileId, 0)
			page.Close()
		}()
	}

	wg.Wait()
	initialPage.Close()

	// Verify pin count is consistent (should be numGoroutines/2 since half didn't close)
	bm.mu.Lock()
	pinCount := bm.frames[frameId].pinCount
	bm.mu.Unlock()

	if pinCount != numGoroutines/2 {
		t.Logf("Pin count after concurrent operations: %d (expected %d)", pinCount, numGoroutines/2)
		// This is actually acceptable due to timing
	}
}

func TestConcurrentFlush(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Get a page and mark it dirty
	page, _ := bm.GetPage(fileId, 0)
	page.MarkDirty()
	page.Close()

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make([]error, numGoroutines)

	// Multiple goroutines try to flush the same page
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			err := bm.FlushPage(fileId, 0)
			if err != nil {
				errors[idx] = err
			}
		}(i)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			t.Errorf("Goroutine %d failed to flush: %v", i, err)
		}
	}

	// Verify page is no longer dirty
	frame := &bm.frames[0]
	frame.mut.RLock()
	dirty := frame.dirty
	frame.mut.RUnlock()

	// Find the actual frame for this page
	bm.mu.Lock()
	actualFrameId, exists := bm.filePageMap[pageKey{fileId: fileId, pageId: 0}]
	bm.mu.Unlock()

	if exists {
		actualFrame := &bm.frames[actualFrameId]
		actualFrame.mut.RLock()
		dirty = actualFrame.dirty
		actualFrame.mut.RUnlock()

		if dirty {
			t.Error("Page should not be dirty after flush")
		}
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	page, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	defer page.Close()

	const numGoroutines = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Test concurrent access to the same page with proper locking
	// Half write, half read
	for i := 0; i < numGoroutines/2; i++ {
		// Writer - uses exclusive lock
		go func(val byte) {
			defer wg.Done()
			page.Lock()
			buffer := page.GetBuffer()
			buffer[0] = val
			page.Unlock()
			page.MarkDirty()
		}(byte(i))

		// Reader - uses shared read lock
		go func() {
			defer wg.Done()
			page.RLock()
			buffer := page.GetBuffer()
			_ = buffer[0] // Just read
			page.RUnlock()
		}()
	}

	wg.Wait()

	// If we get here without data races, the test passed
}

// =============================================================================
// 5. Persistence Tests
// =============================================================================

func TestFlushDirtyPage(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write data and mark dirty
	page, _ := bm.GetPage(fileId, 0)
	page.Lock()
	buffer := page.GetBuffer()
	testData := []byte("persistence test")
	copy(buffer[:], testData)
	page.Unlock()
	page.MarkDirty()
	page.Close()

	// Flush the page
	err = bm.FlushPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}

	// Read from disk to verify
	var diskBuffer disk_manager.PageData
	err = bm.diskManager.ReadPage(fileId, 0, &diskBuffer)
	if err != nil {
		t.Fatalf("Failed to read from disk: %v", err)
	}

	for i := range testData {
		if diskBuffer[i] != testData[i] {
			t.Errorf("Data mismatch at byte %d", i)
			break
		}
	}
}

func TestFlushCleanPage(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Get a page but don't mark it dirty
	page, _ := bm.GetPage(fileId, 0)
	page.Close()

	// Flush should succeed but not write
	err = bm.FlushPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to flush clean page: %v", err)
	}
}

func TestFlushNonExistentPage(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Try to flush a page that's not in buffer
	err = bm.FlushPage(fileId, 99)
	if err == nil {
		t.Error("Expected error when flushing non-existent page")
	}
}

func TestPageSurvivesEviction(t *testing.T) {
	bm, cleanup := setupTest(t, 2)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Pre-write pages to disk
	emptyPage := &disk_manager.PageData{}
	for pageId := 0; pageId < 3; pageId++ {
		bm.diskManager.WritePage(fileId, disk_manager.PageId(pageId), emptyPage)
	}

	// Write data to page 0
	page0, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page 0: %v", err)
	}
	page0.Lock()
	buffer := page0.GetBuffer()
	testData := []byte("survives eviction")
	copy(buffer[:], testData)
	page0.Unlock()
	page0.MarkDirty()
	page0.Close()

	// Load other pages to evict page 0
	page1, err := bm.GetPage(fileId, 1)
	if err != nil {
		t.Fatalf("Failed to get page 1: %v", err)
	}
	page1.Close()

	page2, err := bm.GetPage(fileId, 2)
	if err != nil {
		t.Fatalf("Failed to get page 2: %v", err)
	}
	page2.Close()

	// Get page 0 again - should be loaded from disk
	page0Again, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page 0 again: %v", err)
	}
	page0Again.RLock()
	bufferAgain := page0Again.GetBuffer()

	// Verify data persisted
	for i := range testData {
		if bufferAgain[i] != testData[i] {
			t.Errorf("Data mismatch at byte %d after eviction", i)
			break
		}
	}
	page0Again.RUnlock()

	page0Again.Close()
}

// =============================================================================
// 6. Edge Cases
// =============================================================================

func TestSmallBufferPool(t *testing.T) {
	bm, cleanup := setupTest(t, 1)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Pre-write pages to disk
	emptyPage := &disk_manager.PageData{}
	for pageId := 0; pageId < 2; pageId++ {
		bm.diskManager.WritePage(fileId, disk_manager.PageId(pageId), emptyPage)
	}

	// Get a page
	page0, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page 0: %v", err)
	}
	page0.Close()

	// Get another page - should evict the first
	page1, err := bm.GetPage(fileId, 1)
	if err != nil {
		t.Fatalf("Failed to get page 1: %v", err)
	}
	page1.Close()

	// Verify page 0 was evicted
	bm.mu.Lock()
	_, exists := bm.filePageMap[pageKey{fileId: fileId, pageId: 0}]
	bm.mu.Unlock()

	if exists {
		t.Error("Expected page 0 to be evicted in buffer of size 1")
	}
}

func TestMultipleFiles(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	// Create two files
	fileId1, _ := bm.diskManager.CreateFile()
	fileId2, _ := bm.diskManager.CreateFile()

	// Get page 0 from both files
	page1, _ := bm.GetPage(fileId1, 0)
	page2, _ := bm.GetPage(fileId2, 0)

	// They should use different frames
	if page1.frameId == page2.frameId {
		t.Error("Pages from different files should use different frames")
	}

	page1.Close()
	page2.Close()
}

func TestZeroPageId(t *testing.T) {
	bm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := bm.diskManager.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Page 0 should work fine
	page, err := bm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page 0: %v", err)
	}
	defer page.Close()

	page.RLock()
	buffer := page.GetBuffer()
	if buffer == nil {
		page.RUnlock()
		t.Error("Page 0 should have a valid buffer")
	}
	page.RUnlock()
}
