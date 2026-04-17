package storage_manager

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

// Helper function to setup test and cleanup
func setupTest(t *testing.T, capacity uint16) (*StorageManager, func()) {
	sm, err := New(capacity)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}

	cleanup := func() {
		sm.Close()
		os.RemoveAll("yakv")
	}

	return sm, cleanup
}

func TestNew(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	if sm == nil {
		t.Fatal("Expected non-nil storage manager")
	}

	if sm.bufferManager == nil {
		t.Fatal("Expected non-nil buffer manager")
	}

	if sm.diskManager == nil {
		t.Fatal("Expected non-nil disk manager")
	}
}

func TestCreateFile(t *testing.T) {
	sm, err := New(10)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer sm.Close()

	fileId, err := sm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Verify we can get a page from the file
	page, err := sm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page from created file: %v", err)
	}
	page.Close()
}

func TestCreateFileWithId(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	// Use a random file ID to avoid collisions
	fileId := FileId(50000 + (os.Getpid() % 1000))

	// Clean up if it exists from previous run
	filePath := filepath.Join("yakv", strconv.FormatUint(uint64(fileId), 10))
	os.Remove(filePath)

	err := sm.CreateFileWithId(fileId)
	if err != nil {
		t.Fatalf("Failed to create file with ID: %v", err)
	}

	// Try to create again - should fail
	err = sm.CreateFileWithId(fileId)
	if err != os.ErrExist {
		t.Errorf("Expected os.ErrExist, got: %v", err)
	}
}

func TestGetPageAndClose(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := sm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	page, err := sm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	if page == nil {
		t.Fatal("Expected non-nil page")
	}

	if page.GetFileId() != fileId {
		t.Errorf("Expected fileId %d, got %d", fileId, page.GetFileId())
	}

	if page.GetPageId() != 0 {
		t.Errorf("Expected pageId 0, got %d", page.GetPageId())
	}

	page.Close()
}

func TestPageReadWrite(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := sm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write data
	page, err := sm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	testData := []byte("Hello, Storage Manager!")
	page.Lock()
	buffer := page.GetBuffer()
	copy(buffer[:], testData)
	page.Unlock()
	page.MarkDirty()
	page.Close()

	// Flush to ensure it's written
	err = sm.FlushPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}

	// Read data back
	page2, err := sm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	defer page2.Close()

	page2.RLock()
	readBuffer := page2.GetBuffer()
	if !bytes.Equal(readBuffer[:len(testData)], testData) {
		t.Errorf("Expected %s, got %s", testData, readBuffer[:len(testData)])
	}
	page2.RUnlock()
}

func TestConcurrentReads(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := sm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write test data
	page, err := sm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	testData := []byte("Concurrent test data")
	page.Lock()
	buffer := page.GetBuffer()
	copy(buffer[:], testData)
	page.Unlock()
	page.MarkDirty()
	page.Close()

	sm.FlushPage(fileId, 0)

	// Concurrent reads
	var wg sync.WaitGroup
	numReaders := 10

	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()

			page, err := sm.GetPage(fileId, 0)
			if err != nil {
				t.Errorf("Failed to get page: %v", err)
				return
			}
			defer page.Close()

			page.RLock()
			readBuffer := page.GetBuffer()
			if !bytes.Equal(readBuffer[:len(testData)], testData) {
				t.Errorf("Expected %s, got %s", testData, readBuffer[:len(testData)])
			}
			page.RUnlock()
		}()
	}

	wg.Wait()
}

func TestMultiplePages(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := sm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write to multiple pages
	numPages := 5
	for i := range numPages {
		// Use WritePageDirect to expand file size first
		var pageData PageData
		testData := []byte{byte(i), byte(i + 1), byte(i + 2)}
		copy(pageData[:], testData)
		err = sm.WritePageDirect(fileId, PageId(i), &pageData)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	// Now read via buffer manager
	for i := range numPages {
		page, err := sm.GetPage(fileId, PageId(i))
		if err != nil {
			t.Fatalf("Failed to get page %d: %v", i, err)
		}

		expected := []byte{byte(i), byte(i + 1), byte(i + 2)}
		page.RLock()
		buffer := page.GetBuffer()
		copy(buffer[:], expected) // Write via buffer manager too
		page.RUnlock()
		page.MarkDirty()
		page.Close()
	}

	// Read back and verify
	for i := range numPages {
		page, err := sm.GetPage(fileId, PageId(i))
		if err != nil {
			t.Fatalf("Failed to get page %d: %v", i, err)
		}

		expected := []byte{byte(i), byte(i + 1), byte(i + 2)}
		page.RLock()
		buffer := page.GetBuffer()
		if !bytes.Equal(buffer[:len(expected)], expected) {
			t.Errorf("Page %d: expected %v, got %v", i, expected, buffer[:len(expected)])
		}
		page.RUnlock()
		page.Close()
	}
}

func TestDirectIO(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := sm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write directly
	var writeData PageData
	testData := []byte("Direct I/O test")
	copy(writeData[:], testData)

	err = sm.WritePageDirect(fileId, 0, &writeData)
	if err != nil {
		t.Fatalf("Failed to write page directly: %v", err)
	}

	// Read directly
	var readData PageData
	err = sm.ReadPageDirect(fileId, 0, &readData)
	if err != nil {
		t.Fatalf("Failed to read page directly: %v", err)
	}

	if !bytes.Equal(readData[:len(testData)], testData) {
		t.Errorf("Expected %s, got %s", testData, readData[:len(testData)])
	}
}

func TestGetDiskManager(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	dm := sm.GetDiskManager()
	if dm == nil {
		t.Fatal("Expected non-nil disk manager")
	}

	// Verify we can use it directly
	fileId, err := dm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file via disk manager: %v", err)
	}

	// Write and read using disk manager
	var writeData PageData
	copy(writeData[:], []byte("Disk manager direct access"))

	err = dm.WritePage(fileId, 0, &writeData)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	var readData PageData
	err = dm.ReadPage(fileId, 0, &readData)
	if err != nil {
		t.Fatalf("Failed to read page: %v", err)
	}

	if !bytes.Equal(readData[:26], writeData[:26]) {
		t.Error("Data mismatch after direct disk manager access")
	}
}

func TestPagePinning(t *testing.T) {
	sm, cleanup := setupTest(t, 2) // Small buffer to test eviction
	defer cleanup()

	fileId, err := sm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Expand file to have 3 pages first
	for i := range 3 {
		var pageData PageData
		err = sm.WritePageDirect(fileId, PageId(i), &pageData)
		if err != nil {
			t.Fatalf("Failed to expand file: %v", err)
		}
	}

	// Get page 0 and keep it pinned
	page0, err := sm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page 0: %v", err)
	}
	// Don't close it yet - keep it pinned

	// Write to page 0
	page0.Lock()
	buffer0 := page0.GetBuffer()
	copy(buffer0[:], []byte("Page 0 data"))
	page0.Unlock()
	page0.MarkDirty()

	// Get pages 1 and 2 (should cause eviction attempts)
	page1, err := sm.GetPage(fileId, 1)
	if err != nil {
		t.Fatalf("Failed to get page 1: %v", err)
	}

	page1.Lock()
	buffer1 := page1.GetBuffer()
	copy(buffer1[:], []byte("Page 1 data"))
	page1.Unlock()
	page1.MarkDirty()
	page1.Close()

	page2, err := sm.GetPage(fileId, 2)
	if err != nil {
		t.Fatalf("Failed to get page 2: %v", err)
	}

	page2.Lock()
	buffer2 := page2.GetBuffer()
	copy(buffer2[:], []byte("Page 2 data"))
	page2.Unlock()
	page2.MarkDirty()
	page2.Close()

	// Page 0 should still be accessible (was pinned)
	page0.RLock()
	if !bytes.Equal(buffer0[:11], []byte("Page 0 data")) {
		t.Error("Page 0 data was lost despite being pinned")
	}
	page0.RUnlock()

	// Now unpin page 0
	page0.Close()
}

func TestFlushPage(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	fileId, err := sm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write data and mark dirty
	page, err := sm.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	testData := []byte("Flush test")
	page.Lock()
	buffer := page.GetBuffer()
	copy(buffer[:], testData)
	page.Unlock()
	page.MarkDirty()
	page.Close()

	// Explicit flush
	err = sm.FlushPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}

	// Close and reopen storage manager to verify persistence
	sm.Close()

	sm2, err := New(10)
	if err != nil {
		t.Fatalf("Failed to create new storage manager: %v", err)
	}
	defer sm2.Close()

	// Read back
	page2, err := sm2.GetPage(fileId, 0)
	if err != nil {
		t.Fatalf("Failed to get page after reopen: %v", err)
	}
	defer page2.Close()

	page2.RLock()
	readBuffer := page2.GetBuffer()
	if !bytes.Equal(readBuffer[:len(testData)], testData) {
		t.Errorf("Expected %s, got %s", testData, readBuffer[:len(testData)])
	}
	page2.RUnlock()
}

func TestMultipleFiles(t *testing.T) {
	sm, cleanup := setupTest(t, 10)
	defer cleanup()

	numFiles := 5
	fileIds := make([]FileId, numFiles)

	// Create multiple files
	for i := range numFiles {
		fileId, err := sm.CreateFile()
		if err != nil {
			t.Fatalf("Failed to create file %d: %v", i, err)
		}
		fileIds[i] = fileId

		// Write unique data to each file
		page, err := sm.GetPage(fileId, 0)
		if err != nil {
			t.Fatalf("Failed to get page for file %d: %v", i, err)
		}

		testData := []byte{byte(i * 10), byte(i*10 + 1), byte(i*10 + 2)}
		page.Lock()
		buffer := page.GetBuffer()
		copy(buffer[:], testData)
		page.Unlock()
		page.MarkDirty()
		page.Close()
	}

	// Verify each file has correct data
	for i := range numFiles {
		page, err := sm.GetPage(fileIds[i], 0)
		if err != nil {
			t.Fatalf("Failed to get page for file %d: %v", i, err)
		}

		expected := []byte{byte(i * 10), byte(i*10 + 1), byte(i*10 + 2)}
		page.RLock()
		buffer := page.GetBuffer()
		if !bytes.Equal(buffer[:len(expected)], expected) {
			t.Errorf("File %d: expected %v, got %v", i, expected, buffer[:len(expected)])
		}
		page.RUnlock()
		page.Close()
	}
}

func BenchmarkGetPage(b *testing.B) {
	sm, err := New(100)
	if err != nil {
		b.Fatalf("Failed to create storage manager: %v", err)
	}
	defer func() {
		sm.Close()
		os.RemoveAll("yakv")
	}()

	fileId, err := sm.CreateFile()
	if err != nil {
		b.Fatalf("Failed to create file: %v", err)
	}

	b.ResetTimer()
	for b.Loop() {
		page, err := sm.GetPage(fileId, 0)
		if err != nil {
			b.Fatalf("Failed to get page: %v", err)
		}
		page.Close()
	}
}

func BenchmarkPageReadWrite(b *testing.B) {
	sm, err := New(100)
	if err != nil {
		b.Fatalf("Failed to create storage manager: %v", err)
	}
	defer func() {
		sm.Close()
		os.RemoveAll("yakv")
	}()

	fileId, err := sm.CreateFile()
	if err != nil {
		b.Fatalf("Failed to create file: %v", err)
	}

	testData := []byte("Benchmark data")

	b.ResetTimer()
	for b.Loop() {
		page, err := sm.GetPage(fileId, 0)
		if err != nil {
			b.Fatalf("Failed to get page: %v", err)
		}

		page.Lock()
		buffer := page.GetBuffer()
		copy(buffer[:], testData)
		page.Unlock()
		page.MarkDirty()
		page.Close()
	}
}
