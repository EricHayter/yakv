package disk_manager

import (
	"os"
	"testing"
	"bytes"
)

func setupTest(t *testing.T) (*DiskManager, func()) {
	// Clean up yakv directory before test
	os.RemoveAll(yakvDirectory)

	dm, err := New()
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}

	cleanup := func() {
		dm.Close()
		os.RemoveAll(yakvDirectory)
	}

	return dm, cleanup
}

// Constructor Tests

func TestNew(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	if dm == nil {
		t.Fatal("Expected non-nil DiskManager")
	}

	if dm.fileHandleMap == nil {
		t.Error("Expected fileHandleMap to be initialized")
	}

	if dm.fileCache == nil {
		t.Error("Expected fileCache to be initialized")
	}
}

func TestNew_CreatesDirectory(t *testing.T) {
	// Ensure directory doesn't exist
	os.RemoveAll(yakvDirectory)

	dm, err := New()
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer func() {
		dm.Close()
		os.RemoveAll(yakvDirectory)
	}()

	// Check directory exists
	info, err := os.Stat(yakvDirectory)
	if err != nil {
		t.Fatalf("Directory was not created: %v", err)
	}

	if !info.IsDir() {
		t.Error("Expected yakv path to be a directory")
	}
}

// File Creation Tests

func TestCreateFileWithId_Success(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId := FileId(100)
	err := dm.CreateFileWithId(fileId)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Verify file exists on disk
	filePath := getFilePath(fileId)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("File was not created: %v", err)
	}

	// Check file size is at least one page
	if info.Size() != PageSize {
		t.Errorf("Expected file size %d, got %d", PageSize, info.Size())
	}
}

func TestCreateFileWithId_AlreadyExists(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId := FileId(200)

	// Create file first time
	err := dm.CreateFileWithId(fileId)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Try to create again
	err = dm.CreateFileWithId(fileId)
	if err != os.ErrExist {
		t.Errorf("Expected os.ErrExist, got %v", err)
	}
}

func TestCreateFile_Success(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId, err := dm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Verify file exists
	filePath := getFilePath(fileId)
	_, err = os.Stat(filePath)
	if err != nil {
		t.Fatalf("File was not created: %v", err)
	}
}

func TestCreateFile_ReturnsUniqueIds(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId1, err := dm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create first file: %v", err)
	}

	fileId2, err := dm.CreateFile()
	if err != nil {
		t.Fatalf("Failed to create second file: %v", err)
	}

	if fileId1 == fileId2 {
		t.Errorf("Expected unique file IDs, got %d for both", fileId1)
	}
}

// Read/Write Tests

func TestWriteAndReadPage(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId := FileId(300)
	err := dm.CreateFileWithId(fileId)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Create test data
	writeData := &PageData{}
	for i := 0; i < PageSize; i++ {
		writeData[i] = byte(i % 256)
	}

	// Write page
	err = dm.WritePage(fileId, PageId(0), writeData)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Read page back
	readData := &PageData{}
	err = dm.ReadPage(fileId, PageId(0), readData)
	if err != nil {
		t.Fatalf("Failed to read page: %v", err)
	}

	// Compare data
	if !bytes.Equal(writeData[:], readData[:]) {
		t.Error("Read data doesn't match written data")
	}
}

func TestWritePage_ToNonExistentFile(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId := FileId(999)
	data := &PageData{}

	err := dm.WritePage(fileId, PageId(0), data)
	if err == nil {
		t.Error("Expected error when writing to non-existent file")
	}
}

func TestReadPage_FromNonExistentFile(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId := FileId(999)
	buffer := &PageData{}

	err := dm.ReadPage(fileId, PageId(0), buffer)
	if err == nil {
		t.Error("Expected error when reading from non-existent file")
	}
}

func TestWritePage_MultiplePages(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId := FileId(400)
	err := dm.CreateFileWithId(fileId)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write to pages 0, 1, 2
	for pageId := PageId(0); pageId < 3; pageId++ {
		data := &PageData{}
		// Fill with page-specific pattern
		for i := 0; i < PageSize; i++ {
			data[i] = byte(pageId)
		}

		err = dm.WritePage(fileId, pageId, data)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", pageId, err)
		}
	}

	// Read back and verify
	for pageId := PageId(0); pageId < 3; pageId++ {
		data := &PageData{}
		err = dm.ReadPage(fileId, pageId, data)
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", pageId, err)
		}

		// Verify pattern
		expected := byte(pageId)
		for i := 0; i < PageSize; i++ {
			if data[i] != expected {
				t.Errorf("Page %d: expected byte %d at offset %d, got %d",
					pageId, expected, i, data[i])
				break
			}
		}
	}
}

func TestReadPage_MultiplePages(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId := FileId(500)
	err := dm.CreateFileWithId(fileId)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write different patterns to different pages
	patterns := []byte{0xAA, 0xBB, 0xCC}
	for i, pattern := range patterns {
		data := &PageData{}
		for j := 0; j < PageSize; j++ {
			data[j] = pattern
		}

		err = dm.WritePage(fileId, PageId(i), data)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	// Read pages in different order
	readOrder := []PageId{2, 0, 1}
	for _, pageId := range readOrder {
		data := &PageData{}
		err = dm.ReadPage(fileId, pageId, data)
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", pageId, err)
		}

		expected := patterns[pageId]
		if data[0] != expected {
			t.Errorf("Page %d: expected pattern %x, got %x", pageId, expected, data[0])
		}
	}
}

func TestWriteAndReadPage_MultipleFiles(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileIds := []FileId{600, 601, 602}

	// Create files and write unique data to each
	for _, fileId := range fileIds {
		err := dm.CreateFileWithId(fileId)
		if err != nil {
			t.Fatalf("Failed to create file %d: %v", fileId, err)
		}

		data := &PageData{}
		// Fill with file-specific pattern
		for i := 0; i < PageSize; i++ {
			data[i] = byte(fileId)
		}

		err = dm.WritePage(fileId, PageId(0), data)
		if err != nil {
			t.Fatalf("Failed to write to file %d: %v", fileId, err)
		}
	}

	// Read back and verify each file has correct data
	for _, fileId := range fileIds {
		data := &PageData{}
		err := dm.ReadPage(fileId, PageId(0), data)
		if err != nil {
			t.Fatalf("Failed to read from file %d: %v", fileId, err)
		}

		expected := byte(fileId)
		if data[0] != expected {
			t.Errorf("File %d: expected pattern %d, got %d", fileId, expected, data[0])
		}
	}
}

// Persistence Tests

func TestPersistence_DataSurvivesClose(t *testing.T) {
	// Clean up before test
	os.RemoveAll(yakvDirectory)
	defer os.RemoveAll(yakvDirectory)

	fileId := FileId(700)
	testData := &PageData{}
	for i := 0; i < PageSize; i++ {
		testData[i] = byte(i % 256)
	}

	// Create first disk manager, write data, close
	{
		dm, err := New()
		if err != nil {
			t.Fatalf("Failed to create first DiskManager: %v", err)
		}

		err = dm.CreateFileWithId(fileId)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		err = dm.WritePage(fileId, PageId(0), testData)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}

		dm.Close()
	}

	// Create new disk manager and read data back
	{
		dm, err := New()
		if err != nil {
			t.Fatalf("Failed to create second DiskManager: %v", err)
		}
		defer dm.Close()

		readData := &PageData{}
		err = dm.ReadPage(fileId, PageId(0), readData)
		if err != nil {
			t.Fatalf("Failed to read page: %v", err)
		}

		if !bytes.Equal(testData[:], readData[:]) {
			t.Error("Data did not survive close/reopen")
		}
	}
}

func TestPersistence_FileExistsAfterClose(t *testing.T) {
	// Clean up before and after test
	os.RemoveAll(yakvDirectory)
	defer os.RemoveAll(yakvDirectory)

	fileId := FileId(800)

	dm, err := New()
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}

	err = dm.CreateFileWithId(fileId)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	filePath := getFilePath(fileId)
	dm.Close()

	// Check file still exists
	_, err = os.Stat(filePath)
	if err != nil {
		t.Errorf("File does not exist after Close(): %v", err)
	}
}

// Close Tests

func TestClose_ClosesAllHandles(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	// Create and open several files
	fileIds := []FileId{900, 901, 902}
	for _, fileId := range fileIds {
		err := dm.CreateFileWithId(fileId)
		if err != nil {
			t.Fatalf("Failed to create file %d: %v", fileId, err)
		}

		// Force file to be opened by reading from it
		buffer := &PageData{}
		dm.ReadPage(fileId, PageId(0), buffer)
	}

	// Close should succeed without errors
	err := dm.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func TestClose_ClearsInternalState(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	// Create a file
	fileId := FileId(1000)
	err := dm.CreateFileWithId(fileId)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Force file to be opened
	buffer := &PageData{}
	dm.ReadPage(fileId, PageId(0), buffer)

	// Close
	dm.Close()

	// Check internal state is cleared
	if len(dm.fileHandleMap) != 0 {
		t.Errorf("Expected fileHandleMap to be empty after Close, got %d entries",
			len(dm.fileHandleMap))
	}
}

// Edge Cases

func TestWritePage_ExtendsFile(t *testing.T) {
	dm, cleanup := setupTest(t)
	defer cleanup()

	fileId := FileId(1100)
	err := dm.CreateFileWithId(fileId)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write to page 5 (should extend file beyond initial page)
	data := &PageData{}
	for i := 0; i < PageSize; i++ {
		data[i] = 0xFF
	}

	err = dm.WritePage(fileId, PageId(5), data)
	if err != nil {
		t.Fatalf("Failed to write to page 5: %v", err)
	}

	// Verify we can read it back
	readData := &PageData{}
	err = dm.ReadPage(fileId, PageId(5), readData)
	if err != nil {
		t.Fatalf("Failed to read page 5: %v", err)
	}

	if !bytes.Equal(data[:], readData[:]) {
		t.Error("Data written to extended page doesn't match")
	}
}
