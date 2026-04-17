package disk_manager

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"

	"github.com/EricHayter/yakv/internal/lru"
	"github.com/EricHayter/yakv/server/common"
)

const (
	PageSize = 1 << 12  // 4096 bytes
	MaxFileHandles = 256
)

type FileId uint16
type PageId uint16
type PageData [PageSize]byte

// Maximum file size is 65535 * 4096 = ~256MB per file
// (PageId is uint16, limiting pages per file to 65536)

type DiskManager struct {
	mu              sync.RWMutex // Protects fileHandleMap and fileReplacer
	fileHandleMap   map[FileId]*os.File
	fileReplacer    *lru.Replacer[FileId]
}

func New() (*DiskManager, error) {
	// Create yakv directory if it doesn't exist
	err := os.MkdirAll(common.YakvDirectory, 0755)
	if err != nil {
		return nil, fmt.Errorf("Failed to create yakv directory: %v", err)
	}

	return &DiskManager{
		fileHandleMap: make(map[FileId]*os.File),
		fileReplacer:  lru.New[FileId](),
	}, nil
}

func (diskManager *DiskManager) CreateFileWithId(fileId FileId) error {
	filePath := getFilePath(fileId)

	exists, err := fileExists(filePath)
	if err != nil {
		return fmt.Errorf("Error checking file existence for fileId=%d: %v", fileId, err)
	}
	if exists {
		return os.ErrExist
	}

	// Create and open the file with read-write permissions
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("Failed to create file: fileId=%d, error=%v", fileId, err)
	}

	// Allocate at least one page for convenience
	err = file.Truncate(PageSize)
	if err != nil {
		file.Close()
		return fmt.Errorf("Failed to truncate file: fileId=%d, error=%v", fileId, err)
	}

	// Acquire lock to modify cache
	diskManager.mu.Lock()
	defer diskManager.mu.Unlock()

	// Check if we need to evict a file handle
	if len(diskManager.fileHandleMap) >= MaxFileHandles {
		evictedFileId := diskManager.fileReplacer.Pop()
		evictedFile := diskManager.fileHandleMap[evictedFileId]
		evictedFile.Close()
		delete(diskManager.fileHandleMap, evictedFileId)
	}

	// Add to replacer and file handle map
	diskManager.fileReplacer.Push(fileId)
	diskManager.fileHandleMap[fileId] = file
	return nil
}

func (diskManager *DiskManager) CreateFile() (FileId, error) {
	const maxRetries = 10
	retryCount := 0

	// Generate random FileId in valid uint16 range
	fileId := FileId(rand.Intn(65536))
	exists, err := fileExists(getFilePath(fileId))
	if err != nil {
		return 0, fmt.Errorf("Error checking file existence during ID generation: %v", err)
	}

	for exists && retryCount < maxRetries {
		fileId = FileId(rand.Intn(65536))
		exists, err = fileExists(getFilePath(fileId))
		if err != nil {
			return 0, fmt.Errorf("Error checking file existence during ID generation: %v", err)
		}
		retryCount++
	}

	if retryCount >= maxRetries {
		return 0, fmt.Errorf("Failed to generate unique file ID after %d retries", maxRetries)
	}

	if retryCount > 0 {
		return 0, fmt.Errorf("Generated file ID after %d retries: fileId=%d", retryCount, fileId)
	}

	err = diskManager.CreateFileWithId(fileId)
	return fileId, err
}

func (diskManager *DiskManager) ReadPage(fileId FileId, pageId PageId, buffer *PageData) error {

	file, err := diskManager.loadFile(fileId)
	if err != nil {
		return fmt.Errorf("Failed to load file for reading: fileId=%d, error=%v", fileId, err)
	}

	fileOffset := pageId * PageSize
	_, err = file.ReadAt(buffer[:], int64(fileOffset))
	if err != nil {
		return fmt.Errorf("Failed to read page: fileId=%d, pageId=%d, error=%v", fileId, pageId, err)
	}
	return nil
}

func (diskManager *DiskManager) WritePage(fileId FileId, pageId PageId, data *PageData) error {
	file, err := diskManager.loadFile(fileId)
	if err != nil {
		return fmt.Errorf("Failed to load file for writing: fileId=%d, error=%v", fileId, err)
	}

	fileOffset := pageId * PageSize
	_, err = file.WriteAt(data[:], int64(fileOffset))
	if err != nil {
		return fmt.Errorf("Failed to write page: fileId=%d, pageId=%d, error=%v", fileId, pageId, err)
	}
	return nil
}

func (diskManager *DiskManager) loadFile(fileId FileId) (*os.File, error) {
	// Fast path: check cache with read lock
	diskManager.mu.RLock()
	file, pres := diskManager.fileHandleMap[fileId]
	if pres {
		diskManager.fileReplacer.Get(fileId) // Mark as recently used
		diskManager.mu.RUnlock()
		return file, nil
	}
	diskManager.mu.RUnlock()

	// Slow path: open file and add to cache
	// Open outside of lock to avoid holding lock during I/O
	newFile, err := os.OpenFile(getFilePath(fileId), os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file: fileId=%d, error=%v", fileId, err)
	}

	// Acquire write lock to modify cache
	diskManager.mu.Lock()
	defer diskManager.mu.Unlock()

	// Double-check: another goroutine might have opened it
	if existingFile, exists := diskManager.fileHandleMap[fileId]; exists {
		newFile.Close() // Close the file we just opened
		diskManager.fileReplacer.Get(fileId)
		return existingFile, nil
	}

	// Check if we need to evict a file handle
	if len(diskManager.fileHandleMap) >= MaxFileHandles {
		evictedFileId := diskManager.fileReplacer.Pop()
		evictedFile := diskManager.fileHandleMap[evictedFileId]
		evictedFile.Close()
		delete(diskManager.fileHandleMap, evictedFileId)
	}

	// Add to replacer and file handle map
	diskManager.fileReplacer.Push(fileId)
	diskManager.fileHandleMap[fileId] = newFile
	return newFile, nil
}

// AddPage adds a single page to the file and returns the new page's ID.
func (diskManager *DiskManager) AddPage(fileId FileId) (PageId, error) {
	return diskManager.AddPages(fileId, 1)
}

// AddPages adds multiple pages to the file and returns the ID of the first new page.
func (diskManager *DiskManager) AddPages(fileId FileId, count uint16) (PageId, error) {
	if count == 0 {
		return 0, errors.New("count must be greater than 0")
	}

	file, err := diskManager.loadFile(fileId)
	if err != nil {
		return 0, fmt.Errorf("Failed to load file for adding pages: fileId=%d, error=%v", fileId, err)
	}

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("Failed to stat file: fileId=%d, error=%v", fileId, err)
	}

	currentSize := stat.Size()
	currentPageCount := PageId(currentSize / PageSize)
	newSize := currentSize + int64(count)*PageSize

	// Truncate to add new pages
	err = file.Truncate(newSize)
	if err != nil {
		return 0, fmt.Errorf("Failed to truncate file: fileId=%d, error=%v", fileId, err)
	}

	return currentPageCount, nil
}

// Close closes all open file handles
func (diskManager *DiskManager) Close() error {
	diskManager.mu.Lock()
	defer diskManager.mu.Unlock()

	for _, file := range diskManager.fileHandleMap {
		file.Close()
	}
	diskManager.fileHandleMap = make(map[FileId]*os.File)
	return nil
}
