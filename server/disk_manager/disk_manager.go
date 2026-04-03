package disk_manager

import (
	"os"
	"errors"
	"log"
	"math/rand"
	"github.com/EricHayter/yakv/internal/lru"
)

const (
	yakvDirectory = "yakv"
	PageSize = 1 << 12  // 4096 bytes
	MaxFileHandles = 256
)

type FileId uint16
type PageId uint16
type PageData [PageSize]byte

// Maximum file size is 65535 * 4096 = ~256MB per file
// (PageId is uint16, limiting pages per file to 65536)

type DiskManager struct {
	fileHandleCount uint8
	fileHandleMap   map[FileId]*os.File
	fileCache       *lru.Cache[FileId]
}

func New() (*DiskManager, error) {
	// Create yakv directory if it doesn't exist
	err := os.MkdirAll(yakvDirectory, 0755)
	if err != nil {
		log.Printf("Failed to create yakv directory: %v", err)
		return nil, err
	}

	log.Printf("DiskManager initialized: directory=%s, max_handles=%d, page_size=%d",
		yakvDirectory, MaxFileHandles, PageSize)

	return &DiskManager{
		fileHandleMap: make(map[FileId]*os.File),
		fileCache:     lru.New[FileId](MaxFileHandles),
	}, nil
}

func (diskManager *DiskManager) CreateFileWithId(fileId FileId) error {
	filePath := getFilePath(fileId)

	exists, err := fileExists(filePath)
	if err != nil {
		log.Printf("Error checking file existence for fileId=%d: %v", fileId, err)
		return err
	}
	if exists {
		log.Printf("File already exists: fileId=%d", fileId)
		return os.ErrExist
	}

	// Create and open the file with read-write permissions
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		log.Printf("Failed to create file: fileId=%d, error=%v", fileId, err)
		return err
	}

	// Allocate at least one page for convenience
	err = file.Truncate(PageSize)
	if err != nil {
		file.Close()
		log.Printf("Failed to truncate file: fileId=%d, error=%v", fileId, err)
		return err
	}

	// Add to cache so it's immediately ready for use
	removedKey, evicted := diskManager.fileCache.Put(fileId)
	if evicted {
		removedFile := diskManager.fileHandleMap[removedKey]
		removedFile.Close()
		delete(diskManager.fileHandleMap, removedKey)
		log.Printf("Evicted file handle from cache: fileId=%d", removedKey)
	}

	diskManager.fileHandleMap[fileId] = file
	log.Printf("Created file: fileId=%d, path=%s", fileId, filePath)
	return nil
}

func (diskManager *DiskManager) CreateFile() (FileId, error) {
	const maxRetries = 10
	retryCount := 0

	// Generate random FileId in valid uint16 range
	fileId := FileId(rand.Intn(65536))
	exists, err := fileExists(getFilePath(fileId))
	if err != nil {
		log.Printf("Error checking file existence during ID generation: %v", err)
		return 0, err
	}

	for exists && retryCount < maxRetries {
		fileId = FileId(rand.Intn(65536))
		exists, err = fileExists(getFilePath(fileId))
		if err != nil {
			log.Printf("Error checking file existence during ID generation: %v", err)
			return 0, err
		}
		retryCount++
	}

	if retryCount >= maxRetries {
		log.Printf("Failed to generate unique file ID after %d retries", maxRetries)
		return 0, errors.New("couldn't generate a unique file ID after 10 retries")
	}

	if retryCount > 0 {
		log.Printf("Generated file ID after %d retries: fileId=%d", retryCount, fileId)
	}

	err = diskManager.CreateFileWithId(fileId)
	return fileId, err
}

func (diskManager *DiskManager) ReadPage(fileId FileId, pageId PageId, buffer *PageData) error {

	file, err := diskManager.loadFile(fileId)
	if err != nil {
		log.Printf("Failed to load file for reading: fileId=%d, error=%v", fileId, err)
		return err
	}

	fileOffset := pageId * PageSize
	_, err = file.ReadAt(buffer[:], int64(fileOffset))
	if err != nil {
		log.Printf("Failed to read page: fileId=%d, pageId=%d, error=%v", fileId, pageId, err)
	}
	return err
}

func (diskManager *DiskManager) WritePage(fileId FileId, pageId PageId, data *PageData) error {
	file, err := diskManager.loadFile(fileId)
	if err != nil {
		log.Printf("Failed to load file for writing: fileId=%d, error=%v", fileId, err)
		return err
	}

	fileOffset := pageId * PageSize
	_, err = file.WriteAt(data[:], int64(fileOffset))
	if err != nil {
		log.Printf("Failed to write page: fileId=%d, pageId=%d, error=%v", fileId, pageId, err)
	}
	return err
}

func (diskManager *DiskManager) loadFile(fileId FileId) (*os.File, error) {
	file, pres := diskManager.fileHandleMap[fileId]
	if pres {
		diskManager.fileCache.Get(fileId)  // Mark as recently used
		return file, nil
	}

	// Open the file with read-write permissions
	file, err := os.OpenFile(getFilePath(fileId), os.O_RDWR, 0644)
	if err != nil {
		log.Printf("Failed to open file: fileId=%d, error=%v", fileId, err)
		return nil, err
	}

	log.Printf("Opened file: fileId=%d", fileId)

	// Add to cache and handle eviction
	removedKey, evicted := diskManager.fileCache.Put(fileId)

	// If the cache was full, close the evicted file
	if evicted {
		removedFile := diskManager.fileHandleMap[removedKey]
		removedFile.Close()
		delete(diskManager.fileHandleMap, removedKey)
		log.Printf("Evicted file handle from cache: fileId=%d", removedKey)
	}

	diskManager.fileHandleMap[fileId] = file
	return file, nil
}

// Close closes all open file handles
func (diskManager *DiskManager) Close() error {
	count := len(diskManager.fileHandleMap)
	log.Printf("Closing DiskManager: closing %d file handle(s)", count)

	for fileId, file := range diskManager.fileHandleMap {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file: fileId=%d, error=%v", fileId, err)
		}
	}
	diskManager.fileHandleMap = make(map[FileId]*os.File)

	log.Printf("DiskManager closed successfully")
	return nil
}
