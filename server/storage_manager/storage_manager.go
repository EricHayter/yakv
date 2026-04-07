package storage_manager

import (
	"io"

	"github.com/EricHayter/yakv/server/buffer_manager"
	"github.com/EricHayter/yakv/server/disk_manager"
)

const (
	PageSize = disk_manager.PageSize
)

// Re-export disk manager types through storage manager as the main interface
type FileId = disk_manager.FileId
type PageId = disk_manager.PageId
type PageData = disk_manager.PageData

// StorageManager provides a unified interface for page-level storage operations.
// It abstracts the underlying DiskManager and BufferManager, providing both
// buffered (cached) and direct (unbuffered) I/O modes.
type StorageManager struct {
	bufferManager *buffer_manager.BufferManager
	diskManager   *disk_manager.DiskManager
}

// Page is a handle to a cached page in memory.
// It wraps buffer_manager.Page and provides additional metadata.
type Page struct {
	inner  buffer_manager.Page
	fileId FileId
	pageId PageId
}

// New creates a new StorageManager with the specified buffer pool capacity.
// The capacity determines how many pages can be cached in memory simultaneously.
//
// Parameters:
//   - bufferCapacity: Number of pages to cache (typically 1000-10000 for production)
//
// Returns:
//   - *StorageManager: The initialized storage manager
//   - error: Any error during initialization
func New(bufferCapacity uint16) (*StorageManager, error) {
	diskManager, err := disk_manager.New()
	if err != nil {
		return nil, err
	}

	bufferManager, err := buffer_manager.New(bufferCapacity, diskManager)
	if err != nil {
		return nil, err
	}

	return &StorageManager{
		bufferManager: bufferManager,
		diskManager:   diskManager,
	}, nil
}

// Close releases all resources held by the StorageManager.
// This includes closing all file handles managed by the underlying DiskManager.
// After calling Close, the StorageManager should not be used.
func (sm *StorageManager) Close() error {
	return sm.diskManager.Close()
}

// CreateFile creates a new file with a randomly generated ID.
// The file is initialized with at least one page of storage.
//
// Returns:
//   - FileId: The generated file ID
//   - error: Any error during file creation
func (sm *StorageManager) CreateFile() (FileId, error) {
	return sm.diskManager.CreateFile()
}

// CreateFileWithId creates a new file with a specific ID.
// Returns os.ErrExist if a file with this ID already exists.
//
// Parameters:
//   - fileId: The desired file ID
//
// Returns:
//   - error: Any error during file creation
func (sm *StorageManager) CreateFileWithId(fileId FileId) error {
	return sm.diskManager.CreateFileWithId(fileId)
}

// AddPage adds a single page to the file and returns the new page's ID.
//
// Parameters:
//   - fileId: The file to add a page to
//
// Returns:
//   - PageId: The ID of the newly added page
//   - error: Any error during page addition
func (sm *StorageManager) AddPage(fileId FileId) (PageId, error) {
	return sm.diskManager.AddPage(fileId)
}

// AddPages adds multiple pages to the file and returns the ID of the first new page.
//
// Parameters:
//   - fileId: The file to add pages to
//   - count: Number of pages to add
//
// Returns:
//   - PageId: The ID of the first newly added page
//   - error: Any error during page addition
func (sm *StorageManager) AddPages(fileId FileId, count uint16) (PageId, error) {
	return sm.diskManager.AddPages(fileId, count)
}

// GetPage retrieves a page from the buffer pool, loading from disk if necessary.
// The page is pinned and won't be evicted until Close() is called on it.
// Multiple calls to GetPage with the same fileId/pageId will increment the pin count.
//
// Parameters:
//   - fileId: The file containing the page
//   - pageId: The page number within the file
//
// Returns:
//   - *Page: A handle to the pinned page
//   - error: Any error during page retrieval
func (sm *StorageManager) GetPage(fileId FileId, pageId PageId) (*Page, error) {
	innerPage, err := sm.bufferManager.GetPage(fileId, pageId)
	if err != nil {
		return nil, err
	}

	return &Page{
		inner:  innerPage,
		fileId: fileId,
		pageId: pageId,
	}, nil
}

// FlushPage writes a dirty page back to disk immediately.
// The page remains in the buffer pool after flushing.
// If the page is not in the buffer pool or is not dirty, this is a no-op.
//
// Parameters:
//   - fileId: The file containing the page
//   - pageId: The page number within the file
//
// Returns:
//   - error: Any error during flushing
func (sm *StorageManager) FlushPage(fileId FileId, pageId PageId) error {
	return sm.bufferManager.FlushPage(fileId, pageId)
}

// ReadPageDirect reads a page directly from disk, bypassing the buffer pool.
// Use this for operations that don't benefit from caching (e.g., sequential scans).
//
// Parameters:
//   - fileId: The file to read from
//   - pageId: The page number to read
//   - buffer: Buffer to read data into
//
// Returns:
//   - error: Any error during reading
func (sm *StorageManager) ReadPageDirect(fileId FileId, pageId PageId, buffer *PageData) error {
	return sm.diskManager.ReadPage(fileId, pageId, buffer)
}

// WritePageDirect writes a page directly to disk, bypassing the buffer pool.
// Use this for atomic operations or when you need immediate durability.
//
// WARNING: If the page is also cached in the buffer pool, this creates inconsistency.
// The caller must ensure either:
//  1. The page is not cached, or
//  2. The cached version is invalidated/evicted
//
// Parameters:
//   - fileId: The file to write to
//   - pageId: The page number to write
//   - data: Data to write
//
// Returns:
//   - error: Any error during writing
func (sm *StorageManager) WritePageDirect(fileId FileId, pageId PageId, data *PageData) error {
	return sm.diskManager.WritePage(fileId, pageId, data)
}

// GetDiskManager returns direct access to the underlying DiskManager.
// This is an escape hatch for special operations like atomic file replacement.
//
// WARNING: Using this breaks the abstraction and can lead to inconsistencies
// with the buffer pool. Only use this when you know what you're doing.
//
// Use cases:
//   - Manifest atomic writes (CreateTemp → Write → Sync → Rename)
//   - Bulk operations that bypass buffering
//   - Direct file system operations
func (sm *StorageManager) GetDiskManager() *disk_manager.DiskManager {
	return sm.diskManager
}

// Close unpins the page, making it eligible for eviction.
// The page must not be accessed after calling Close.
// This method is idempotent - calling it multiple times is safe.
func (p *Page) Close() {
	p.inner.Close()
}

// Lock acquires an exclusive write lock on the page's buffer.
// Only one goroutine can hold a write lock at a time.
// Must be paired with Unlock.
func (p *Page) Lock() {
	p.inner.Lock()
}

// Unlock releases an exclusive write lock on the page's buffer.
func (p *Page) Unlock() {
	p.inner.Unlock()
}

// RLock acquires a read lock on the page's buffer.
// Multiple goroutines can hold read locks simultaneously.
// Must be paired with RUnlock.
func (p *Page) RLock() {
	p.inner.RLock()
}

// RUnlock releases a read lock on the page's buffer.
func (p *Page) RUnlock() {
	p.inner.RUnlock()
}

// GetBuffer returns a pointer to the page's 4KB buffer.
// The page must be pinned (not closed) during buffer access.
// Callers should use RLock/RUnlock for reading or Lock/Unlock for writing.
//
// Returns:
//   - *PageData: Pointer to the 4KB page buffer
func (p *Page) GetBuffer() *PageData {
	return p.inner.GetBuffer()
}

// MarkDirty marks the page as modified.
// Dirty pages will be written to disk when evicted or flushed.
// This method is thread-safe.
func (p *Page) MarkDirty() {
	p.inner.MarkDirty()
}

// GetFileId returns the file ID this page belongs to.
func (p *Page) GetFileId() FileId {
	return p.fileId
}

// GetPageId returns the page number within its file.
func (p *Page) GetPageId() PageId {
	return p.pageId
}

// PageWriter implements io.Writer for writing to a Page.
type PageWriter struct {
	offset int
	page   *Page
}

// NewWriter creates a new PageWriter for this page.
func (p *Page) NewWriter() *PageWriter {
	return &PageWriter{page: p}
}

// Write writes data to the page buffer starting at the current offset.
// Returns the number of bytes written. If the data doesn't fit, it writes
// as much as possible and the caller can detect a short write by checking n < len(p).
func (w *PageWriter) Write(p []byte) (n int, err error) {
	if w.offset >= PageSize {
		return 0, io.EOF
	}
	n = min(len(p), PageSize-w.offset)
	copy(w.page.GetBuffer()[w.offset:w.offset+n], p[:n])
	w.offset += n
	return n, nil
}

// WriteAt writes data to the page buffer at the specified offset.
// Does not affect the internal offset used by Write().
// Implements io.WriterAt interface.
func (w *PageWriter) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= PageSize {
		return 0, io.EOF
	}
	offset := int(off)
	n = min(len(p), PageSize-offset)
	copy(w.page.GetBuffer()[offset:offset+n], p[:n])
	return n, nil
}

// PageReader implements io.Reader for reading from a Page.
type PageReader struct {
	offset int
	page   *Page
}

// NewReader creates a new PageReader for this page.
func (p *Page) NewReader() *PageReader {
	return &PageReader{page: p, offset: 0}
}

// Read reads data from the page buffer starting at the current offset.
// Returns io.EOF when reaching the end of the page.
func (r *PageReader) Read(p []byte) (n int, err error) {
	if r.offset >= PageSize {
		return 0, io.EOF
	}
	n = min(len(p), PageSize-r.offset)
	copy(p, r.page.GetBuffer()[r.offset:r.offset+n])
	r.offset += n
	if r.offset >= PageSize {
		return n, io.EOF
	}
	return n, nil
}

// ReadAt reads data from the page buffer at the specified offset.
// Does not affect the internal offset used by Read().
// Implements io.ReaderAt interface.
func (r *PageReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= PageSize {
		return 0, io.EOF
	}
	offset := int(off)
	n = min(len(p), PageSize-offset)
	copy(p, r.page.GetBuffer()[offset:offset+n])
	if offset+n >= PageSize {
		return n, io.EOF
	}
	return n, nil
}
