package buffer_manager

import (
	"errors"
	"sync"
	"github.com/EricHayter/yakv/server/disk_manager"
	"github.com/EricHayter/yakv/internal/lru"
)

func GetPageId(offset uint64) disk_manager.PageId {
	return disk_manager.PageId(offset / disk_manager.PageSize)
}

type frameId uint16

type pageKey struct {
	fileId disk_manager.FileId
	pageId disk_manager.PageId
}

type frame struct {
	id 			frameId
	fileId 		disk_manager.FileId
	pageId 		disk_manager.PageId
	dirty 		bool
	mut 		sync.RWMutex  // Protects fileId, pageId, dirty, buffer
	pinCount 	uint32        // Protected by BufferManager.mu
	buffer 		*disk_manager.PageData
}

type Page struct {
	bufferManager *BufferManager
	frameId       frameId
}

func (page *Page) Close() {
	page.bufferManager.mu.Lock()
	defer page.bufferManager.mu.Unlock()
	page.bufferManager.unpinPage(page.frameId)
}

// RLock acquires a read lock on the page's buffer.
// Multiple goroutines can hold read locks simultaneously.
// Must be paired with RUnlock.
func (page *Page) RLock() {
	frame := &page.bufferManager.frames[page.frameId]
	frame.mut.RLock()
}

// RUnlock releases a read lock on the page's buffer.
func (page *Page) RUnlock() {
	frame := &page.bufferManager.frames[page.frameId]
	frame.mut.RUnlock()
}

// Lock acquires an exclusive write lock on the page's buffer.
// Only one goroutine can hold a write lock at a time.
// Must be paired with Unlock.
func (page *Page) Lock() {
	frame := &page.bufferManager.frames[page.frameId]
	frame.mut.Lock()
}

// Unlock releases an exclusive write lock on the page's buffer.
func (page *Page) Unlock() {
	frame := &page.bufferManager.frames[page.frameId]
	frame.mut.Unlock()
}

// GetBuffer returns a pointer to the page's buffer.
// The page must be pinned (not closed) during buffer access.
// Callers should use RLock/RUnlock for reading or Lock/Unlock for writing
// to ensure proper synchronization when accessing the buffer.
func (page *Page) GetBuffer() *disk_manager.PageData {
	frame := &page.bufferManager.frames[page.frameId]
	return frame.buffer
}

// MarkDirty marks the page as modified.
// Acquires the page lock internally.
func (page *Page) MarkDirty() {
	frame := &page.bufferManager.frames[page.frameId]
	frame.mut.Lock()
	defer frame.mut.Unlock()
	frame.dirty = true
}

func New(pageCapacity uint16) (*BufferManager, error) {
	diskManager, err := disk_manager.New()
	if err != nil {
		return nil, err
	}

	bufferManager := &BufferManager{
		filePageMap: make(map[pageKey]frameId),
		frames: make([]frame, pageCapacity),
		diskManager: diskManager,
		frameReplacer: lru.New[frameId](),
	}

	for i := range pageCapacity {
		bufferManager.frameReplacer.Push(frameId(i))
		bufferManager.frames[i] = frame{
			id: frameId(i),
			mut: sync.RWMutex{},
			pinCount: 0,
			buffer: new(disk_manager.PageData),
		}
	}

	return bufferManager, nil
}

type BufferManager struct {
	mu            sync.Mutex  // Protects pin count + replacer coordination
	filePageMap   map[pageKey]frameId
	frames        []frame
	diskManager   *disk_manager.DiskManager
	frameReplacer *lru.Replacer[frameId]
}

// pinPage increments the pin count and removes from replacer
// Caller must hold bm.mu
func (bm *BufferManager) pinPage(frameId frameId) {
	frame := &bm.frames[frameId]
	frame.pinCount++
	// Remove from replacer if pin count was 0 (now pinned, can't be evicted)
	if frame.pinCount == 1 {
		bm.frameReplacer.Remove(frameId)
	}
}

// unpinPage decrements the pin count and adds back to replacer if count reaches 0
// Caller must hold bm.mu
func (bm *BufferManager) unpinPage(frameId frameId) {
	frame := &bm.frames[frameId]
	if frame.pinCount == 0 {
		// This should never happen - indicates a bug
		panic("unpinPage called on frame with pinCount 0")
	}
	frame.pinCount--

	// If pin count reached 0, add back to replacer (makes it evictable)
	if frame.pinCount == 0 {
		bm.frameReplacer.Push(frameId)
	}
}

// GetPage retrieves a page from the buffer pool, loading from disk if necessary.
// The returned Page must be closed when done to release the pin.
func (bufferManager *BufferManager) GetPage(fileId disk_manager.FileId, pageId disk_manager.PageId) (Page, error) {
	key := pageKey{fileId: fileId, pageId: pageId}

	// Hold bm.mu for the entire operation to prevent duplicate loading
	bufferManager.mu.Lock()
	defer bufferManager.mu.Unlock()

	// Check if page is already in buffer
	frameId, prs := bufferManager.filePageMap[key]
	if prs {
		// Page is already in buffer, just pin it
		bufferManager.pinPage(frameId)
		return Page{ bufferManager: bufferManager, frameId: frameId }, nil
	}

	// Page not in buffer, need to load from disk
	return bufferManager.loadPageLocked(fileId, pageId)
}

// loadPageLocked loads a page from disk into the buffer pool.
// Caller must hold bm.mu.
func (bufferManager* BufferManager) loadPageLocked(fileId disk_manager.FileId, pageId disk_manager.PageId) (Page, error) {
	key := pageKey{
		fileId: fileId,
		pageId: pageId,
	}

	// Get a frame to evict from the replacer
	frameId := bufferManager.frameReplacer.Pop()
	frame := &bufferManager.frames[frameId]

	// Lock the frame to access its metadata safely
	frame.mut.Lock()

	// If the frame is dirty, flush it first
	if frame.dirty {
		oldFileId := frame.fileId
		oldPageId := frame.pageId

		// Unlock frame and bm.mu to avoid holding both during I/O
		frame.mut.Unlock()
		bufferManager.mu.Unlock()

		err := bufferManager.diskManager.WritePage(oldFileId, oldPageId, frame.buffer)

		// Re-acquire locks
		bufferManager.mu.Lock()
		frame.mut.Lock()

		if err != nil {
			// Put frame back in replacer since we couldn't flush
			frame.mut.Unlock()
			bufferManager.frameReplacer.Push(frameId)
			return Page{}, err
		}
		frame.dirty = false
	}

	// Remove old page mapping if frame was used for another page
	if frame.fileId != 0 || frame.pageId != 0 {
		oldKey := pageKey{fileId: frame.fileId, pageId: frame.pageId}
		delete(bufferManager.filePageMap, oldKey)
	}

	// Update frame metadata before reading (so other threads see correct state)
	frame.fileId = fileId
	frame.pageId = pageId
	frame.dirty = false

	// Unlock frame during disk I/O, unlock bm.mu too
	frame.mut.Unlock()
	bufferManager.mu.Unlock()

	// Read new page from disk (without holding locks)
	err := bufferManager.diskManager.ReadPage(fileId, pageId, frame.buffer)

	// Re-acquire locks
	bufferManager.mu.Lock()

	if err != nil {
		// Put frame back in replacer since read failed
		// Reset frame state
		frame.mut.Lock()
		frame.fileId = 0
		frame.pageId = 0
		frame.mut.Unlock()

		bufferManager.frameReplacer.Push(frameId)
		return Page{}, err
	}

	// Increment pin count (frame is now pinned by caller)
	bufferManager.pinPage(frameId)

	// Add new page mapping
	bufferManager.filePageMap[key] = frameId

	return Page{ bufferManager: bufferManager, frameId: frameId }, nil
}

// FlushPage writes a dirty page back to disk.
func (bufferManager* BufferManager) FlushPage(fileId disk_manager.FileId, pageId disk_manager.PageId) error {
	key := pageKey{fileId: fileId, pageId: pageId}

	// Lock to check if page is in buffer
	bufferManager.mu.Lock()
	frameId, prs := bufferManager.filePageMap[key]
	bufferManager.mu.Unlock()

	if !prs {
		return errors.New("Page not in buffer manager, nothing to flush.")
	}

	frame := &bufferManager.frames[frameId]

	// Lock frame to check dirty flag and access buffer
	frame.mut.Lock()

	// Nothing to write out since the page isn't dirty
	if !frame.dirty {
		frame.mut.Unlock()
		return nil
	}

	// Unlock during I/O
	frame.mut.Unlock()

	// Write to disk
	err := bufferManager.diskManager.WritePage(fileId, pageId, frame.buffer)

	// If successful, clear dirty flag
	if err == nil {
		frame.mut.Lock()
		frame.dirty = false
		frame.mut.Unlock()
	}

	return err
}

