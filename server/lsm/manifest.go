package lsm

import (
	"os"
	"io"
	"errors"
	"log/slog"
	"time"
	"encoding/binary"
	"path/filepath"
	"github.com/EricHayter/yakv/server/storage_manager"
)

var (
	ErrInvalidManifestData = errors.New("invalid manifest data")
	ErrManifestTooShort    = errors.New("manifest data too short")
)

/* Manifest file
 * The manifest file will be used to persist information relating to the LSM
 * tree so that it may be rebuilt after restarts.
 *
 * In particular, the manifest file will contain the following information:
 * - last timestamp inserted into the lsm
 * - information on each of the levels of the sstables in particular, the
 *   number of levels, the amount of files per level, and the file ids of each
 *   of the files in the level.
 *
 * Current format:
 * <latest timestamp> [uint64]
 * <number of levels> [uint16]
 * <entries in level 0> [uint16]
 * <fileId for first table in level 0> [fileId]
 * <fileId for second table in level 0> [fileId]
 * ...
 * <fileId for nth table in level 0> [fileId]
 * <entries in level n> [uint16]
 * <fileId for first table in level n> [fileId]
 * <fileId for second table in level n> [fileId]
 * ...
 * <fileId for nth table in level n> [fileId]
 */

const (
	ManifestFileName = "manifest"
	YakvDirectory = "yakv"
)

var ManifestPath = filepath.Join(YakvDirectory, ManifestFileName)

type manifest struct {
	syncing        bool
	flushSignaler  <-chan struct{}
	storageManager *storage_manager.StorageManager
	lsm            *LogStructuredMergeTree
}

type version struct {
	lastTimestamp uint64
	sstables      [][]storage_manager.FileId
}

// serialize writes the version to a writer
func (v *version) serialize(w io.Writer) error {
	// Write timestamp
	if err := binary.Write(w, binary.LittleEndian, v.lastTimestamp); err != nil {
		return err
	}

	// Write number of levels
	numLevels := uint16(len(v.sstables))
	if err := binary.Write(w, binary.LittleEndian, numLevels); err != nil {
		return err
	}

	// Write each level
	for _, level := range v.sstables {
		numTables := uint16(len(level))
		if err := binary.Write(w, binary.LittleEndian, numTables); err != nil {
			return err
		}

		// Write file IDs
		for _, fileId := range level {
			if err := binary.Write(w, binary.LittleEndian, uint16(fileId)); err != nil {
				return err
			}
		}
	}

	return nil
}

// deserializeVersion creates a version from a reader
func deserializeVersion(r io.Reader) (*version, error) {
	v := &version{}

	// Read timestamp
	if err := binary.Read(r, binary.LittleEndian, &v.lastTimestamp); err != nil {
		return nil, err
	}

	// Read number of levels
	var numLevels uint16
	if err := binary.Read(r, binary.LittleEndian, &numLevels); err != nil {
		return nil, err
	}

	v.sstables = make([][]storage_manager.FileId, numLevels)

	// Read each level
	for i := range numLevels {
		var numTables uint16
		if err := binary.Read(r, binary.LittleEndian, &numTables); err != nil {
			return nil, err
		}

		v.sstables[i] = make([]storage_manager.FileId, numTables)

		// Read file IDs
		for j := range numTables {
			var fileId uint16
			if err := binary.Read(r, binary.LittleEndian, &fileId); err != nil {
				return nil, err
			}
			v.sstables[i][j] = storage_manager.FileId(fileId)
		}
	}

	return v, nil
}

func (m *manifest) flushLsmMetadata() error {
	/* Create a temporary file in the yakv directory, then serialize the
	 * version data. Once the file is written, an atomic rename is done such
	 * that the old data is only replaced IF we successfully write the new
	 * version data.
	 */
	manifestPath := ManifestPath

	// Get current version snapshot from LSM
	v := m.lsm.getVersion()

	// Create temp file in yakv directory (same filesystem for atomic rename)
	f, err := os.CreateTemp(YakvDirectory, ManifestFileName+"-*.tmp")
	if err != nil {
		return err
	}
	tempPath := f.Name()

	// Cleanup on error
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(tempPath)
		}
	}()

	if err := v.serialize(f); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}
	f = nil // Prevent defer from closing again

	if err := os.Rename(tempPath, manifestPath); err != nil {
		os.Remove(tempPath)
		return err
	}

	return nil
}

func (m *manifest) versionFlusher() {
	for m.syncing {
		select {
		case <-m.flushSignaler:
			// Explicit signal from LSM (e.g., sstables changed)
			err := m.flushLsmMetadata()
			if err != nil {
				slog.Error(err.Error())
			}
		case <-time.After(50 * time.Millisecond):
			// Periodic flush to catch timestamp updates
			err := m.flushLsmMetadata()
			if err != nil {
				slog.Error(err.Error())
			}
		}
	}
}

// loadVersion reads the version from disk. Returns nil if file doesn't exist.
func loadVersion() (*version, error) {
	f, err := os.Open(ManifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No manifest file, fresh start
		}
		return nil, err
	}
	defer f.Close()

	return deserializeVersion(f)
}

// newManifest creates a new manifest for the given LSM and starts the background flusher.
func newManifest(lsm *LogStructuredMergeTree, storageManager *storage_manager.StorageManager, flushSignaler <-chan struct{}) *manifest {
	m := &manifest{
		syncing:        true,
		flushSignaler:  flushSignaler,
		storageManager: storageManager,
		lsm:            lsm,
	}

	// Start background flusher
	go m.versionFlusher()

	return m
}
