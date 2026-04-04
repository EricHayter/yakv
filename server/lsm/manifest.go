package lsm

import (
	"os"
	"io"
	"errors"
	"github.com/EricHayter/yakv/server/disk_manager"
	"encoding/binary"
	"path/filepath"
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
)

var ManifestPath = filepath.Join(disk_manager.YakvDirectory, ManifestFileName)

type Manifest struct {
   diskManager 	*disk_manager.DiskManager
   version 		Version
}

type Version struct {
	lastTimestamp uint64
	sstables [][]disk_manager.FileId
}

// Serialize writes the version to a writer
func (v *Version) Serialize(w io.Writer) error {
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

// DeserializeVersion creates a Version from a reader
func DeserializeVersion(r io.Reader) (*Version, error) {
	v := &Version{}

	// Read timestamp
	if err := binary.Read(r, binary.LittleEndian, &v.lastTimestamp); err != nil {
		return nil, err
	}

	// Read number of levels
	var numLevels uint16
	if err := binary.Read(r, binary.LittleEndian, &numLevels); err != nil {
		return nil, err
	}

	v.sstables = make([][]disk_manager.FileId, numLevels)

	// Read each level
	for i := range numLevels {
		var numTables uint16
		if err := binary.Read(r, binary.LittleEndian, &numTables); err != nil {
			return nil, err
		}

		v.sstables[i] = make([]disk_manager.FileId, numTables)

		// Read file IDs
		for j := range numTables {
			var fileId uint16
			if err := binary.Read(r, binary.LittleEndian, &fileId); err != nil {
				return nil, err
			}
			v.sstables[i][j] = disk_manager.FileId(fileId)
		}
	}

	return v, nil
}

func (manifest *Manifest) FlushLsmMetadata() error {
	/* Create a temporary file in the yakv directory, then serialize the
	 * version data. Once the file is written, an atomic rename is done such
	 * that the old data is only replaced IF we successfully write the new
	 * version data.
	 */
	manifestPath := ManifestPath

	// Create temp file in yakv directory (same filesystem for atomic rename)
	f, err := os.CreateTemp(disk_manager.YakvDirectory, ManifestFileName+"-*.tmp")
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

	if err := manifest.version.Serialize(f); err != nil {
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
