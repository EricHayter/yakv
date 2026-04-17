package lsm

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/EricHayter/yakv/server/common"
	"github.com/EricHayter/yakv/server/disk_manager"
	"github.com/EricHayter/yakv/server/storage_manager"
)

// Helper function to create test version
func createTestVersion(timestamp uint64, levels int, filesPerLevel int) *version {
	sstables := make([][]disk_manager.FileId, levels)
	fileId := disk_manager.FileId(1)

	for i := 0; i < levels; i++ {
		sstables[i] = make([]disk_manager.FileId, filesPerLevel)
		for j := 0; j < filesPerLevel; j++ {
			sstables[i][j] = fileId
			fileId++
		}
	}

	return &version{
		lastTimestamp: timestamp,
		sstables:      sstables,
	}
}

// =============================================================================
// Serialize/Deserialize Tests
// =============================================================================

func TestVersionSerializeDeserializeRoundTrip(t *testing.T) {
	tests := []struct {
		name          string
		timestamp     uint64
		levels        int
		filesPerLevel int
	}{
		{"Empty version", 0, 0, 0},
		{"Single level, single file", 100, 1, 1},
		{"Single level, multiple files", 200, 1, 5},
		{"Multiple levels, single file each", 300, 3, 1},
		{"Multiple levels, multiple files", 500, 4, 3},
		{"Large timestamp", 18446744073709551615, 2, 2}, // max uint64
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create original version
			original := createTestVersion(tt.timestamp, tt.levels, tt.filesPerLevel)

			// Serialize
			var buf bytes.Buffer
			if err := original.serialize(&buf); err != nil {
				t.Fatalf("Serialize failed: %v", err)
			}

			// Deserialize
			deserialized, err := deserializeVersion(&buf)
			if err != nil {
				t.Fatalf("Deserialize failed: %v", err)
			}

			// Verify timestamp
			if deserialized.lastTimestamp != original.lastTimestamp {
				t.Errorf("Timestamp mismatch: got %d, want %d",
					deserialized.lastTimestamp, original.lastTimestamp)
			}

			// Verify number of levels
			if len(deserialized.sstables) != len(original.sstables) {
				t.Errorf("Level count mismatch: got %d, want %d",
					len(deserialized.sstables), len(original.sstables))
			}

			// Verify each level
			for i := range original.sstables {
				if len(deserialized.sstables[i]) != len(original.sstables[i]) {
					t.Errorf("Level %d file count mismatch: got %d, want %d",
						i, len(deserialized.sstables[i]), len(original.sstables[i]))
				}

				// Verify file IDs
				for j := range original.sstables[i] {
					if deserialized.sstables[i][j] != original.sstables[i][j] {
						t.Errorf("Level %d, file %d ID mismatch: got %d, want %d",
							i, j, deserialized.sstables[i][j], original.sstables[i][j])
					}
				}
			}
		})
	}
}

func TestVersionDeserializeEmptyData(t *testing.T) {
	var buf bytes.Buffer
	_, err := deserializeVersion(&buf)
	if err == nil {
		t.Error("Expected error when deserializing empty data")
	}
}

func TestVersionDeserializeTruncatedData(t *testing.T) {
	// Create a valid version
	original := createTestVersion(100, 2, 3)
	var buf bytes.Buffer
	original.serialize(&buf)

	// Truncate the data
	data := buf.Bytes()

	tests := []struct {
		name   string
		length int
	}{
		{"Only timestamp", 8},
		{"Timestamp + partial levels", 10},
		{"Missing file IDs", 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			truncated := bytes.NewReader(data[:tt.length])
			_, err := deserializeVersion(truncated)
			if err == nil {
				t.Error("Expected error when deserializing truncated data")
			}
		})
	}
}

func TestVersionSerializeWriteError(t *testing.T) {
	version := createTestVersion(100, 2, 3)

	// Use a writer that always fails
	errWriter := &failWriter{}

	err := version.serialize(errWriter)
	if err == nil {
		t.Error("Expected error when writing to failing writer")
	}
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestVersionEmptyLevels(t *testing.T) {
	version := &version{
		lastTimestamp: 42,
		sstables:      [][]disk_manager.FileId{},
	}

	var buf bytes.Buffer
	if err := version.serialize(&buf); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := deserializeVersion(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if deserialized.lastTimestamp != 42 {
		t.Errorf("Timestamp mismatch: got %d, want 42", deserialized.lastTimestamp)
	}

	if len(deserialized.sstables) != 0 {
		t.Errorf("Expected 0 levels, got %d", len(deserialized.sstables))
	}
}

func TestVersionEmptyLevel(t *testing.T) {
	version := &version{
		lastTimestamp: 100,
		sstables: [][]disk_manager.FileId{
			{1, 2, 3},
			{}, // Empty level
			{4, 5},
		},
	}

	var buf bytes.Buffer
	if err := version.serialize(&buf); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := deserializeVersion(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(deserialized.sstables) != 3 {
		t.Errorf("Expected 3 levels, got %d", len(deserialized.sstables))
	}

	if len(deserialized.sstables[1]) != 0 {
		t.Errorf("Expected empty level 1, got %d files", len(deserialized.sstables[1]))
	}
}

func TestVersionLargeNumberOfLevels(t *testing.T) {
	const numLevels = 100
	version := createTestVersion(1000, numLevels, 2)

	var buf bytes.Buffer
	if err := version.serialize(&buf); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := deserializeVersion(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(deserialized.sstables) != numLevels {
		t.Errorf("Level count mismatch: got %d, want %d",
			len(deserialized.sstables), numLevels)
	}
}

// =============================================================================
// Manifest FlushLsmMetadata Tests
// =============================================================================

func TestManifestFlushAndLoad(t *testing.T) {
	// Clean up yakv directory before test
	os.RemoveAll(common.YakvDirectory)
	defer os.RemoveAll(common.YakvDirectory)

	// Ensure yakv directory exists
	if err := os.MkdirAll(common.YakvDirectory, 0755); err != nil {
		t.Fatalf("Failed to create yakv directory: %v", err)
	}

	// Create LSM with test data
	sm, err := storage_manager.New(100)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer sm.Close()

	testVersion := createTestVersion(500, 3, 2)
	lsm := &LogStructuredMergeTree{
		lastTimestamp:  testVersion.lastTimestamp,
		sstables:       testVersion.sstables,
		storageManager: sm,
	}

	// Create manifest
	m := &manifest{
		lsm: lsm,
	}

	// Flush to disk
	if err := m.flushLsmMetadata(); err != nil {
		t.Fatalf("flushLsmMetadata failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(ManifestPath); os.IsNotExist(err) {
		t.Fatal("Manifest file was not created")
	}

	// Read back from disk
	f, err := os.Open(ManifestPath)
	if err != nil {
		t.Fatalf("Failed to open manifest file: %v", err)
	}
	defer f.Close()

	loaded, err := deserializeVersion(f)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	// Verify data matches
	if loaded.lastTimestamp != testVersion.lastTimestamp {
		t.Errorf("Timestamp mismatch: got %d, want %d",
			loaded.lastTimestamp, testVersion.lastTimestamp)
	}

	if len(loaded.sstables) != len(testVersion.sstables) {
		t.Errorf("Level count mismatch: got %d, want %d",
			len(loaded.sstables), len(testVersion.sstables))
	}
}

func TestManifestAtomicWrite(t *testing.T) {
	// Clean up yakv directory before test
	os.RemoveAll(common.YakvDirectory)
	defer os.RemoveAll(common.YakvDirectory)

	// Ensure yakv directory exists
	if err := os.MkdirAll(common.YakvDirectory, 0755); err != nil {
		t.Fatalf("Failed to create yakv directory: %v", err)
	}

	// Create storage manager
	sm, err := storage_manager.New(100)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer sm.Close()

	// Create LSM and manifest with initial data
	testVersion1 := createTestVersion(100, 1, 1)
	lsm := &LogStructuredMergeTree{
		lastTimestamp:  testVersion1.lastTimestamp,
		sstables:       testVersion1.sstables,
		storageManager: sm,
	}
	m := &manifest{
		lsm: lsm,
	}

	if err := m.flushLsmMetadata(); err != nil {
		t.Fatalf("First flush failed: %v", err)
	}

	// Update LSM data
	testVersion2 := createTestVersion(200, 2, 2)
	lsm.mu.Lock()
	lsm.lastTimestamp = testVersion2.lastTimestamp
	lsm.sstables = testVersion2.sstables
	lsm.mu.Unlock()

	// Flush again
	if err := m.flushLsmMetadata(); err != nil {
		t.Fatalf("Second flush failed: %v", err)
	}

	// Verify only the new manifest exists
	f, err := os.Open(ManifestPath)
	if err != nil {
		t.Fatalf("Failed to open manifest: %v", err)
	}
	defer f.Close()

	loaded, err := deserializeVersion(f)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	if loaded.lastTimestamp != 200 {
		t.Errorf("Expected timestamp 200, got %d", loaded.lastTimestamp)
	}
}

func TestManifestFlushCreatesFile(t *testing.T) {
	// Clean up yakv directory before test
	os.RemoveAll(common.YakvDirectory)
	defer os.RemoveAll(common.YakvDirectory)

	// Ensure yakv directory exists
	if err := os.MkdirAll(common.YakvDirectory, 0755); err != nil {
		t.Fatalf("Failed to create yakv directory: %v", err)
	}

	// Create LSM and manifest
	sm, err := storage_manager.New(100)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer sm.Close()

	testVersion := createTestVersion(42, 1, 1)
	lsm := &LogStructuredMergeTree{
		lastTimestamp:  testVersion.lastTimestamp,
		sstables:       testVersion.sstables,
		storageManager: sm,
	}
	m := &manifest{
		lsm: lsm,
	}

	if err := m.flushLsmMetadata(); err != nil {
		t.Fatalf("flushLsmMetadata failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(ManifestPath); os.IsNotExist(err) {
		t.Error("Manifest file was not created")
	}
}

// =============================================================================
// Helper Types
// =============================================================================

// failWriter always returns an error on Write
type failWriter struct{}

func (fw *failWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrShortWrite
}
