package bloom_filter

import (
	"bytes"
	"testing"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name               string
		numBits            uint
		numHashFunctions   uint
		expectError        bool
		errorMsg           string
	}{
		{
			name:             "valid power of 2",
			numBits:          1024,
			numHashFunctions: 3,
			expectError:      false,
		},
		{
			name:             "another valid power of 2",
			numBits:          256,
			numHashFunctions: 5,
			expectError:      false,
		},
		{
			name:             "not a power of 2",
			numBits:          100,
			numHashFunctions: 3,
			expectError:      true,
			errorMsg:         "numBits must be a power of 2",
		},
		{
			name:             "zero bits",
			numBits:          0,
			numHashFunctions: 3,
			expectError:      true,
			errorMsg:         "numBits must be a positive number (> 0)",
		},
		{
			name:             "zero hash functions",
			numBits:          256,
			numHashFunctions: 0,
			expectError:      true,
			errorMsg:         "numHashFunctions must be a positive number (> 0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf, err := New(tt.numBits, tt.numHashFunctions)

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error but got nil")
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error message %q, got %q", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if bf == nil {
				t.Fatal("expected non-nil bloom filter")
			}

			if len(bf.filters) != int(tt.numHashFunctions) {
				t.Errorf("expected %d hash functions, got %d", tt.numHashFunctions, len(bf.filters))
			}

			if len(bf.bits) != int(tt.numBits) {
				t.Errorf("expected %d bits, got %d", tt.numBits, len(bf.bits))
			}
		})
	}
}

func TestInsertAndPresent(t *testing.T) {
	bf, err := New(1024, 3)
	if err != nil {
		t.Fatalf("failed to create bloom filter: %v", err)
	}

	// Test that items not inserted return false
	if bf.Present([]byte("hello")) {
		t.Error("expected false for item not inserted")
	}

	// Insert an item
	bf.Insert([]byte("hello"))

	// Test that inserted item returns true
	if !bf.Present([]byte("hello")) {
		t.Error("expected true for inserted item")
	}

	// Test that different items still return false
	if bf.Present([]byte("world")) {
		t.Error("expected false for different item not inserted")
	}

	// Insert more items
	bf.Insert([]byte("world"))
	bf.Insert([]byte("foo"))
	bf.Insert([]byte("bar"))

	// All inserted items should be present
	if !bf.Present([]byte("hello")) {
		t.Error("expected hello to be present")
	}
	if !bf.Present([]byte("world")) {
		t.Error("expected world to be present")
	}
	if !bf.Present([]byte("foo")) {
		t.Error("expected foo to be present")
	}
	if !bf.Present([]byte("bar")) {
		t.Error("expected bar to be present")
	}
}

func TestNoFalseNegatives(t *testing.T) {
	bf, err := New(512, 4)
	if err != nil {
		t.Fatalf("failed to create bloom filter: %v", err)
	}

	items := []string{
		"apple", "banana", "cherry", "date", "elderberry",
		"fig", "grape", "honeydew", "kiwi", "lemon",
	}

	// Insert all items
	for _, item := range items {
		bf.Insert([]byte(item))
	}

	// Bloom filter must never have false negatives
	for _, item := range items {
		if !bf.Present([]byte(item)) {
			t.Errorf("false negative: %s should be present", item)
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	// Small bloom filter to increase false positive rate for testing
	bf, err := New(64, 2)
	if err != nil {
		t.Fatalf("failed to create bloom filter: %v", err)
	}

	// Insert a few items
	inserted := []string{"item1", "item2", "item3"}
	for _, item := range inserted {
		bf.Insert([]byte(item))
	}

	// Test many items that weren't inserted
	// We expect some false positives
	notInserted := []string{
		"test1", "test2", "test3", "test4", "test5",
		"test6", "test7", "test8", "test9", "test10",
		"test11", "test12", "test13", "test14", "test15",
	}

	falsePositives := 0
	for _, item := range notInserted {
		if bf.Present([]byte(item)) {
			falsePositives++
		}
	}

	// With a small bloom filter, we should see some false positives
	// but not too many. This is a sanity check.
	t.Logf("False positive rate: %d/%d (%.2f%%)",
		falsePositives, len(notInserted),
		float64(falsePositives)/float64(len(notInserted))*100)
}

func TestSerializeDeserialize(t *testing.T) {
	// Create a bloom filter and insert some items
	bf, err := New(256, 3)
	if err != nil {
		t.Fatalf("failed to create bloom filter: %v", err)
	}

	items := []string{"apple", "banana", "cherry"}
	for _, item := range items {
		bf.Insert([]byte(item))
	}

	// Serialize to buffer
	var buf bytes.Buffer
	if err := bf.Serialize(&buf); err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Deserialize from buffer
	deserialized, err := DeserializeBloomFilter(&buf)
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	// Verify structure
	if len(deserialized.filters) != len(bf.filters) {
		t.Errorf("expected %d hash functions, got %d",
			len(bf.filters), len(deserialized.filters))
	}

	if len(deserialized.bits) != len(bf.bits) {
		t.Errorf("expected %d bits, got %d",
			len(bf.bits), len(deserialized.bits))
	}

	// Verify all inserted items are still present
	for _, item := range items {
		if !deserialized.Present([]byte(item)) {
			t.Errorf("deserialized bloom filter missing item: %s", item)
		}
	}

	// Verify items not inserted still return false
	if deserialized.Present([]byte("notinserted")) {
		t.Error("deserialized bloom filter should not contain 'notinserted'")
	}
}

func TestSerializeDeserializeEmpty(t *testing.T) {
	// Test serialization of empty bloom filter
	bf, err := New(128, 2)
	if err != nil {
		t.Fatalf("failed to create bloom filter: %v", err)
	}

	var buf bytes.Buffer
	if err := bf.Serialize(&buf); err != nil {
		t.Fatalf("failed to serialize empty bloom filter: %v", err)
	}

	deserialized, err := DeserializeBloomFilter(&buf)
	if err != nil {
		t.Fatalf("failed to deserialize empty bloom filter: %v", err)
	}

	// Empty bloom filter should return false for any item
	if deserialized.Present([]byte("anything")) {
		t.Error("empty bloom filter should not contain any items")
	}
}

func BenchmarkInsert(b *testing.B) {
	bf, _ := New(1024, 3)
	data := []byte("benchmark test data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Insert(data)
	}
}

func BenchmarkPresent(b *testing.B) {
	bf, _ := New(1024, 3)
	data := []byte("benchmark test data")
	bf.Insert(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Present(data)
	}
}

func BenchmarkSerialize(b *testing.B) {
	bf, _ := New(1024, 3)
	bf.Insert([]byte("test1"))
	bf.Insert([]byte("test2"))
	bf.Insert([]byte("test3"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		bf.Serialize(&buf)
	}
}

func BenchmarkDeserialize(b *testing.B) {
	bf, _ := New(1024, 3)
	bf.Insert([]byte("test1"))
	bf.Insert([]byte("test2"))

	var buf bytes.Buffer
	bf.Serialize(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DeserializeBloomFilter(bytes.NewReader(data))
	}
}
