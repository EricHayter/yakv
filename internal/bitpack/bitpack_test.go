package bitpack

import (
	"bytes"
	"testing"
)

func TestSerializeDeserializeEmpty(t *testing.T) {
	bools := []bool{}

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected empty slice, got length %d", len(result))
	}
}

func TestSerializeSingleTrue(t *testing.T) {
	bools := []bool{true}

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected length 1, got %d", len(result))
	}

	if result[0] != true {
		t.Errorf("Expected true, got false")
	}
}

func TestSerializeSingleFalse(t *testing.T) {
	bools := []bool{false}

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected length 1, got %d", len(result))
	}

	if result[0] != false {
		t.Errorf("Expected false, got true")
	}
}

func TestSerializeMultipleBoolsLessThan8(t *testing.T) {
	bools := []bool{true, false, true, true, false}

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != len(bools) {
		t.Fatalf("Expected length %d, got %d", len(bools), len(result))
	}

	for i := range bools {
		if result[i] != bools[i] {
			t.Errorf("Index %d: expected %v, got %v", i, bools[i], result[i])
		}
	}
}

func TestSerializeExactly8Bools(t *testing.T) {
	bools := []bool{true, false, true, false, true, false, true, false}

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != len(bools) {
		t.Fatalf("Expected length %d, got %d", len(bools), len(result))
	}

	for i := range bools {
		if result[i] != bools[i] {
			t.Errorf("Index %d: expected %v, got %v", i, bools[i], result[i])
		}
	}
}

func TestSerializeMoreThan8Bools(t *testing.T) {
	bools := []bool{
		true, false, true, false, true, false, true, false,  // byte 0
		false, true, false, true, false, true, false, true,  // byte 1
		true, true, false, false,                             // byte 2 (partial)
	}

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != len(bools) {
		t.Fatalf("Expected length %d, got %d", len(bools), len(result))
	}

	for i := range bools {
		if result[i] != bools[i] {
			t.Errorf("Index %d: expected %v, got %v", i, bools[i], result[i])
		}
	}
}

func TestSerializeAllTrue(t *testing.T) {
	bools := make([]bool, 20)
	for i := range bools {
		bools[i] = true
	}

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != len(bools) {
		t.Fatalf("Expected length %d, got %d", len(bools), len(result))
	}

	for i := range bools {
		if result[i] != true {
			t.Errorf("Index %d: expected true, got false", i)
		}
	}
}

func TestSerializeAllFalse(t *testing.T) {
	bools := make([]bool, 20)
	// All false by default

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != len(bools) {
		t.Fatalf("Expected length %d, got %d", len(bools), len(result))
	}

	for i := range bools {
		if result[i] != false {
			t.Errorf("Index %d: expected false, got true", i)
		}
	}
}

func TestSerializeAlternatingPattern(t *testing.T) {
	bools := make([]bool, 32)
	for i := range bools {
		bools[i] = i%2 == 0
	}

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != len(bools) {
		t.Fatalf("Expected length %d, got %d", len(bools), len(result))
	}

	for i := range bools {
		if result[i] != bools[i] {
			t.Errorf("Index %d: expected %v, got %v", i, bools[i], result[i])
		}
	}
}

func TestSerializeLargeArray(t *testing.T) {
	// Test with a large array (100 bools = 13 bytes)
	bools := make([]bool, 100)
	for i := range bools {
		bools[i] = i%3 == 0 // Every third one is true
	}

	var buf bytes.Buffer
	err := Serialize(&buf, bools)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(result) != len(bools) {
		t.Fatalf("Expected length %d, got %d", len(bools), len(result))
	}

	for i := range bools {
		if result[i] != bools[i] {
			t.Errorf("Index %d: expected %v, got %v", i, bools[i], result[i])
		}
	}
}

func TestSerializeEdgeCases(t *testing.T) {
	testCases := []struct {
		name  string
		bools []bool
	}{
		{"7 bools", []bool{true, false, true, false, true, false, true}},
		{"9 bools", []bool{true, false, true, false, true, false, true, false, true}},
		{"15 bools", make([]bool, 15)},
		{"16 bools", make([]bool, 16)},
		{"17 bools", make([]bool, 17)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Serialize(&buf, tc.bools)
			if err != nil {
				t.Fatalf("Serialize failed: %v", err)
			}

			result, err := Deserialize(&buf)
			if err != nil {
				t.Fatalf("Deserialize failed: %v", err)
			}

			if len(result) != len(tc.bools) {
				t.Fatalf("Expected length %d, got %d", len(tc.bools), len(result))
			}

			for i := range tc.bools {
				if result[i] != tc.bools[i] {
					t.Errorf("Index %d: expected %v, got %v", i, tc.bools[i], result[i])
				}
			}
		})
	}
}

func TestSerializeSpecificPatterns(t *testing.T) {
	testCases := []struct {
		name  string
		bools []bool
	}{
		{
			"first bit only",
			[]bool{true, false, false, false, false, false, false, false},
		},
		{
			"last bit only",
			[]bool{false, false, false, false, false, false, false, true},
		},
		{
			"first and last",
			[]bool{true, false, false, false, false, false, false, true},
		},
		{
			"checkerboard",
			[]bool{true, false, true, false, true, false, true, false},
		},
		{
			"inverse checkerboard",
			[]bool{false, true, false, true, false, true, false, true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Serialize(&buf, tc.bools)
			if err != nil {
				t.Fatalf("Serialize failed: %v", err)
			}

			result, err := Deserialize(&buf)
			if err != nil {
				t.Fatalf("Deserialize failed: %v", err)
			}

			if len(result) != len(tc.bools) {
				t.Fatalf("Expected length %d, got %d", len(tc.bools), len(result))
			}

			for i := range tc.bools {
				if result[i] != tc.bools[i] {
					t.Errorf("Index %d: expected %v, got %v", i, tc.bools[i], result[i])
				}
			}
		})
	}
}

func BenchmarkSerialize(b *testing.B) {
	bools := make([]bool, 1000)
	for i := range bools {
		bools[i] = i%2 == 0
	}

	b.ResetTimer()
	for b.Loop() {
		var buf bytes.Buffer
		Serialize(&buf, bools)
	}
}

func BenchmarkDeserialize(b *testing.B) {
	bools := make([]bool, 1000)
	for i := range bools {
		bools[i] = i%2 == 0
	}

	var buf bytes.Buffer
	Serialize(&buf, bools)
	data := buf.Bytes()

	b.ResetTimer()
	for b.Loop() {
		Deserialize(bytes.NewReader(data))
	}
}
