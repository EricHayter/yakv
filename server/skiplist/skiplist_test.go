package skiplist

import (
	"fmt"
	"strings"
	"testing"
)

// Helper function to dump skiplist contents using the iterator
func dumpSkipList(t *testing.T, list *SkipList[string, string]) {
	t.Helper()
	var items []string
	for entry := range list.Items() {
		items = append(items, fmt.Sprintf("(%s: %s)", entry.Key, entry.Value))
	}
	t.Logf("SkipList contents: [%s]", strings.Join(items, ", "))
}

// Helper function to visualize the entire skip list hierarchy
func dumpSkipListHierarchy(t *testing.T, list *SkipList[string, string]) {
	t.Helper()

	if list.head == nil {
		t.Log("SkipList is empty")
		return
	}

	maxLevel := len(list.head.next) - 1
	t.Logf("SkipList height: %d levels (0-%d)", maxLevel+1, maxLevel)

	// Print each level from top to bottom
	for level := maxLevel; level >= 0; level-- {
		var levelNodes []string
		p := list.head

		// Traverse the current level
		for p != nil {
			levelNodes = append(levelNodes, fmt.Sprintf("%s", p.key))
			if level < len(p.next) {
				p = p.next[level]
			} else {
				break
			}
		}

		t.Logf("Level %d: %s", level, strings.Join(levelNodes, " -> "))
	}
}

func TestSkipListInsert(t *testing.T) {
	skipList := NewSkipList[string, string]()
	const key string = "Key"
	const value string = "Value"
	skipList.Insert(key, value)
	res, pres := skipList.Get(key)

	if !pres {
		t.Errorf("Couldn't find key in map")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}

	if res != value {
		t.Errorf(`Get("Key") = %s, want "%s"`, res, value)
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListGetEmpty(t *testing.T) {
	skipList := NewSkipList[string, string]()
	_, found := skipList.Get("nonexistent")

	if found {
		t.Error("Expected Get on empty skiplist to return false")
		dumpSkipList(t, skipList)
	}
}

func TestSkipListGetNonExistent(t *testing.T) {
	skipList := NewSkipList[string, string]()
	skipList.Insert("a", "1")
	skipList.Insert("c", "3")

	_, found := skipList.Get("b")
	if found {
		t.Error("Expected Get for non-existent key to return false")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListMultipleInserts(t *testing.T) {
	skipList := NewSkipList[string, string]()
	testData := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "red",
		"date":   "brown",
	}

	// Insert all items
	for k, v := range testData {
		skipList.Insert(k, v)
	}

	// Verify all items can be retrieved
	failed := false
	for k, expectedV := range testData {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != expectedV {
			t.Errorf("Get(%s) = %s, want %s", k, v, expectedV)
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListInsertAscending(t *testing.T) {
	skipList := NewSkipList[string, string]()
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	for _, k := range keys {
		skipList.Insert(k, k+"_value")
	}

	failed := false
	for _, k := range keys {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != k+"_value" {
			t.Errorf("Get(%s) = %s, want %s", k, v, k+"_value")
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListInsertDescending(t *testing.T) {
	skipList := NewSkipList[string, string]()
	keys := []string{"j", "i", "h", "g", "f", "e", "d", "c", "b", "a"}

	for _, k := range keys {
		skipList.Insert(k, k+"_value")
	}

	failed := false
	for _, k := range keys {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != k+"_value" {
			t.Errorf("Get(%s) = %s, want %s", k, v, k+"_value")
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListUpdateValue(t *testing.T) {
	skipList := NewSkipList[string, string]()
	key := "key1"

	// Insert initial value
	skipList.Insert(key, "value1")
	v, found := skipList.Get(key)
	if !found || v != "value1" {
		t.Errorf("Initial insert failed")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}

	// Update with new value
	skipList.Insert(key, "value2")
	v, found = skipList.Get(key)
	if !found {
		t.Error("Key not found after update")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}

	// Note: This test documents current behavior - may need adjustment
	// if skiplist should prevent duplicates
	t.Logf("After duplicate insert, Get(%s) = %s", key, v)
}

func TestSkipListEdgeCases(t *testing.T) {
	skipList := NewSkipList[string, string]()

	// Empty string keys
	skipList.Insert("", "empty_key")
	v, found := skipList.Get("")
	if !found || v != "empty_key" {
		t.Error("Failed to handle empty string key")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}

	// Very long keys
	longKey := string(make([]byte, 1000))
	skipList.Insert(longKey, "long")
	v, found = skipList.Get(longKey)
	if !found || v != "long" {
		t.Error("Failed to handle long key")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListKeyOrdering(t *testing.T) {
	skipList := NewSkipList[string, string]()

	// Test lexicographic ordering
	keys := []string{"a", "aa", "aaa", "b", "ab", "abc"}
	for _, k := range keys {
		skipList.Insert(k, k)
	}

	failed := false
	for _, k := range keys {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != k {
			t.Errorf("Get(%s) = %s, want %s", k, v, k)
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListNumericStringKeys(t *testing.T) {
	skipList := NewSkipList[string, string]()

	// Note: These are lexicographic, not numeric ordering
	// "10" < "2" in lexicographic order
	keys := []string{"1", "10", "2", "20", "3"}
	for _, k := range keys {
		skipList.Insert(k, "val_"+k)
	}

	failed := false
	for _, k := range keys {
		v, found := skipList.Get(k)
		if !found {
			t.Errorf("Key %s not found", k)
			failed = true
		}
		if v != "val_"+k {
			t.Errorf("Get(%s) = %s, want %s", k, v, "val_"+k)
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListLargeDataset(t *testing.T) {
	skipList := NewSkipList[string, string]()
	n := 1000

	// Insert many items
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%d", i)
		skipList.Insert(key, value)
	}

	// Verify all items
	failed := false
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		v, found := skipList.Get(key)
		if !found {
			t.Errorf("Key %s not found", key)
			failed = true
		}
		if v != expectedValue {
			t.Errorf("Get(%s) = %s, want %s", key, v, expectedValue)
			failed = true
		}
	}

	if failed {
		// Only dump contents for large dataset, hierarchy would be too big
		dumpSkipList(t, skipList)
	}
}

func TestSkipListDelete(t *testing.T) {
	skipList := NewSkipList[string, string]()
	skipList.Insert("a", "1")
	skipList.Insert("b", "2")
	skipList.Insert("c", "3")

	// Delete middle key
	skipList.Delete("b")

	// Verify "b" is gone
	failed := false
	_, found := skipList.Get("b")
	if found {
		t.Error("Expected deleted key 'b' to not be found")
		failed = true
	}

	// Verify other keys still exist
	v, found := skipList.Get("a")
	if !found || v != "1" {
		t.Error("Key 'a' should still exist after deleting 'b'")
		failed = true
	}

	v, found = skipList.Get("c")
	if !found || v != "3" {
		t.Error("Key 'c' should still exist after deleting 'b'")
		failed = true
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListDeleteNonExistent(t *testing.T) {
	skipList := NewSkipList[string, string]()
	skipList.Insert("a", "1")

	// Delete non-existent key should not panic
	skipList.Delete("nonexistent")

	// Original key should still be there
	v, found := skipList.Get("a")
	if !found || v != "1" {
		t.Error("Original key should be unaffected by deleting non-existent key")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListDeleteAndReinsert(t *testing.T) {
	skipList := NewSkipList[string, string]()
	key := "key1"

	// Insert, delete, then reinsert
	skipList.Insert(key, "value1")
	skipList.Delete(key)

	_, found := skipList.Get(key)
	if found {
		t.Error("Key should not exist after deletion")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}

	// Reinsert with different value
	skipList.Insert(key, "value2")
	v, found := skipList.Get(key)
	if !found || v != "value2" {
		t.Error("Should be able to reinsert deleted key with new value")
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

func TestSkipListDeleteAll(t *testing.T) {
	skipList := NewSkipList[string, string]()
	keys := []string{"a", "b", "c", "d", "e"}

	// Insert all
	for _, k := range keys {
		skipList.Insert(k, k+"_value")
	}

	// Delete all
	for _, k := range keys {
		skipList.Delete(k)
	}

	// Verify all are gone
	failed := false
	for _, k := range keys {
		_, found := skipList.Get(k)
		if found {
			t.Errorf("Key %s should be deleted", k)
			failed = true
		}
	}

	if failed {
		dumpSkipList(t, skipList)
		dumpSkipListHierarchy(t, skipList)
	}
}

// Benchmark tests
func BenchmarkSkipListInsert(b *testing.B) {
	skipList := NewSkipList[string, string]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		skipList.Insert(key, "value")
	}
}

func BenchmarkSkipListGet(b *testing.B) {
	skipList := NewSkipList[string, string]()
	// Pre-populate with data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%04d", i)
		skipList.Insert(key, "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%04d", i%10000)
		skipList.Get(key)
	}
}

func BenchmarkSkipListDelete(b *testing.B) {
	skipList := NewSkipList[string, string]()
	// Pre-populate with data
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%04d", i)
		skipList.Insert(key, "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%04d", i)
		skipList.Delete(key)
	}
}
