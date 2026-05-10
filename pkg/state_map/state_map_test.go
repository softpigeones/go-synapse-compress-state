// Copyright 2018 New Vector Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package state_map

import (
	"testing"
)

// TestStateMapNew tests creating a new state map
func TestStateMapNew(t *testing.T) {
	m := NewStateMap()

	if m.Len() != 0 {
		t.Errorf("Expected length 0, got %d", m.Len())
	}
}

// TestStateMapSetAndGet tests setting and getting values
func TestStateMapSetAndGet(t *testing.T) {
	m := NewStateMap()

	m.Set("type1", "key1", "value1")

	val, ok := m.Get("type1", "key1")
	if !ok {
		t.Error("Expected Get to return true")
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %v", val)
	}
}

// TestStateMapGetNotFound tests getting a non-existent key
func TestStateMapGetNotFound(t *testing.T) {
	m := NewStateMap()

	val, ok := m.Get("type1", "key1")
	if ok {
		t.Error("Expected Get to return false for non-existent key")
	}
	if val != "" {
		t.Errorf("Expected empty value, got %v", val)
	}
}

// TestStateMapContains tests the Contains method
func TestStateMapContains(t *testing.T) {
	m := NewStateMap()

	if m.Contains("type1", "key1") {
		t.Error("Expected Contains to return false for non-existent key")
	}

	m.Set("type1", "key1", "value1")

	if !m.Contains("type1", "key1") {
		t.Error("Expected Contains to return true for existing key")
	}
}

// TestStateMapLen tests the Len method
func TestStateMapLen(t *testing.T) {
	m := NewStateMap()

	if m.Len() != 0 {
		t.Errorf("Expected length 0, got %d", m.Len())
	}

	m.Set("type1", "key1", "value1")
	if m.Len() != 1 {
		t.Errorf("Expected length 1, got %d", m.Len())
	}

	m.Set("type1", "key2", "value2")
	if m.Len() != 2 {
		t.Errorf("Expected length 2, got %d", m.Len())
	}

	// Overwriting should not increase length
	m.Set("type1", "key1", "new_value")
	if m.Len() != 2 {
		t.Errorf("Expected length still 2 after overwrite, got %d", m.Len())
	}
}

// TestStateMapIterator tests iterating over the map
func TestStateMapIterator(t *testing.T) {
	m := NewStateMap()

	m.Set("type1", "key1", "value1")
	m.Set("type1", "key2", "value2")
	m.Set("type2", "key1", "value3")

	count := 0
	iter := m.Iterator()
	for iter.Next() {
		count++
		key := iter.Key()
		_ = key
		value := iter.Value()
		_ = value
	}

	if count != 3 {
		t.Errorf("Expected to iterate 3 items, got %d", count)
	}
}

// TestStateMapCopy tests copying a state map
func TestStateMapCopy(t *testing.T) {
	m := NewStateMap()
	m.Set("type1", "key1", "value1")
	m.Set("type2", "key2", "value2")

	copy := m.Copy()

	if copy.Len() != m.Len() {
		t.Errorf("Expected copy length %d, got %d", m.Len(), copy.Len())
	}

	val, ok := copy.Get("type1", "key1")
	if !ok || val != "value1" {
		t.Errorf("Expected copy to have value1, got %v (ok=%v)", val, ok)
	}

	// Modify original - copy should not change
	m.Set("type1", "key1", "modified")
	val, ok = copy.Get("type1", "key1")
	if !ok || val != "value1" {
		t.Errorf("Expected copy to still have value1, got %v (ok=%v)", val, ok)
	}
}

// TestStateMapRange tests the Range method
func TestStateMapRange(t *testing.T) {
	m := NewStateMap()
	m.Set("type1", "key1", "value1")
	m.Set("type1", "key2", "value2")

	count := 0
	m.Range(func(key Key, value string) bool {
		count++
		return true
	})

	if count != 2 {
		t.Errorf("Expected to range over 2 items, got %d", count)
	}
}

// TestStateMapRangeEarlyExit tests that Range can exit early
func TestStateMapRangeEarlyExit(t *testing.T) {
	m := NewStateMap()
	m.Set("type1", "key1", "value1")
	m.Set("type1", "key2", "value2")
	m.Set("type1", "key3", "value3")

	count := 0
	m.Range(func(key Key, value string) bool {
		count++
		return count < 2 // Stop after 2 items
	})

	if count != 2 {
		t.Errorf("Expected to range over 2 items before exit, got %d", count)
	}
}
