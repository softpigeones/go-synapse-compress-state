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

package compressor

import (
	"testing"

	"synapse_compress_state/pkg/state_map"
)

// TestLevelNew tests creating a new level
func TestLevelNew(t *testing.T) {
	level := NewLevel(100)

	if level.MaxLength != 100 {
		t.Errorf("Expected MaxLength to be 100, got %d", level.MaxLength)
	}
	if level.CurrentChainLength != 0 {
		t.Errorf("Expected CurrentChainLength to be 0, got %d", level.CurrentChainLength)
	}
	if level.Head != nil {
		t.Errorf("Expected Head to be nil, got %v", level.Head)
	}
	if !level.HasSpace() {
		t.Error("Expected HasSpace to return true for new level")
	}
}

// TestLevelRestore tests restoring a level from saved state
func TestLevelRestore(t *testing.T) {
	head := int64(42)
	level := RestoreLevel(50, 10, &head)

	if level.MaxLength != 50 {
		t.Errorf("Expected MaxLength to be 50, got %d", level.MaxLength)
	}
	if level.CurrentChainLength != 10 {
		t.Errorf("Expected CurrentChainLength to be 10, got %d", level.CurrentChainLength)
	}
	if level.Head == nil || *level.Head != 42 {
		t.Errorf("Expected Head to be 42, got %v", level.Head)
	}
}

// TestLevelUpdateWithDelta tests updating a level with a delta
func TestLevelUpdateWithDelta(t *testing.T) {
	level := NewLevel(100)

	level.Update(1, true)

	if level.Head == nil || *level.Head != 1 {
		t.Errorf("Expected Head to be 1, got %v", level.Head)
	}
	if level.CurrentChainLength != 1 {
		t.Errorf("Expected CurrentChainLength to be 1, got %d", level.CurrentChainLength)
	}
}

// TestLevelUpdateWithoutDelta tests updating a level without a delta (new chain)
func TestLevelUpdateWithoutDelta(t *testing.T) {
	level := NewLevel(100)

	// First update with delta
	level.Update(1, true)
	// Then update without delta (start new chain)
	level.Update(2, false)

	if level.Head == nil || *level.Head != 2 {
		t.Errorf("Expected Head to be 2, got %v", level.Head)
	}
	if level.CurrentChainLength != 1 {
		t.Errorf("Expected CurrentChainLength to be 1 after starting new chain, got %d", level.CurrentChainLength)
	}
}

// TestLevelHasSpace tests the HasSpace method
func TestLevelHasSpace(t *testing.T) {
	level := NewLevel(3)

	if !level.HasSpace() {
		t.Error("Expected HasSpace to return true initially")
	}

	level.Update(1, true)
	if !level.HasSpace() {
		t.Error("Expected HasSpace to return true after first insert")
	}

	level.Update(2, true)
	if !level.HasSpace() {
		t.Error("Expected HasSpace to return true after second insert")
	}

	level.Update(3, true)
	if level.HasSpace() {
		t.Error("Expected HasSpace to return false when level is full")
	}
}

// TestLevelGetters tests the getter methods
func TestLevelGetters(t *testing.T) {
	head := int64(99)
	level := RestoreLevel(75, 25, &head)

	if level.GetMaxLength() != 75 {
		t.Errorf("Expected GetMaxLength to return 75, got %d", level.GetMaxLength())
	}
	if level.GetCurrentLength() != 25 {
		t.Errorf("Expected GetCurrentLength to return 25, got %d", level.GetCurrentLength())
	}
	if level.GetHead() == nil || *level.GetHead() != 99 {
		t.Errorf("Expected GetHead to return 99, got %v", level.GetHead())
	}
}

// TestCollapseStateMaps tests collapsing state maps with deltas
func TestCollapseStateMaps(t *testing.T) {
	// Create a simple chain: 0 -> 1 -> 2
	stateMap := make(map[int64]*StateGroupEntry)

	// Group 0 has no predecessor (snapshot)
	entry0 := &StateGroupEntry{
		InRange:        true,
		PrevStateGroup: nil,
		StateMap:       state_map.NewStateMap(),
	}
	entry0.StateMap.Set("type1", "key0", "value0")
	stateMap[0] = entry0

	// Group 1 references group 0
	entry1 := &StateGroupEntry{
		InRange:        true,
		PrevStateGroup: ptrToInt64(0),
		StateMap:       state_map.NewStateMap(),
	}
	entry1.StateMap.Set("type1", "key1", "value1")
	stateMap[1] = entry1

	// Group 2 references group 1
	entry2 := &StateGroupEntry{
		InRange:        true,
		PrevStateGroup: ptrToInt64(1),
		StateMap:       state_map.NewStateMap(),
	}
	entry2.StateMap.Set("type1", "key2", "value2")
	stateMap[2] = entry2

	// Collapse group 2 - should have all three entries
	collapsed := CollapseStateMaps(stateMap, 2)

	if collapsed.Len() != 3 {
		t.Errorf("Expected collapsed map to have 3 entries, got %d", collapsed.Len())
	}

	val, ok := collapsed.Get("type1", "key0")
	if !ok || val != "value0" {
		t.Errorf("Expected key0 to have value0, got %v (ok=%v)", val, ok)
	}

	val, ok = collapsed.Get("type1", "key1")
	if !ok || val != "value1" {
		t.Errorf("Expected key1 to have value1, got %v (ok=%v)", val, ok)
	}

	val, ok = collapsed.Get("type1", "key2")
	if !ok || val != "value2" {
		t.Errorf("Expected key2 to have value2, got %v (ok=%v)", val, ok)
	}
}

// TestCollapseStateMapsOverwrite tests that later deltas overwrite earlier ones
func TestCollapseStateMapsOverwrite(t *testing.T) {
	stateMap := make(map[int64]*StateGroupEntry)

	// Group 0 has initial value
	entry0 := &StateGroupEntry{
		InRange:        true,
		PrevStateGroup: nil,
		StateMap:       state_map.NewStateMap(),
	}
	entry0.StateMap.Set("type1", "key", "old_value")
	stateMap[0] = entry0

	// Group 1 overwrites the value
	entry1 := &StateGroupEntry{
		InRange:        true,
		PrevStateGroup: ptrToInt64(0),
		StateMap:       state_map.NewStateMap(),
	}
	entry1.StateMap.Set("type1", "key", "new_value")
	stateMap[1] = entry1

	collapsed := CollapseStateMaps(stateMap, 1)

	val, ok := collapsed.Get("type1", "key")
	if !ok || val != "new_value" {
		t.Errorf("Expected key to have new_value, got %v (ok=%v)", val, ok)
	}
}

// TestCompressorBasic tests basic compression
func TestCompressorBasic(t *testing.T) {
	// Create a simple linear chain
	stateMap := make(map[int64]*StateGroupEntry)

	for i := int64(0); i < 5; i++ {
		var prev *int64
		if i > 0 {
			prev = ptrToInt64(i - 1)
		}
		entry := &StateGroupEntry{
			InRange:        true,
			PrevStateGroup: prev,
			StateMap:       state_map.NewStateMap(),
		}
		entry.StateMap.Set("type", "key", string(rune('a'+i)))
		stateMap[i] = entry
	}

	// Compress with level sizes [2, 2]
	compressor := NewCompressor(stateMap, []int{2, 2})

	// Check that we got results
	if len(compressor.NewStateGroupMap) != 5 {
		t.Errorf("Expected 5 entries in new state group map, got %d", len(compressor.NewStateGroupMap))
	}

	// Verify that states match after compression
	for sg := range stateMap {
		expected := CollapseStateMaps(stateMap, sg)
		actual := CollapseStateMaps(compressor.NewStateGroupMap, sg)

		if expected.Len() != actual.Len() {
			t.Errorf("State group %d: expected length %d, got %d", sg, expected.Len(), actual.Len())
		}

		iter := expected.Iterator()
		for iter.Next() {
			key := iter.Key()
			value := iter.Value()
			actualVal, ok := actual.Get(key.Type, key.StateKey)
			if !ok || actualVal != value {
				t.Errorf("State group %d: key %v expected %v, got %v (ok=%v)", sg, key, value, actualVal, ok)
			}
		}
	}
}

// Helper function to create int64 pointer
func ptrToInt64(v int64) *int64 {
	return &v
}
