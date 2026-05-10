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

package manager

import (
"testing"

"synapse_compress_state/pkg/compressor"
"synapse_compress_state/pkg/database"
"synapse_compress_state/pkg/state_map"
)

// TestCheckThatMapsMatch tests that CheckThatMapsMatch correctly validates
// that two state group mappings produce the same collapsed state.
func TestCheckThatMapsMatch(t *testing.T) {
// Create two identical maps
oldMap := make(map[int64]*database.StateGroupEntry)
newMap := make(map[int64]*database.StateGroupEntry)

sm := state_map.NewStateMap()
sm.Set("m.room.create", "", "event1")

oldMap[1] = &database.StateGroupEntry{
InRange:        true,
PrevStateGroup: nil,
StateMap:       sm,
}

newMap[1] = &database.StateGroupEntry{
InRange:        true,
PrevStateGroup: nil,
StateMap:       sm,
}

// Should not panic
CheckThatMapsMatch(oldMap, newMap)
}

// TestCheckThatMapsMatchDifferent tests that CheckThatMapsMatch panics
// when two state group mappings produce different collapsed states.
func TestCheckThatMapsMatchDifferent(t *testing.T) {
defer func() {
if r := recover(); r == nil {
t.Errorf("CheckThatMapsMatch did not panic on different maps")
}
}()

// Create two different maps
oldMap := make(map[int64]*database.StateGroupEntry)
newMap := make(map[int64]*database.StateGroupEntry)

sm1 := state_map.NewStateMap()
sm1.Set("m.room.create", "", "event1")

sm2 := state_map.NewStateMap()
sm2.Set("m.room.create", "", "event2")

oldMap[1] = &database.StateGroupEntry{
InRange:        true,
PrevStateGroup: nil,
StateMap:       sm1,
}

newMap[1] = &database.StateGroupEntry{
InRange:        true,
PrevStateGroup: nil,
StateMap:       sm2,
}

CheckThatMapsMatch(oldMap, newMap)
}

// TestChunkStats tests the ChunkStats struct.
func TestChunkStats(t *testing.T) {
stats := &ChunkStats{
NewLevelInfo: []*compressor.Level{
compressor.NewLevel(100),
compressor.NewLevel(50),
compressor.NewLevel(25),
},
LastCompressedGroup: 42,
OriginalNumRows:     1000,
NewNumRows:          500,
Commited:            true,
}

if stats.LastCompressedGroup != 42 {
t.Errorf("Expected LastCompressedGroup to be 42, got %d", stats.LastCompressedGroup)
}

if stats.OriginalNumRows != 1000 {
t.Errorf("Expected OriginalNumRows to be 1000, got %d", stats.OriginalNumRows)
}

if stats.NewNumRows != 500 {
t.Errorf("Expected NewNumRows to be 500, got %d", stats.NewNumRows)
}

if !stats.Commited {
t.Error("Expected Commited to be true")
}

savings := stats.OriginalNumRows - stats.NewNumRows
if savings != 500 {
t.Errorf("Expected savings to be 500, got %d", savings)
}
}
