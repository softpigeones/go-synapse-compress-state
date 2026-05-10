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
	"fmt"
	"sort"

	"synapse_compress_state/pkg/state_map"
)

// StateGroupEntry is an entry for a state group. Consists of an (optional) previous group and the
// delta from that previous group (or the full state if no previous group)
type StateGroupEntry struct {
	InRange        bool
	PrevStateGroup *int64
	StateMap       *state_map.StateMap
}

// Stats keeps track of some statistics of a compression run.
type Stats struct {
	// How many state groups we couldn't find a delta for, despite trying.
	ResetsNoSuitablePrev int
	// The sum of the rows of the state groups counted by ResetsNoSuitablePrev.
	ResetsNoSuitablePrevSize int
	// How many state groups we have changed.
	StateGroupsChanged int
}

// Compressor attempts to compress a set of state deltas using the given level sizes.
type Compressor struct {
	OriginalStateMap map[int64]*StateGroupEntry
	NewStateGroupMap map[int64]*StateGroupEntry
	Levels           []*Level
	Stats            *Stats
}

// NewCompressor creates a compressor and runs the compression algorithm.
func NewCompressor(originalStateMap map[int64]*StateGroupEntry, levelSizes []int) *Compressor {
	levels := make([]*Level, len(levelSizes))
	for i, size := range levelSizes {
		levels[i] = NewLevel(size)
	}

	c := &Compressor{
		OriginalStateMap: originalStateMap,
		NewStateGroupMap: make(map[int64]*StateGroupEntry),
		Levels:           levels,
		Stats:            &Stats{},
	}

	c.CreateNewTree()

	return c
}

// NewCompressorFromSave creates a compressor and runs the compression algorithm.
// Used when restoring compressor state from a previous run in which case the levels heads are also known.
func NewCompressorFromSave(originalStateMap map[int64]*StateGroupEntry, levelInfo []*Level) *Compressor {
	levels := make([]*Level, len(levelInfo))
	for i, l := range levelInfo {
		levels[i] = RestoreLevel(l.MaxLength, l.CurrentChainLength, l.Head)
	}

	c := &Compressor{
		OriginalStateMap: originalStateMap,
		NewStateGroupMap: make(map[int64]*StateGroupEntry),
		Levels:           levels,
		Stats:            &Stats{},
	}

	c.CreateNewTree()

	return c
}

// GetLevelInfo returns all the state required to save the compressor so it can be continued later.
func (c *Compressor) GetLevelInfo() []*Level {
	return c.Levels
}

// CreateNewTree actually runs the compression algorithm.
func (c *Compressor) CreateNewTree() {
	if len(c.NewStateGroupMap) != 0 {
		panic("Can only call `CreateNewTree` once")
	}

	// Sort the state group keys
	keys := make([]int64, 0, len(c.OriginalStateMap))
	for k := range c.OriginalStateMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, stateGroup := range keys {
		entry := c.OriginalStateMap[stateGroup]

		// Check whether this entry is in_range or is just present in the map due to being
		// a predecessor of a group that IS in_range for compression
		if !entry.InRange {
			newEntry := &StateGroupEntry{
				// InRange is kept the same so that the new entry is equal to the old entry
				// otherwise it might trigger a useless database transaction
				InRange:        entry.InRange,
				PrevStateGroup: entry.PrevStateGroup,
				StateMap:       entry.StateMap.Copy(),
			}
			// Paranoidly assert that not making changes to this entry
			// could probably be removed...
			if !entriesEqual(newEntry, entry) {
				panic("Entry mismatch")
			}
			c.NewStateGroupMap[stateGroup] = newEntry
			continue
		}

		var prevStateGroup *int64
		for _, level := range c.Levels {
			if level.HasSpace() {
				prevStateGroup = level.GetHead()
				level.Update(stateGroup, true)
				break
			} else {
				level.Update(stateGroup, false)
			}
		}

		var delta *state_map.StateMap
		var actualPrevStateGroup *int64

		if entriesHaveSamePrev(entry, prevStateGroup) {
			delta = entry.StateMap.Copy()
			actualPrevStateGroup = prevStateGroup
		} else {
			c.Stats.StateGroupsChanged++
			delta, actualPrevStateGroup = c.getDelta(prevStateGroup, stateGroup)
		}

		c.NewStateGroupMap[stateGroup] = &StateGroupEntry{
			InRange:        true,
			PrevStateGroup: actualPrevStateGroup,
			StateMap:       delta,
		}
	}
}

func entriesEqual(a, b *StateGroupEntry) bool {
	if a.InRange != b.InRange {
		return false
	}
	if (a.PrevStateGroup == nil) != (b.PrevStateGroup == nil) {
		return false
	}
	if a.PrevStateGroup != nil && b.PrevStateGroup != nil && *a.PrevStateGroup != *b.PrevStateGroup {
		return false
	}
	return stateMapsEqual(a.StateMap, b.StateMap)
}

func entriesHaveSamePrev(entry *StateGroupEntry, prevStateGroup *int64) bool {
	if (entry.PrevStateGroup == nil) != (prevStateGroup == nil) {
		return false
	}
	if entry.PrevStateGroup != nil && prevStateGroup != nil {
		return *entry.PrevStateGroup == *prevStateGroup
	}
	return true
}

func stateMapsEqual(a, b *state_map.StateMap) bool {
	if a.Len() != b.Len() {
		return false
	}
	equal := true
	a.Range(func(key state_map.Key, value string) bool {
		bVal, ok := b.Get(key.Type, key.StateKey)
		if !ok || bVal != value {
			equal = false
			return false
		}
		return true
	})
	return equal
}

// getDelta attempts to calculate the delta between two state groups.
//
// This is not always possible if the given candidate previous state group
// have state keys that are not in the new state group. In this case the
// function will try and iterate back up the current tree to find a state
// group that can be used as a base for a delta.
//
// Returns the state map and the actual base state group (if any) used.
func (c *Compressor) getDelta(prevSG *int64, sg int64) (*state_map.StateMap, *int64) {
	stateMap := CollapseStateMaps(c.OriginalStateMap, sg)

	if prevSG == nil {
		return stateMap, nil
	}

	// This is a loop to go through to find the first prev_sg which can be
	// a valid base for the state group.
	currentPrevSG := *prevSG
	var prevStateMap *state_map.StateMap

outer:
	for {
		prevStateMap = CollapseStateMaps(c.OriginalStateMap, currentPrevSG)

		iter := prevStateMap.Iterator()
		for iter.Next() {
			key := iter.Key()
			if !stateMap.Contains(key.Type, key.StateKey) {
				// This is not a valid base as it contains key the new state
				// group doesn't have. Attempt to walk up the tree to find a
				// better base.
				if c.NewStateGroupMap[currentPrevSG].PrevStateGroup != nil {
					currentPrevSG = *c.NewStateGroupMap[currentPrevSG].PrevStateGroup
					continue outer
				}

				// Couldn't find a new base, so we give up and just persist
				// a full state group here.
				c.Stats.ResetsNoSuitablePrev++
				c.Stats.ResetsNoSuitablePrevSize += stateMap.Len()

				return stateMap, nil
			}
		}

		break
	}

	// We've found a valid base, now we just need to calculate the delta.
	deltaMap := state_map.NewStateMap()

	iter := stateMap.Iterator()
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		prevVal, ok := prevStateMap.Get(key.Type, key.StateKey)
		if !ok || prevVal != value {
			deltaMap.Set(key.Type, key.StateKey, value)
		}
	}

	return deltaMap, &currentPrevSG
}

// CollapseStateMaps gets the full state for a given group from the map (of deltas).
func CollapseStateMaps(m map[int64]*StateGroupEntry, stateGroup int64) *state_map.StateMap {
	entry, ok := m[stateGroup]
	if !ok {
		panic(fmt.Sprintf("Missing %d", stateGroup))
	}

	stateMap := state_map.NewStateMap()
	stack := []int64{stateGroup}

	for entry.PrevStateGroup != nil {
		prevSG := *entry.PrevStateGroup
		stack = append(stack, prevSG)
		nextEntry, ok := m[prevSG]
		if !ok {
			panic(fmt.Sprintf("Missing %d", prevSG))
		}
		entry = nextEntry
	}

	// Iterate through stack in reverse
	for i := len(stack) - 1; i >= 0; i-- {
		sg := stack[i]
		entry := m[sg]
		iter := entry.StateMap.Iterator()
		for iter.Next() {
			key := iter.Key()
			value := iter.Value()
			stateMap.Set(key.Type, key.StateKey, value)
		}
	}

	return stateMap
}
