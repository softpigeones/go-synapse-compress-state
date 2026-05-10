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
	"context"
	"fmt"

	"synapse_compress_state/pkg/compressor"
	"synapse_compress_state/pkg/database"
	"synapse_compress_state/pkg/state_saving"
)

// ChunkStats contains information about what compressor did to chunk that it was ran on.
type ChunkStats struct {
	// The state of each of the levels of the compressor when it stopped
	NewLevelInfo []*compressor.Level
	// The last state_group that was compressed
	// (to continue from where the compressor stopped, call with this as 'start' value)
	LastCompressedGroup int64
	// The number of rows in the database for the current chunk of state_groups before compressing
	OriginalNumRows int
	// The number of rows in the database for the current chunk of state_groups after compressing
	NewNumRows int
	// Whether or not the changes were commited to the database
	Commited bool
}

// RunCompressorOnRoomChunk runs the compressor on a chunk of the room.
//
// Returns `Some(chunk_stats)` if the compressor has progressed
// and `None` if it had already got to the end of the room.
//
// Arguments:
//   - dbURL: The URL of the postgres database that synapse is using.
//     e.g. "postgresql://user:password@domain.com/synapse"
//   - roomID: The id of the room to run the compressor on. Note this
//     is the id as stored in the database and will look like
//     "!aasdfasdfafdsdsa:matrix.org" instead of the common name
//   - chunkSize: The number of state_groups to work on. All of the entries
//     from state_groups_state are requested from the database
//     for state groups that are worked on. Therefore small
//     chunk sizes may be needed on machines with low memory.
//     (Note: if the compressor fails to find space savings on the
//     chunk as a whole (which may well happen in rooms with lots
//     of backfill in) then the entire chunk is skipped.)
//   - defaultLevels: If the compressor has never been run on this room before
//     then we need to provide the compressor with some information
//     on what sort of compression structure we want. The default that
//     the library suggests is []Level{Level::new(100), Level::new(50), Level::new(25)}
func RunCompressorOnRoomChunk(dbURL string, roomID string, chunkSize int64, defaultLevels []*compressor.Level) (*ChunkStats, error) {
	ctx := context.Background()

	// connect to the database
	client, err := state_saving.ConnectToDatabase(dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", dbURL, err)
	}
	defer client.Close(ctx)

	// Access the database to find out where the compressor last got up to
	start, levelInfo, err := state_saving.ReadRoomCompressorState(ctx, client, roomID)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressor state for room %s: %w", roomID, err)
	}

	// If the database didn't contain any information, then use the default state
	if start == nil || levelInfo == nil {
		start = nil
		levelInfo = defaultLevels
	}

	// run the compressor on this chunk
	chunkStats := ContinueRun(start, chunkSize, dbURL, roomID, levelInfo)

	if chunkStats == nil {
		return nil, nil
	}

	// Check to see whether the compressor sent its changes to the database
	if !chunkStats.Commited {
		if chunkStats.NewNumRows-chunkStats.OriginalNumRows != 0 {
			// Skip over the failed chunk and set the level info to the default (empty) state
			err := state_saving.WriteRoomCompressorState(ctx, client, roomID, defaultLevels, chunkStats.LastCompressedGroup)
			if err != nil {
				return nil, fmt.Errorf("failed to skip chunk in room %s between %v and %d: %w", roomID, start, chunkStats.LastCompressedGroup, err)
			}
		} else {
			// Skip over the failed chunk and set the level info to the default (empty) state
			err := state_saving.WriteRoomCompressorState(ctx, client, roomID, defaultLevels, chunkStats.LastCompressedGroup)
			if err != nil {
				return nil, fmt.Errorf("failed to skip chunk in room %s between %v and %d: %w", roomID, start, chunkStats.LastCompressedGroup, err)
			}
		}

		return chunkStats, nil
	}

	// Save where we got up to after this successful commit
	err = state_saving.WriteRoomCompressorState(ctx, client, roomID, chunkStats.NewLevelInfo, chunkStats.LastCompressedGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to save state after compressing chunk in room %s between %v and %d: %w", roomID, start, chunkStats.LastCompressedGroup, err)
	}

	return chunkStats, nil
}

// CompressChunksOfDatabase runs the compressor in chunks on rooms with the lowest uncompressed state group ids.
//
// Arguments:
//   - dbURL: The URL of the postgres database that synapse is using.
//     e.g. "postgresql://user:password@domain.com/synapse"
//   - chunkSize: The number of state_groups to work on. All of the entries
//     from state_groups_state are requested from the database
//     for state groups that are worked on. Therefore small
//     chunk sizes may be needed on machines with low memory.
//     (Note: if the compressor fails to find space savings on the
//     chunk as a whole (which may well happen in rooms with lots
//     of backfill in) then the entire chunk is skipped.)
//   - defaultLevels: If the compressor has never been run on this room before
//     Then we need to provide the compressor with some information
//     on what sort of compression structure we want. The default that
//     the library suggests is empty levels with max sizes of 100, 50 and 25
//   - numberOfChunks: The number of chunks to compress. The larger this number is, the longer
//     the compressor will run for.
func CompressChunksOfDatabase(dbURL string, chunkSize int64, defaultLevels []*compressor.Level, numberOfChunks int64) error {
	ctx := context.Background()

	// connect to the database
	client, err := state_saving.ConnectToDatabase(dbURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database at %s: %w", dbURL, err)
	}
	defer client.Close(ctx)

	err = state_saving.CreateTablesIfNeeded(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to create state compressor tables: %w", err)
	}

	skippedChunks := 0
	rowsSaved := 0
	chunksProcessed := int64(0)

	for chunksProcessed < numberOfChunks {
		roomToCompress, err := state_saving.GetNextRoomToCompress(ctx, client)
		if err != nil {
			return fmt.Errorf("failed to work out what room to compress next: %w", err)
		}

		if roomToCompress == nil {
			break
		}

		fmt.Printf("Running compressor on room %s with chunk size %d\n", *roomToCompress, chunkSize)

		workDone, err := RunCompressorOnRoomChunk(dbURL, *roomToCompress, chunkSize, defaultLevels)
		if err != nil {
			return err
		}

		if workDone != nil {
			if workDone.Commited {
				savings := workDone.OriginalNumRows - workDone.NewNumRows
				rowsSaved += savings
			} else {
				skippedChunks++
			}
			chunksProcessed++
		} else {
			return fmt.Errorf("ran the compressor on a room that had no more work to do!")
		}
	}

	fmt.Printf("Finished running compressor. Saved %d rows. Skipped %d/%d chunks\n", rowsSaved, skippedChunks, chunksProcessed)
	return nil
}

// ContinueRun loads a compressor state, runs it on a room and then returns info on how it got on.
func ContinueRun(start *int64, chunkSize int64, dbURL string, roomID string, levelInfo []*compressor.Level) *ChunkStats {
	ctx := context.Background()

	// First we need to get the current state groups
	// If nothing was found then return nil
	dbStateGroupMap, maxGroupFound, found := database.ReloadDataFromDB(ctx, dbURL, roomID, start, &chunkSize, levelInfo)
	if !found {
		return nil
	}

	originalNumRows := 0
	for _, v := range dbStateGroupMap {
		originalNumRows += v.StateMap.Len()
	}

	// Convert database StateGroupEntry to compressor StateGroupEntry
	stateGroupMap := make(map[int64]*compressor.StateGroupEntry)
	for k, v := range dbStateGroupMap {
		stateGroupMap[k] = &compressor.StateGroupEntry{
			InRange:        v.InRange,
			PrevStateGroup: v.PrevStateGroup,
			StateMap:       v.StateMap,
		}
	}

	// Now we actually call the compression algorithm.
	compressorObj := compressor.NewCompressorFromSave(stateGroupMap, levelInfo)
	newStateGroupMap := compressorObj.NewStateGroupMap

	// Done! Now to print a bunch of stats.
	newNumRows := 0
	for _, v := range newStateGroupMap {
		newNumRows += v.StateMap.Len()
	}

	ratio := float64(newNumRows) / float64(originalNumRows)

	if ratio > 1.0 {
		return &ChunkStats{
			NewLevelInfo:        compressorObj.GetLevelInfo(),
			LastCompressedGroup: maxGroupFound,
			OriginalNumRows:     originalNumRows,
			NewNumRows:          newNumRows,
			Commited:            false,
		}
	}

	// Check that maps match (convert back to database format for comparison)
	dbNewStateGroupMap := make(map[int64]*database.StateGroupEntry)
	for k, v := range newStateGroupMap {
		dbNewStateGroupMap[k] = &database.StateGroupEntry{
			InRange:        v.InRange,
			PrevStateGroup: v.PrevStateGroup,
			StateMap:       v.StateMap,
		}
	}
	CheckThatMapsMatch(dbStateGroupMap, dbNewStateGroupMap)

	// Send changes to database
	database.SendChangesToDB(ctx, dbURL, roomID, dbStateGroupMap, dbNewStateGroupMap)

	return &ChunkStats{
		NewLevelInfo:        compressorObj.GetLevelInfo(),
		LastCompressedGroup: maxGroupFound,
		OriginalNumRows:     originalNumRows,
		NewNumRows:          newNumRows,
		Commited:            true,
	}
}

// CheckThatMapsMatch compares two sets of state groups.
//
// A state group entry contains a predecessor state group and a delta.
// The complete contents of a certain state group can be calculated by
// following this chain of predecessors back to some empty state and
// combining all the deltas together. This is called "collapsing".
//
// This function confirms that two state groups mappings lead to the
// exact same entries for each state group after collapsing them down.
//
// Arguments:
//   - oldMap: The state group data currently in the database
//   - newMap: The state group data that the oldMap is being compared to
func CheckThatMapsMatch(oldMap, newMap map[int64]*database.StateGroupEntry) {
	// Convert to compressor format
	oldMapConverted := make(map[int64]*compressor.StateGroupEntry)
	for k, v := range oldMap {
		oldMapConverted[k] = &compressor.StateGroupEntry{
			InRange:        v.InRange,
			PrevStateGroup: v.PrevStateGroup,
			StateMap:       v.StateMap,
		}
	}
	newMapConverted := make(map[int64]*compressor.StateGroupEntry)
	for k, v := range newMap {
		newMapConverted[k] = &compressor.StateGroupEntry{
			InRange:        v.InRange,
			PrevStateGroup: v.PrevStateGroup,
			StateMap:       v.StateMap,
		}
	}

	// Now let's iterate through and assert that the state for each group
	// matches between the two versions.
	for sg := range oldMapConverted {
		expected := compressor.CollapseStateMaps(oldMapConverted, sg)
		actual := compressor.CollapseStateMaps(newMapConverted, sg)

		if expected.Len() != actual.Len() {
			panic(fmt.Sprintf("States for group %d do not match. Expected length %d, found length %d", sg, expected.Len(), actual.Len()))
		}

		// Check all keys match
		iter := expected.Iterator()
		for iter.Next() {
			key := iter.Key()
			value := iter.Value()
			actualVal, ok := actual.Get(key.Type, key.StateKey)
			if !ok || actualVal != value {
				panic(fmt.Sprintf("States for group %d do not match. Expected %v=%v, found %v=%v", sg, key.Type, value, key.Type, actualVal))
			}
		}
	}
}
