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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"synapse_compress_state/pkg/compressor"
	"synapse_compress_state/pkg/database"
	"synapse_compress_state/pkg/graphing"
	"synapse_compress_state/pkg/state_map"
)

// Config contains configuration information for this run of the compressor.
type Config struct {
	// The URL for the postgres database
	// This should be of the form postgresql://user:pass@domain/database
	DBURL string
	// The file where the transactions are written that would carry out
	// the compression that get's calculated
	OutputFile *os.File
	// The ID of the room who's state is being compressed
	RoomID string
	// The group to start compressing from
	// N.B. THIS STATE ITSELF IS NOT COMPRESSED!!!
	// Note there is no state 0 so if want to compress all then can enter 0
	// (this is the same as leaving it blank)
	MinStateGroup int64
	// How many groups to do the compression on
	// Note: State groups within the range specified will get compressed
	// if they are in the state_groups table. States that only appear in
	// the edges table MIGHT NOT get compressed - it is assumed that these
	// groups have no associated state. (Note that this was also an assumption
	// in previous versions of the state compressor, and would only be a problem
	// if the database was in a bad way already...)
	GroupsToCompress int64
	// If the compressor results in less than this many rows being saved then
	// it will abort
	MinSavedRows int64
	// If a max_state_group is specified then only state groups with id's lower
	// than this number are able to be compressed.
	MaxStateGroup int64
	// The sizes of the different levels in the new state_group tree being built
	LevelSizes []int
	// Whether or not to wrap each change to an individual state_group in a transaction
	// This is very much recommended when running the compression when synapse is live
	Transactions bool
	// Whether or not to output before and after directed graphs (these can be
	// visualised in something like Gephi)
	Graphs bool
	// Whether or not to commit changes to the database automatically
	// N.B. currently assumes transactions is true (to be on the safe side)
	CommitChanges bool
	// Whether to verify the correctness of the compressed state groups by
	// comparing them to the original groups
	Verify bool
}

// LevelSizes is a helper struct for parsing the level_sizes argument.
type LevelSizes []int

func parseLevelSizes(s string) (LevelSizes, error) {
	parts := strings.Split(s, ",")
	sizes := make([]int, len(parts))
	for i, part := range parts {
		size, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("not a comma separated list of numbers: %v", err)
		}
		sizes[i] = size
	}
	return LevelSizes(sizes), nil
}

func parseArgs() *Config {
	config := &Config{}
	var outputFile string

	flag.StringVar(&config.DBURL, "p", "", "The configuration for connecting to the Postgres database. This should be of the form \"postgresql://username:password@mydomain.com/database\" or a key-value pair string: \"user=username password=password dbname=database host=mydomain.com\"")
	flag.StringVar(&config.RoomID, "r", "", "The room to process. This is the value found in the rooms table of the database, not the common name for the room - it should look like: \"!wOlkWNmgkAZFxbTaqj:matrix.org\"")
	flag.Int64Var(&config.MinStateGroup, "b", 0, "The state group to start processing from (non inclusive)")
	flag.Int64Var(&config.MinSavedRows, "m", 0, "Abort if fewer than COUNT rows would be saved")
	flag.Int64Var(&config.GroupsToCompress, "n", 0, "How many groups to load into memory to compress (starting from the 1st group in the room or the group specified by -s)")
	flag.StringVar(&outputFile, "o", "", "File to output the changes to in SQL")
	flag.Int64Var(&config.MaxStateGroup, "s", 0, "The maximum state group to process up to")
	levelSizesStr := flag.String("l", "100,50,25", "Sizes of each new level in the compression algorithm, as a comma separated list.")
	flag.BoolVar(&config.Transactions, "t", false, "Whether to wrap each state group change in a transaction")
	flag.BoolVar(&config.Graphs, "g", false, "Output before and after graphs")
	flag.BoolVar(&config.CommitChanges, "c", false, "Commit changes to the database")
	noVerify := flag.Bool("N", false, "Do not double-check that the compression was performed correctly")

	flag.Parse()

	if config.DBURL == "" {
		log.Fatal("postgres-url is required")
	}
	if config.RoomID == "" {
		log.Fatal("room_id is required")
	}

	var err error
	config.LevelSizes, err = parseLevelSizes(*levelSizesStr)
	if err != nil {
		log.Fatalf("Unable to parse level_sizes: %v", err)
	}

	config.Verify = !*noVerify

	if outputFile != "" {
		config.OutputFile = openOutputFile(outputFile)
	}

	return config
}

func int64Ptr(i int64) *int64 {
	return &i
}

func int32Ptr(i int32) *int32 {
	return &i
}

func main() {
	config := parseArgs()
	run(config)
}

// Run runs through the steps of the compression:
//
// - Fetches current state groups for a room and their predecessors
// - Outputs #state groups and #lines in table they occupy
// - Runs the compressor to produce a new predecessor mapping
// - Outputs #lines in table that the new mapping would occupy
// - Outputs info about how the compressor got on
// - Checks that number of lines saved is greater than threshold
// - Ensures new mapping doesn't affect actual state contents
// - Produces SQL code to carry out changes and saves it to file
func run(config *Config) {
	ctx := context.Background()

	// First we need to get the current state groups
	log.Printf("Fetching state from DB for room '%s'...", config.RoomID)

	stateGroupMapDB, maxGroupFound, ok := database.GetDataFromDB(
		ctx,
		config.DBURL,
		config.RoomID,
		&config.MinStateGroup,
		&config.GroupsToCompress,
		&config.MaxStateGroup,
	)
	if !ok {
		log.Fatal("No state groups found within this range")
	}

	// Convert database entries to compressor entries
	stateGroupMap := make(map[int64]*compressor.StateGroupEntry)
	for k, v := range stateGroupMapDB {
		stateGroupMap[k] = &compressor.StateGroupEntry{
			InRange:        v.InRange,
			PrevStateGroup: v.PrevStateGroup,
			StateMap:       v.StateMap,
		}
	}

	log.Printf("Fetched state groups up to %d", maxGroupFound)
	log.Printf("Number of state groups: %d", len(stateGroupMap))

	originalSummedSize := 0
	for _, v := range stateGroupMap {
		originalSummedSize += v.StateMap.Len()
	}

	log.Printf("Number of rows in current table: %d", originalSummedSize)

	// Now we actually call the compression algorithm.
	log.Println("Compressing state...")

	compressorInstance := compressor.NewCompressor(stateGroupMap, config.LevelSizes)
	newStateGroupMap := compressorInstance.NewStateGroupMap

	// Done! Now to print a bunch of stats.
	compressedSummedSize := 0
	for _, v := range newStateGroupMap {
		compressedSummedSize += v.StateMap.Len()
	}

	ratio := float64(compressedSummedSize) / float64(originalSummedSize)

	log.Printf("Number of rows after compression: %d (%.2f%%)", compressedSummedSize, ratio*100)

	log.Println("Compression Statistics:")
	log.Printf("  Number of forced resets due to lacking prev: %d", compressorInstance.Stats.ResetsNoSuitablePrev)
	log.Printf("  Number of compressed rows caused by the above: %d", compressorInstance.Stats.ResetsNoSuitablePrevSize)
	log.Printf("  Number of state groups changed: %d", compressorInstance.Stats.StateGroupsChanged)

	if config.Graphs {
		// Convert maps for graphing
		beforeGraph := make(graphing.Graph)
		afterGraph := make(graphing.Graph)
		for k, v := range stateGroupMap {
			beforeGraph[k] = v
		}
		for k, v := range newStateGroupMap {
			afterGraph[k] = v
		}
		graphing.MakeGraphs(beforeGraph, afterGraph)
	}

	if ratio > 1.0 {
		log.Println("This compression would not remove any rows. Exiting.")
		return
	}

	if config.MinSavedRows != 0 {
		saving := originalSummedSize - compressedSummedSize
		if saving < int(config.MinSavedRows) {
			log.Printf("Only %d rows would be saved by this compression. Skipping output.", saving)
			return
		}
	}

	if config.Verify {
		checkThatMapsMatch(stateGroupMap, newStateGroupMap)
	}

	// If we are given an output file, we output the changes as SQL. If the
	// `transactions` argument is set we wrap each change to a state group in a
	// transaction.
	outputSQL(config, stateGroupMap, newStateGroupMap)

	// If commit_changes is set then commit the changes to the database
	if config.CommitChanges {
		// Convert back to database entries
		oldMapDB := make(map[int64]*database.StateGroupEntry)
		newMapDB := make(map[int64]*database.StateGroupEntry)
		for k, v := range stateGroupMap {
			oldMapDB[k] = &database.StateGroupEntry{
				InRange:        v.InRange,
				PrevStateGroup: v.PrevStateGroup,
				StateMap:       v.StateMap,
			}
		}
		for k, v := range newStateGroupMap {
			newMapDB[k] = &database.StateGroupEntry{
				InRange:        v.InRange,
				PrevStateGroup: v.PrevStateGroup,
				StateMap:       v.StateMap,
			}
		}
		database.SendChangesToDB(ctx, config.DBURL, config.RoomID, oldMapDB, newMapDB)
	}
}

// checkThatMapsMatch compares two sets of state groups.
//
// A state group entry contains a predecessor state group and a delta.
// The complete contents of a certain state group can be calculated by
// following this chain of predecessors back to some empty state and
// combining all the deltas together. This is called "collapsing".
//
// This function confirms that two state groups mappings lead to the
// exact same entries for each state group after collapsing them down.
func checkThatMapsMatch(oldMap, newMap map[int64]*compressor.StateGroupEntry) {
	log.Println("Checking that state maps match...")

	for sg := range oldMap {
		expected := collapseStateMaps(oldMap, sg)
		actual := collapseStateMaps(newMap, sg)

		if !stateMapsEqual(expected, actual) {
			log.Fatalf("States for group %d do not match. Expected %#v, found %#v", sg, expected, actual)
		}
	}

	log.Println("New state map matches old one")
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

// collapseStateMaps gets the full state for a given group from the map (of deltas).
func collapseStateMaps(m map[int64]*compressor.StateGroupEntry, stateGroup int64) *state_map.StateMap {
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

// generateSQL produces SQL code to carry out changes to database.
//
// It returns an iterator where each call to `next()` will
// return the SQL to alter a single state group in the database.
func generateSQL(oldMap, newMap map[int64]*compressor.StateGroupEntry, roomID string) <-chan string {
	ch := make(chan string)

	go func() {
		defer close(ch)

		for sg, oldEntry := range oldMap {
			newEntry, ok := newMap[sg]
			if !ok {
				continue
			}

			// Check if the new map has a different entry for this state group
			if !entriesEqual(oldEntry, newEntry) {
				// The sql commands that will carry out these changes
				var sql strings.Builder

				// Remove the current edge
				sql.WriteString(fmt.Sprintf("DELETE FROM state_group_edges WHERE state_group = %d;\n", sg))

				// If the new entry has a predecessor then put that into state_group_edges
				if newEntry.PrevStateGroup != nil {
					sql.WriteString(fmt.Sprintf("INSERT INTO state_group_edges (state_group, prev_state_group) VALUES (%d, %d);\n", sg, *newEntry.PrevStateGroup))
				}

				// Remove the current deltas for this state group
				sql.WriteString(fmt.Sprintf("DELETE FROM state_groups_state WHERE state_group = %d;\n", sg))

				if newEntry.StateMap.Len() > 0 {
					// Place all the deltas for the state group in the new map into state_groups_state
					sql.WriteString("INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES\n")

					escapedRoomID := database.PGEscape(roomID)
					iter := newEntry.StateMap.Iterator()
					for iter.Next() {
						key := iter.Key()
						value := iter.Value()
						escapedType := database.PGEscape(key.Type)
						escapedStateKey := database.PGEscape(key.StateKey)
						escapedEventID := database.PGEscape(value)

						// Write the row to be inserted of the form:
						// (state_group, room_id, type, state_key, event_id)
						sql.WriteString(fmt.Sprintf("    (%d, '%s', '%s', '%s', '%s'),\n", sg, escapedRoomID, escapedType, escapedStateKey, escapedEventID))
					}

					// Replace the last comma with a semicolon
					sqlStr := sql.String()
					sqlStr = sqlStr[:len(sqlStr)-2] + ";\n"
					ch <- sqlStr
				} else {
					ch <- sql.String()
				}
			}
		}
	}()

	return ch
}

func entriesEqual(a, b *compressor.StateGroupEntry) bool {
	if a.InRange != b.InRange {
		return false
	}
	if (a.PrevStateGroup == nil) != (b.PrevStateGroup == nil) {
		return false
	}
	if a.PrevStateGroup != nil && b.PrevStateGroup != nil && *a.PrevStateGroup != *b.PrevStateGroup {
		return false
	}
	if a.StateMap.Len() != b.StateMap.Len() {
		return false
	}
	equal := true
	a.StateMap.Range(func(key state_map.Key, value string) bool {
		bVal, ok := b.StateMap.Get(key.Type, key.StateKey)
		if !ok || bVal != value {
			equal = false
			return false
		}
		return true
	})
	return equal
}

// outputSQL produces SQL code to carry out changes and saves it to file.
func outputSQL(config *Config, oldMap, newMap map[int64]*compressor.StateGroupEntry) {
	if config.OutputFile == nil {
		return
	}

	log.Println("Writing changes...")

	for sqlTransaction := range generateSQL(oldMap, newMap, config.RoomID) {
		if config.Transactions {
			sqlTransaction = "BEGIN;\n" + sqlTransaction + "COMMIT;\n"
		}

		_, err := config.OutputFile.WriteString(sqlTransaction)
		if err != nil {
			log.Fatalf("Something went wrong while writing SQL to file: %v", err)
		}
	}
}

// Unused helper functions kept for potential future use
var _ = func() {} // Placeholder to avoid unused import errors

// Helper to set flag.Int64Var with a pointer
func flagInt64VarPtr(p **int64, name string, value int64, usage string) {
	*p = flag.Int64(name, value, usage)
}

// Helper to open output file
func openOutputFile(path string) *os.File {
	if path == "" {
		return nil
	}
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Unable to create output file: %v", err)
	}
	return file
}

// Helper to write to file
func writeFile(w io.Writer, data string) {
	_, err := w.Write([]byte(data))
	if err != nil {
		log.Fatalf("Error writing: %v", err)
	}
}
