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

package database

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v4"
	"synapse_compress_state/pkg/compressor"
	"synapse_compress_state/pkg/state_map"
)

// StateGroupEntry is an entry for a state group.
type StateGroupEntry struct {
	InRange        bool
	PrevStateGroup *int64
	StateMap       *state_map.StateMap[string]
}

// GetInitialDataFromDB fetches the entries in state_groups_state and immediate predecessors for
// a specific room.
//
// - Fetches first `[groups_to_compress]` rows with group id higher than min
// - Stores the group id, predecessor id and deltas into a map
// - returns map and maximum row that was considered
func GetInitialDataFromDB(ctx context.Context, conn *pgx.Conn, roomID string, minStateGroup *int64, maxGroupFound int64) map[int64]*StateGroupEntry {
	sql := `
		SELECT m.id, prev_state_group, type, state_key, s.event_id
		FROM state_groups AS m
		LEFT JOIN state_groups_state AS s ON (m.id = s.state_group)
		LEFT JOIN state_group_edges AS e ON (m.id = e.state_group)
		WHERE m.room_id = $1 AND m.id <= $2
	`

	var rows pgx.Rows
	var err error

	if minStateGroup != nil {
		rows, err = conn.Query(ctx, fmt.Sprintf("%s AND m.id > $3", sql), roomID, maxGroupFound, *minStateGroup)
	} else {
		rows, err = conn.Query(ctx, sql, roomID, maxGroupFound)
	}

	if err != nil {
		panic(fmt.Sprintf("Something went wrong while querying the database: %v", err))
	}
	defer rows.Close()

	stateGroupMap := make(map[int64]*StateGroupEntry)

	for rows.Next() {
		var id int64
		var prevStateGroup *int64
		var eventType, stateKey, eventID string

		err := rows.Scan(&id, &prevStateGroup, &eventType, &stateKey, &eventID)
		if err != nil {
			panic(fmt.Sprintf("Error scanning row: %v", err))
		}

		entry, ok := stateGroupMap[id]
		if !ok {
			entry = &StateGroupEntry{
				StateMap: state_map.New[string](),
			}
			stateGroupMap[id] = entry
		}

		// Save the predecessor and mark for compression
		entry.PrevStateGroup = prevStateGroup
		entry.InRange = true

		// Copy the single delta from the predecessor stored in this row
		if eventType != "" {
			entry.StateMap.Set(eventType, stateKey, eventID)
		}
	}

	return stateGroupMap
}

// LoadLevelHeads loads the state_groups that are at the head of each compressor level.
// NOTE this does not also retrieve their predecessors.
func LoadLevelHeads(ctx context.Context, conn *pgx.Conn, levelInfo []*compressor.Level) map[int64]*StateGroupEntry {
	// Obtain all of the heads that aren't nil from levelInfo
	levelHeads := make([]int64, 0)
	for _, l := range levelInfo {
		if l.GetHead() != nil {
			levelHeads = append(levelHeads, *l.GetHead())
		}
	}

	if len(levelHeads) == 0 {
		return make(map[int64]*StateGroupEntry)
	}

	// Query to get id, predecessor and deltas for each state group
	sql := `
		SELECT m.id, prev_state_group, type, state_key, s.event_id
		FROM state_groups AS m
		LEFT JOIN state_groups_state AS s ON (m.id = s.state_group)
		LEFT JOIN state_group_edges AS e ON (m.id = e.state_group)
		WHERE m.id = ANY($1)
		ORDER BY m.id
	`

	rows, err := conn.Query(ctx, sql, levelHeads)
	if err != nil {
		panic(fmt.Sprintf("Error querying level heads: %v", err))
	}
	defer rows.Close()

	stateGroupMap := make(map[int64]*StateGroupEntry)

	for rows.Next() {
		var id int64
		var prevStateGroup *int64
		var eventType, stateKey, eventID string

		err := rows.Scan(&id, &prevStateGroup, &eventType, &stateKey, &eventID)
		if err != nil {
			panic(fmt.Sprintf("Error scanning row: %v", err))
		}

		entry, ok := stateGroupMap[id]
		if !ok {
			entry = &StateGroupEntry{
				// Default StateGroupEntry has InRange as false
				// This is what we want since as a level head, it has already been compressed by the
				// previous run!
				StateMap: state_map.New[string](),
			}
			stateGroupMap[id] = entry
		}

		// Save the predecessor (this may already be there)
		entry.PrevStateGroup = prevStateGroup

		// Copy the single delta from the predecessor stored in this row
		if eventType != "" {
			entry.StateMap.Set(eventType, stateKey, eventID)
		}
	}

	return stateGroupMap
}

// FindMaxGroup returns the group ID of the last group to be compressed.
//
// This can be saved so that future runs of the compressor only
// continue from after this point. If no groups can be found in
// the range specified it returns nil.
func FindMaxGroup(ctx context.Context, conn *pgx.Conn, roomID string, minStateGroup *int64, groupsToCompress *int64, maxStateGroup *int64) *int64 {
	// Get list of state_id's in a certain room
	queryChunkOfIDs := "SELECT id FROM state_groups WHERE room_id = $1"

	if maxStateGroup != nil {
		queryChunkOfIDs = fmt.Sprintf("%s AND id <= %d", queryChunkOfIDs, *maxStateGroup)
	}

	var sqlQuery string
	var args []interface{}

	// Adds additional constraint if a groups_to_compress or min_state_group have been specified
	// Note a min state group is only used if groups_to_compress also is
	if minStateGroup != nil && groupsToCompress != nil {
		args = []interface{}{roomID, *minStateGroup, *groupsToCompress}
		sqlQuery = fmt.Sprintf(
			"SELECT id FROM (%s AND id > $2 ORDER BY id ASC LIMIT $3) AS ids ORDER BY ids.id DESC LIMIT 1",
			queryChunkOfIDs,
		)
	} else if groupsToCompress != nil {
		args = []interface{}{roomID, *groupsToCompress}
		sqlQuery = fmt.Sprintf(
			"SELECT id FROM (%s ORDER BY id ASC LIMIT $2) AS ids ORDER BY ids.id DESC LIMIT 1",
			queryChunkOfIDs,
		)
	} else {
		args = []interface{}{roomID}
		sqlQuery = fmt.Sprintf(
			"SELECT id FROM (%s) AS ids ORDER BY ids.id DESC LIMIT 1",
			queryChunkOfIDs,
		)
	}

	var finalRow int64
	err := conn.QueryRow(ctx, sqlQuery, args...).Scan(&finalRow)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		panic(fmt.Sprintf("Something went wrong while querying the database: %v", err))
	}

	return &finalRow
}

// GetDataFromDB fetches the entries in state_groups_state (and their prev groups) for a
// specific room.
//
// Returns with the state_group map and the id of the last group that was used
// Or nil if there are no state groups within the range given
func GetDataFromDB(ctx context.Context, dbURL string, roomID string, minStateGroup *int64, groupsToCompress *int64, maxStateGroup *int64) (map[int64]*StateGroupEntry, int64, bool) {
	conn, err := connectDB(dbURL)
	if err != nil {
		panic(fmt.Sprintf("Error connecting to the database: %v", err))
	}
	defer conn.Close(ctx)

	// Search for the group id of the groups_to_compress'th group after min_state_group
	// If this is saved, then the compressor can continue by having min_state_group being
	// set to this maximum. If no such group can be found then return nil.
	maxGroupFound := FindMaxGroup(ctx, conn, roomID, minStateGroup, groupsToCompress, maxStateGroup)
	if maxGroupFound == nil {
		return nil, 0, false
	}

	stateGroupMap := GetInitialDataFromDB(ctx, conn, roomID, minStateGroup, *maxGroupFound)

	return LoadMapFromDB(ctx, conn, roomID, minStateGroup, *maxGroupFound, stateGroupMap), *maxGroupFound, true
}

// ReloadDataFromDB fetches the entries in state_groups_state (and their prev groups) for a
// specific room. This method should only be called if resuming the compressor from
// where it last finished - and as such also loads in the state groups from the heads
// of each of the levels (as they were at the end of the last run of the compressor).
//
// Returns with the state_group map and the id of the last group that was used
// Or nil if there are no state groups within the range given
func ReloadDataFromDB(ctx context.Context, dbURL string, roomID string, minStateGroup *int64, groupsToCompress *int64, levelInfo []*compressor.Level) (map[int64]*StateGroupEntry, int64, bool) {
	conn, err := connectDB(dbURL)
	if err != nil {
		panic(fmt.Sprintf("Error connecting to the database: %v", err))
	}
	defer conn.Close(ctx)

	// Search for the group id of the groups_to_compress'th group after min_state_group
	// If this is saved, then the compressor can continue by having min_state_group being
	// set to this maximum. If no such group can be found then return nil.
	maxGroupFound := FindMaxGroup(ctx, conn, roomID, minStateGroup, groupsToCompress, nil)
	if maxGroupFound == nil {
		return nil, 0, false
	}

	// Load just the state_groups at the head of each level
	// this doesn't load their predecessors as that will be done at the end of
	// LoadMapFromDB()
	stateGroupMap := LoadLevelHeads(ctx, conn, levelInfo)

	return LoadMapFromDB(ctx, conn, roomID, minStateGroup, *maxGroupFound, stateGroupMap), *maxGroupFound, true
}

// LoadMapFromDB fetches the entries in state_groups_state (and their prev groups) for a
// specific room within a certain range. These are appended onto the provided map.
//
// - Fetches the first `[group]` rows with group id after `[min]`
// - Recursively searches for missing predecessors and adds those
//
// Returns with the state_group map and the id of the last group that was used
func LoadMapFromDB(ctx context.Context, conn *pgx.Conn, roomID string, minStateGroup *int64, maxGroupFound int64, stateGroupMap map[int64]*StateGroupEntry) map[int64]*StateGroupEntry {
	initialData := GetInitialDataFromDB(ctx, conn, roomID, minStateGroup, maxGroupFound)
	for k, v := range initialData {
		stateGroupMap[k] = v
	}

	// Due to reasons some of the state groups appear in the edges table, but
	// not in the state_groups_state table.
	//
	// Also it is likely that the predecessor of a node will not be within the
	// chunk that was specified by min_state_group and groups_to_compress.
	// This means they don't get included in our DB queries, so we have to fetch
	// any missing groups explicitly.
	//
	// Since the returned groups may themselves reference groups we don't have,
	// we need to do this recursively until we don't find any more missing.
	for {
		missingSGs := make([]int64, 0)
		for _, entry := range stateGroupMap {
			if entry.PrevStateGroup != nil {
				prevSG := *entry.PrevStateGroup
				if _, ok := stateGroupMap[prevSG]; !ok {
					missingSGs = append(missingSGs, prevSG)
				}
			}
		}

		if len(missingSGs) == 0 {
			break
		}

		sort.Slice(missingSGs, func(i, j int) bool { return missingSGs[i] < missingSGs[j] })

		// Deduplicate
		deduped := make([]int64, 0, len(missingSGs))
		for i, sg := range missingSGs {
			if i == 0 || sg != missingSGs[i-1] {
				deduped = append(deduped, sg)
			}
		}
		missingSGs = deduped

		// Find state groups not picked up already and add them to the map
		mapData := GetMissingFromDB(ctx, conn, missingSGs, minStateGroup, maxGroupFound)
		for k, v := range mapData {
			if _, exists := stateGroupMap[k]; !exists {
				stateGroupMap[k] = v
			}
		}
	}

	return stateGroupMap
}

// GetMissingFromDB fetches missing state groups from the database.
func GetMissingFromDB(ctx context.Context, conn *pgx.Conn, missingSGs []int64, minStateGroup *int64, maxGroupFound int64) map[int64]*StateGroupEntry {
	if len(missingSGs) == 0 {
		return make(map[int64]*StateGroupEntry)
	}

	sql := `
		SELECT m.id, prev_state_group, type, state_key, s.event_id
		FROM state_groups AS m
		LEFT JOIN state_groups_state AS s ON (m.id = s.state_group)
		LEFT JOIN state_group_edges AS e ON (m.id = e.state_group)
		WHERE m.id = ANY($1)
	`

	rows, err := conn.Query(ctx, sql, missingSGs)
	if err != nil {
		panic(fmt.Sprintf("Error querying missing state groups: %v", err))
	}
	defer rows.Close()

	stateGroupMap := make(map[int64]*StateGroupEntry)

	for rows.Next() {
		var id int64
		var prevStateGroup *int64
		var eventType, stateKey, eventID string

		err := rows.Scan(&id, &prevStateGroup, &eventType, &stateKey, &eventID)
		if err != nil {
			panic(fmt.Sprintf("Error scanning row: %v", err))
		}

		entry, ok := stateGroupMap[id]
		if !ok {
			entry = &StateGroupEntry{
				StateMap: state_map.New[string](),
			}
			stateGroupMap[id] = entry
		}

		// Save the predecessor and mark for compression
		entry.PrevStateGroup = prevStateGroup
		// Only mark as in_range if it's within the specified range
		if minStateGroup == nil || id > *minStateGroup {
			if id <= maxGroupFound {
				entry.InRange = true
			}
		}

		// Copy the single delta from the predecessor stored in this row
		if eventType != "" {
			entry.StateMap.Set(eventType, stateKey, eventID)
		}
	}

	return stateGroupMap
}

// SendChangesToDB commits changes to the database.
func SendChangesToDB(ctx context.Context, dbURL string, roomID string, oldMap, newMap map[int64]*StateGroupEntry) {
	conn, err := connectDB(dbURL)
	if err != nil {
		panic(fmt.Sprintf("Error connecting to the database: %v", err))
	}
	defer conn.Close(ctx)

	// Start a transaction
	tx, err := conn.Begin(ctx)
	if err != nil {
		panic(fmt.Sprintf("Error starting transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	for sg, oldEntry := range oldMap {
		newEntry, ok := newMap[sg]
		if !ok {
			continue
		}

		// Check if the new map has a different entry for this state group
		if !entriesEqual(oldEntry, newEntry) {
			// Remove the current edge
			_, err := tx.Exec(ctx, "DELETE FROM state_group_edges WHERE state_group = $1", sg)
			if err != nil {
				panic(fmt.Sprintf("Error deleting edge: %v", err))
			}

			// If the new entry has a predecessor then put that into state_group_edges
			if newEntry.PrevStateGroup != nil {
				_, err := tx.Exec(ctx, "INSERT INTO state_group_edges (state_group, prev_state_group) VALUES ($1, $2)", sg, *newEntry.PrevStateGroup)
				if err != nil {
					panic(fmt.Sprintf("Error inserting edge: %v", err))
				}
			}

			// Remove the current deltas for this state group
			_, err = tx.Exec(ctx, "DELETE FROM state_groups_state WHERE state_group = $1", sg)
			if err != nil {
				panic(fmt.Sprintf("Error deleting state groups state: %v", err))
			}

			if newEntry.StateMap.Len() > 0 {
				// Place all the deltas for the state group in the new map into state_groups_state
				iter := newEntry.StateMap.Iterator()
				for iter.Next() {
					key := iter.Key()
					value := iter.Value()
					_, err = tx.Exec(ctx, "INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES ($1, $2, $3, $4, $5)", sg, roomID, key.Type, key.StateKey, value)
					if err != nil {
						panic(fmt.Sprintf("Error inserting state groups state: %v", err))
					}
				}
			}
		}
	}

	// Commit the transaction
	err = tx.Commit(ctx)
	if err != nil {
		panic(fmt.Sprintf("Error committing transaction: %v", err))
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

func connectDB(dbURL string) (*pgx.Conn, error) {
	// Create a TLS config that skips verification (matching the Rust code)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	// Parse the connection string and add SSL mode
	config, err := pgx.ParseConfig(dbURL)
	if err != nil {
		return nil, err
	}
	config.TLSConfig = tlsConfig

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// PGEscape escapes a string for use in SQL queries.
func PGEscape(s string) string {
	// Simple escaping - double quotes for PostgreSQL
	result := ""
	for _, r := range s {
		if r == '\'' {
			result += "''"
		} else {
			result += string(r)
		}
	}
	return result
}
