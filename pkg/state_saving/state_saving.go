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

package state_saving

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/jackc/pgx/v4"
	"synapse_compress_state/pkg/compressor"
)

// ConnectToDatabase connects to the database and returns a postgres client.
//
// Arguments:
//   - dbURL: The URL of the postgres database that synapse is using.
//     e.g. "postgresql://user:password@domain.com/synapse"
func ConnectToDatabase(dbURL string) (*pgx.Conn, error) {
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

// CreateTablesIfNeeded creates the state_compressor_state and state_compressor_progress tables.
//
// If these tables already exist then this function does nothing.
//
// Arguments:
//   - client: A postgres client used to send the requests to the database
func CreateTablesIfNeeded(ctx context.Context, conn *pgx.Conn) error {
	createStateTable := `
		CREATE TABLE IF NOT EXISTS state_compressor_state (
			room_id TEXT NOT NULL,
			level_num INT NOT NULL,
			max_size INT NOT NULL,
			current_length INT NOT NULL,
			current_head BIGINT,
			UNIQUE (room_id, level_num)
		)`

	_, err := conn.Exec(ctx, createStateTable)
	if err != nil {
		return err
	}

	createStateTableIndexes := `
		CREATE INDEX IF NOT EXISTS state_compressor_state_index ON state_compressor_state (room_id)`

	_, err = conn.Exec(ctx, createStateTableIndexes)
	if err != nil {
		return err
	}

	createProgressTable := `
		CREATE TABLE IF NOT EXISTS state_compressor_progress (
			room_id TEXT PRIMARY KEY,
			last_compressed BIGINT NOT NULL
		)`

	_, err = conn.Exec(ctx, createProgressTable)
	if err != nil {
		return err
	}

	createCompressorGlobalProgressTable := `
		CREATE TABLE IF NOT EXISTS state_compressor_total_progress(
			lock CHAR(1) NOT NULL DEFAULT 'X' UNIQUE,
			lowest_uncompressed_group BIGINT NOT NULL,
			CHECK (lock='X')
		);
		INSERT INTO state_compressor_total_progress 
			(lowest_uncompressed_group) 
			VALUES (0)
		ON CONFLICT (lock) DO NOTHING;
	`

	_, err = conn.Exec(ctx, createCompressorGlobalProgressTable)
	if err != nil {
		return err
	}

	return nil
}

// ReadRoomCompressorState retrieves the level info so we can restart the compressor.
//
// Arguments:
//   - client: A postgres client used to send the requests to the database
//   - roomID: The room who's saved compressor state we want to load
func ReadRoomCompressorState(ctx context.Context, conn *pgx.Conn, roomID string) (*int64, []*compressor.Level, error) {
	// Query to retrieve all levels from state_compressor_state
	// Ordered by ascending level_number
	sql := `
		SELECT level_num, max_size, current_length, current_head, last_compressed
		FROM state_compressor_state 
		LEFT JOIN state_compressor_progress USING (room_id)
		WHERE room_id = $1
		ORDER BY level_num ASC
	`

	rows, err := conn.Query(ctx, sql, roomID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// Needed to ensure that the rows are for unique consecutive levels
	// starting from 1 (i.e of form [1,2,3] not [0,1,2] or [1,1,2,2,3])
	prevSeen := 0

	// The vector to store the level info from the database in
	var levelInfo []*compressor.Level

	// Where the last compressor run stopped
	var lastCompressed *int64
	// Used to only read last_compressed value once
	firstRow := true

	// Loop through all the rows retrieved by that query
	for rows.Next() {
		var levelNum int32
		var maxSize int32
		var currentLength int32
		var currentHead *int64
		var rowLastCompressed *int64

		err := rows.Scan(&levelNum, &maxSize, &currentLength, &currentHead, &rowLastCompressed)
		if err != nil {
			return nil, nil, err
		}

		// Only read the last compressed column once since is the same for each row
		if firstRow {
			lastCompressed = rowLastCompressed
			if lastCompressed == nil {
				return nil, nil, fmt.Errorf("no entry in state_compressor_progress for room %s but entries in state_compressor_state were found", roomID)
			}
			firstRow = false
		}

		// Check that there aren't multiple entries for the same level number
		// in the database. (Should be impossible due to unique key constraint)
		if prevSeen == int(levelNum) {
			return nil, nil, fmt.Errorf("the level %d occurs twice in state_compressor_state for room %s", levelNum, roomID)
		}

		// Check that there is no missing level in the database
		// e.g. if the previous row retrieved was for level 1 and this
		// row is for level 3 then since the SQL query orders the results
		// in ascenting level numbers, there was no level 2 found!
		if prevSeen != int(levelNum)-1 {
			return nil, nil, fmt.Errorf("levels between %d and %d are missing", prevSeen, levelNum)
		}

		// if the level is not empty, then it must have a head!
		if currentHead == nil && currentLength != 0 {
			return nil, nil, fmt.Errorf("level %d has no head but current length is %d in room %s", levelNum, currentLength, roomID)
		}

		// If the level has more groups in than the maximum then something is wrong!
		if int(currentLength) > int(maxSize) {
			return nil, nil, fmt.Errorf("level %d has length %d but max size %d in room %s", levelNum, currentLength, maxSize, roomID)
		}

		// Add this level to the level_info vector
		levelInfo = append(levelInfo, compressor.RestoreLevel(int(maxSize), int(currentLength), currentHead))
		// Mark the previous level_number seen as the current one
		prevSeen = int(levelNum)
	}

	// If we didn't retrieve anything from the database then there is no saved state
	// in the database!
	if len(levelInfo) == 0 {
		return nil, nil, nil
	}

	// Return the compressor state we retrieved
	// lastCompressed cannot be nil at this point
	return lastCompressed, levelInfo, nil
}

// WriteRoomCompressorState saves the level info so it can be loaded by the next run of the compressor.
//
// Arguments:
//   - client: A postgres client used to send the requests to the database
//   - roomID: The room who's saved compressor state we want to save
//   - levelInfo: The state that can be used to restore the compressor later
//   - lastCompressed: The last state_group that was compressed. This is needed
//     so that the compressor knows where to start from next
func WriteRoomCompressorState(ctx context.Context, conn *pgx.Conn, roomID string, levelInfo []*compressor.Level, lastCompressed int64) error {
	// Wrap all the changes to the state for this room in a transaction
	// This prevents accidentally having malformed compressor start info
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Go through every level that the compressor is using
	for levelNum, level := range levelInfo {
		// the 1st level is level 1 not level 0, but enumerate starts at 0
		// so need to add 1 to get correct number
		dbLevelNum := levelNum + 1

		// bring the level info out of the Level struct
		maxSize := level.GetMaxLength()
		currentLen := level.GetCurrentLength()
		currentHead := level.GetHead()

		// Update the database with this compressor state information
		_, err := tx.Exec(ctx, `
			INSERT INTO state_compressor_state 
				(room_id, level_num, max_size, current_length, current_head) 
				VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (room_id, level_num) 
				DO UPDATE SET 
					max_size = excluded.max_size,
					current_length = excluded.current_length,
					current_head = excluded.current_head;
		`, roomID, int32(dbLevelNum), int32(maxSize), int32(currentLen), currentHead)
		if err != nil {
			return err
		}
	}

	// Update the database with this progress information
	_, err = tx.Exec(ctx, `
		INSERT INTO state_compressor_progress (room_id, last_compressed) 
			VALUES ($1, $2)
		ON CONFLICT (room_id)
			DO UPDATE SET last_compressed = excluded.last_compressed;
	`, roomID, lastCompressed)
	if err != nil {
		return err
	}

	// Commit the transaction (otherwise changes never happen)
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	return nil
}

// GetNextRoomToCompress returns the room with with the lowest uncompressed state group id.
//
// A group is detected as uncompressed if it is greater than the `last_compressed`
// entry in `state_compressor_progress` for that room.
//
// The `lowest_uncompressed_group` value stored in `state_compressor_total_progress`
// stores where this method last finished, to prevent repeating work.
//
// Arguments:
//   - client: A postgres client used to send the requests to the database
func GetNextRoomToCompress(ctx context.Context, conn *pgx.Conn) (*string, error) {
	// Walk the state_groups table until find next uncompressed group
	getNextRoom := `
		SELECT room_id, id 
		FROM state_groups
		LEFT JOIN state_compressor_progress USING (room_id)
		WHERE
			id >= (SELECT lowest_uncompressed_group FROM state_compressor_total_progress)
			AND (
				id > last_compressed
				OR last_compressed IS NULL
			)
		ORDER BY id ASC
		LIMIT 1
	`

	row := conn.QueryRow(ctx, getNextRoom)

	var nextRoom string
	var lowestUncompressedGroup int64
	err := row.Scan(&nextRoom, &lowestUncompressedGroup)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// This method has determined where the lowest uncompressesed group is, save that
	// information so we don't have to redo this work in the future.
	updateTotalProgress := `
		UPDATE state_compressor_total_progress SET lowest_uncompressed_group = $1;
	`

	_, err = conn.Exec(ctx, updateTotalProgress, lowestUncompressedGroup)
	if err != nil {
		return nil, err
	}

	return &nextRoom, nil
}
