# Compress Synapse State Tables

This repository contains experimental tools that attempt to reduce the number of
rows in the `state_groups_state` table inside of a Synapse PostgreSQL database.

The project is implemented in Go and provides a manual compression tool.

## Manual tool: synapse_compress_state

### Introduction

A manual tool that reads in the rows from `state_groups_state` and `state_group_edges`
tables for a specified room and calculates the changes that could be made that
(hopefully) will significantly reduce the number of rows.

This tool currently *does not* write to the database by default, so should be
safe to run. If the `-o` option is specified then SQL will be written to the
given file that would change the tables to match the calculated state. (Note
that if `-t` is given then each change to a particular state group is wrapped
in a transaction). If you do wish to send the changes to the database automatically
then the `-c` flag can be set.

The SQL generated is safe to apply against the database with Synapse running.
This is because the `state_groups` and `state_groups_state` tables are append-only:
once written to the database, they are never modified. There is therefore no danger
of a modification racing against a running Synapse. Further, this script makes its
changes within atomic transactions, and each transaction should not affect the results
from any of the queries that Synapse performs.

The tool will also ensure that the generated state deltas do give the same state
as the existing state deltas before generating any SQL.

### Building

This tool requires Go 1.21 or later to be installed. See https://go.dev/dl/ for
installation instructions.

To build `synapse_compress_state`, clone this repository and navigate to the
repository root, then execute:

```bash
go build ./cmd/synapse_compress_state
```

This will create an executable named `synapse_compress_state` in the current directory.

### Example usage

```bash
$ ./synapse_compress_state -p "postgresql://localhost/synapse" -r '!some_room:example.com' -o out.sql -t
Fetching state from DB for room '!some_room:example.com'...
Number of state groups: 73904
Number of rows in current table: 2240043
Number of rows after compression: 165754 (7.40%)
Compression Statistics:
  Number of forced resets due to lacking prev: 34
  Number of compressed rows caused by the above: 17092
  Number of state groups changed: 2748
New state map matches old one

# It's finished, so we can now go and rewrite the DB
$ psql synapse < out.sql
```

### Running Options

- `-p [POSTGRES_LOCATION]` **Required**  
  The configuration for connecting to the Postgres database. This should be of the form
  `"postgresql://username:password@mydomain.com/database"` or a key-value pair
  string: `"user=username password=password dbname=database host=mydomain.com"`

- `-r [ROOM_ID]` **Required**  
  The room to process (this is the value found in the `rooms` table of the database,
  not the common name for the room - it should look like: `!wOlkWNmgkAZFxbTaqj:matrix.org`).

- `-b [MIN_STATE_GROUP]`  
  The state group to start processing from (non-inclusive).

- `-n [GROUPS_TO_COMPRESS]`  
  How many groups to load into memory to compress (starting from the 1st group in the room
  or the group specified by `-b`).

- `-l [LEVELS]`  
  Sizes of each new level in the compression algorithm, as a comma-separated list.
  The first entry in the list is for the lowest, most granular level, with each
  subsequent entry being for the next highest level. The number of entries in the
  list determines the number of levels that will be used. The sum of the sizes of
  the levels affects the performance of fetching the state from the database, as the
  sum of the sizes is the upper bound on the number of iterations needed to fetch a
  given set of state. [defaults to `"100,50,25"`]

- `-m [COUNT]`  
  If the compressor cannot save this many rows from the database then it will stop early.

- `-s [MAX_STATE_GROUP]`  
  If a `max_state_group` is specified then only state groups with id's lower than this
  number can be compressed.

- `-o [FILE]`  
  File to output the SQL transactions to (for later running on the database).

- `-t`  
  If this flag is set then each change to a particular state group is wrapped in a
  transaction. This should be done if you wish to apply the changes while Synapse is
  still running.

- `-c`  
  If this flag is set then the changes the compressor makes will be committed to the
  database. This should be safe to use while Synapse is running as it wraps the changes
  to every state group in its own transaction (as if the transaction flag was set).

- `-g`  
  If this flag is set then output the node and edge information for the state_group
  directed graph built up from the predecessor state_group links. These can be looked
  at in something like Gephi (https://gephi.org).

- `-N`  
  Do not double-check that the compression was performed correctly (skip verification).

## Docker

You can also run the tool using Docker:

```bash
docker run --rm ghcr.io/matrix-org/synapse-compress-state:latest \
  synapse_compress_state -p "postgresql://user:pass@localhost/synapse" -r '!room:example.com' -o /out/out.sql -t
```

Make sure to mount a volume for the output file:

```bash
docker run --rm -v $(pwd)/output:/out ghcr.io/matrix-org/synapse-compress-state:latest \
  synapse_compress_state -p "postgresql://user:pass@host.docker.internal/synapse" -r '!room:example.com' -o /out/out.sql -t
```

## Running tests

To run the tests:

```bash
go test -v ./...
```

## Troubleshooting

### Connecting to database

#### From local machine

If you setup Synapse using the instructions on https://matrix-org.github.io/synapse/latest/postgres.html
you should have a username and password to use to login to the postgres database. To run the compressor
from the machine where Postgres is running, the url will be the following:

`postgresql://synapse_user:synapse_password@localhost/synapse`

#### From remote machine

If you wish to connect from a different machine, you'll need to edit your Postgres settings to allow
remote connections. This requires updating the
[`pg_hba.conf`](https://www.postgresql.org/docs/current/auth-pg-hba-conf.html) and the `listen_addresses`
setting in [`postgresql.conf`](https://www.postgresql.org/docs/current/runtime-config-connection.html)

### Building difficulties

Go requires minimal setup. Just ensure you have Go 1.21 or later installed.

On Ubuntu you can install Go with:

```bash
sudo apt-get update && sudo apt-get install golang-go
```

Or download from https://go.dev/dl/

### Compressor is trying to increase the number of rows

Backfilling can lead to issues with compression. The compressor may skip chunks it can't
reduce the size of, which helps jump over backfilled state_groups. Lots of state resolution
might also impact the ability to use the compressor.

To examine the state_group hierarchy, run the manual tool on a room with the `-g` option
and look at the graphs.
