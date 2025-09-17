# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Go-based Postgres database synchronization service that performs one-way batched copying of new rows between two databases with identical schemas. The service is designed for security scanning workflows where findings and assets need to be synchronized from a source database to a target database.

## Architecture

- **Single file application**: All logic contained in `main.go` 
- **Configuration-driven**: Table sync configuration defined in `config.json`
- **Idempotent**: Uses `ON CONFLICT ... DO NOTHING` to prevent duplicates
- **Watermark-_based_**: Tracks last sync timestamp per table in a metadata table
- **Batch processing**: Configurable batch sizes to handle large datasets efficiently

## Key Components

- `TableConfig`: Defines sync configuration per table (name, key columns, sync column)
- `sync_meta` table: Stores watermark timestamps in the target database
- Column discovery: Runtime schema introspection via `information_schema`
- Stable paging: Orders by sync column + first key column for consistent results

## Common Commands

### Build and Run
```bash
go build -o syncsvc
./syncsvc
```

### Environment Variables Required
```bash
export SOURCE_DSN="postgres://scoutUser:scoutPass@localhost:5433/scout?sslmode=disable"
export TARGET_DSN="postgres://scoutUser:scoutPass@localhost:5433/scanner?sslmode=disable" 
export CONFIG_PATH="./config.json"  # optional, defaults to ./config.json
export BATCH_SIZE="1000"            # optional, defaults to 1000
export DRY_RUN="false"              # optional, defaults to false
```

### Testing/Development
- Use `DRY_RUN=true` to test without writing to target database
- Monitor logs for sync progress and watermark advancement
- Verify target database constraints match `key_columns` configuration

## Configuration Format

The `config.json` defines which tables to sync:
- `name`: Full table name (schema.table format)
- `key_columns`: Columns that form unique constraint in target (for conflict resolution)
- `sync_column`: Timestamp column for incremental sync (defaults to "created_at")

## Dependencies

- `github.com/jackc/pgx/v5`: PostgreSQL driver and connection pooling
- Go 1.24+

## Important Constraints

- Target tables must have UNIQUE/PRIMARY KEY constraints matching `key_columns`
- Sync columns must be monotonic timestamps (e.g., `created_at`)
- Both databases must have identical schemas for synced tables
- Service runs once and exits (designed for cron/CI integration)