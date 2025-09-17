// main.go
// A small Go service that batches new rows from a source Postgres database into a target Postgres database.
// - Works across two databases with identical schemas (same tables/columns).
// - Copies only NEW rows using a per-table sync watermark (e.g., created_at).
// - Idempotent via ON CONFLICT (key_columns) DO NOTHING. Requires a UNIQUE/PK over key_columns in target.
// - Discovers all columns at runtime via information_schema, so it works generically across tables.
//
// Usage (env):
//   SOURCE_DSN=postgres://user:pass@host:5432/source_db?sslmode=disable
//   TARGET_DSN=postgres://user:pass@host:5432/target_db?sslmode=disable
//   CONFIG_PATH=./config.json  // see example format below
//   BATCH_SIZE=1000            // optional (default 1000)
//   DRY_RUN=false              // optional (true to log but not write)
//
// config.json example:
// {
//   "tables": [
//     {"name": "public.findings", "key_columns": ["id"], "sync_column": "created_at"},
//     {"name": "public.assets",   "key_columns": ["asset_hash", "scan_id"], "sync_column": "created_at"}
//   ]
// }
//
// Build & run:
//   go mod init syncsvc
//   go get github.com/jackc/pgx/v5 github.com/jackc/pgx/v5/pgxpool
//   go build -o syncsvc
//   ./syncsvc
//
// Notes:
// - Ensure target tables have UNIQUE/PRIMARY KEY constraints matching key_columns.
// - Ensure sync_column exists and is monotonic (e.g., created_at timestamptz).
// - A metadata table sync_meta is created in TARGET to store per-table watermarks.
// - The service runs once and exits. Use a cron/systemd timer or CI job after the scanner finishes.

package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TableConfig struct {
	Name       string   `json:"name"`
	KeyColumns []string `json:"key_columns"`
	SyncColumn string   `json:"sync_column"`
}

// IDMapping stores mapping between source and target IDs for a specific table
type IDMapping struct {
	tableName           string
	keyColumns          []string
	mapping             map[string]int64 // "key1,key2,..." -> target_id
	sourceIDToKeyValues map[int64][]any  // source_id -> key_values for reverse lookup
}

// IDMappingCache caches ID mappings for all tables
type IDMappingCache map[string]*IDMapping

type AppConfig struct {
	Tables []TableConfig `json:"tables"`
}

const metaTableDDL = `
CREATE TABLE IF NOT EXISTS public.sync_meta (
    table_name   text PRIMARY KEY,
    last_sync_ts timestamptz NOT NULL DEFAULT 'epoch'
);
`

func getenv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env %s", key)
	}
	return v
}

func loadConfig(path string) (AppConfig, error) {
	var cfg AppConfig
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	if len(cfg.Tables) == 0 {
		return cfg, errors.New("config.tables is empty")
	}
	for i := range cfg.Tables {
		if cfg.Tables[i].Name == "" {
			return cfg, fmt.Errorf("tables[%d].name is empty", i)
		}
		if len(cfg.Tables[i].KeyColumns) == 0 {
			return cfg, fmt.Errorf("tables[%d].key_columns empty (need at least 1 unique/PK column)", i)
		}
		if cfg.Tables[i].SyncColumn == "" {
			cfg.Tables[i].SyncColumn = "created_at"
		}
	}
	return cfg, nil
}

// getAllColumns returns ordered column names for a table (schema.table) from information_schema.
func getAllColumns(ctx context.Context, pool *pgxpool.Pool, table string) ([]string, error) {
	schema, name := splitTable(table)
	const q = `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position;
`
	rows, err := pool.Query(ctx, q, schema, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns discovered for %s", table)
	}
	return cols, nil
}

func splitTable(full string) (schema, name string) {
	parts := strings.Split(strings.ToLower(full), ".")
	if len(parts) == 1 {
		return "public", parts[0]
	}
	return parts[0], parts[1]
}

// isPrimaryKeyColumn checks if a column name is likely a primary key ID column
func isPrimaryKeyColumn(col string) bool {
	return strings.EqualFold(col, "id")
}

// isForeignKeyColumn checks if a column name looks like a foreign key (ends with _id)
func isForeignKeyColumn(col string) bool {
	return strings.HasSuffix(strings.ToLower(col), "_id")
}

// buildKeyHash creates a hash key from key column values for mapping lookups
func buildKeyHash(values []any) string {
	var parts []string
	for _, v := range values {
		if v == nil {
			parts = append(parts, "<nil>")
		} else {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
	}
	hash := md5.Sum([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(hash[:])
}

// loadIDMapping builds a mapping cache for a table by loading existing target records
func loadIDMapping(ctx context.Context, target *pgxpool.Pool, tableName string, keyColumns []string) (*IDMapping, error) {
	schema, name := splitTable(tableName)

	// Load mapping for all tables that have an 'id' column
	// (not just those with FK in key_columns, since they might be referenced by other tables)

	// Check if target table has 'id' column
	hasIDCol := false
	cols, err := getAllColumns(ctx, target, tableName)
	if err != nil {
		return nil, fmt.Errorf("get columns for %s: %v", tableName, err)
	}
	for _, col := range cols {
		if isPrimaryKeyColumn(col) {
			hasIDCol = true
			break
		}
	}

	if !hasIDCol {
		return &IDMapping{
			tableName:           tableName,
			keyColumns:          keyColumns,
			mapping:             make(map[string]int64),
			sourceIDToKeyValues: make(map[int64][]any),
		}, nil
	}

	// Load existing mappings from target table
	keyList := strings.Join(quoteCols(keyColumns), ", ")
	q := fmt.Sprintf(`SELECT id, %s FROM %s.%s`, keyList, pq(schema), pq(name))

	rows, err := target.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query target mappings for %s: %v", tableName, err)
	}
	defer rows.Close()

	mapping := make(map[string]int64)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}

		if len(vals) < len(keyColumns)+1 {
			continue
		}

		targetID, ok := vals[0].(int64)
		if !ok {
			// Try different integer types
			switch v := vals[0].(type) {
			case int32:
				targetID = int64(v)
			case int:
				targetID = int64(v)
			default:
				continue
			}
		}

		keyValues := vals[1:]
		keyHash := buildKeyHash(keyValues)
		mapping[keyHash] = targetID
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return &IDMapping{
		tableName:           tableName,
		keyColumns:          keyColumns,
		mapping:             mapping,
		sourceIDToKeyValues: make(map[int64][]any), // Will be populated later
	}, nil
}

// lookupTargetID finds the target ID for given key values, returns 0 if not found
func (im *IDMapping) lookupTargetID(keyValues []any) int64 {
	keyHash := buildKeyHash(keyValues)
	return im.mapping[keyHash]
}

// translateSourceIDToTargetID translates source ID to target ID using key values
func (im *IDMapping) translateSourceIDToTargetID(sourceID int64) int64 {
	// Get key values for this source ID
	keyValues, exists := im.sourceIDToKeyValues[sourceID]
	if !exists {
		log.Printf("Debug: No source mapping found for sourceID=%d", sourceID)
		return 0
	}

	log.Printf("Debug: Found source mapping for sourceID=%d -> keyValues=%v", sourceID, keyValues)

	// Look up target ID using key values
	targetID := im.lookupTargetID(keyValues)
	log.Printf("Debug: lookupTargetID returned targetID=%d for keyValues=%v", targetID, keyValues)
	return targetID
}

// loadSourceIDMapping loads source ID to key values mapping for FK translation
func loadSourceIDMapping(ctx context.Context, source *pgxpool.Pool, tableName string, keyColumns []string) (map[int64][]any, error) {
	schema, name := splitTable(tableName)

	// Check if source table has 'id' column
	cols, err := getAllColumns(ctx, source, tableName)
	if err != nil {
		return nil, fmt.Errorf("get columns for %s: %v", tableName, err)
	}

	hasIDCol := false
	for _, col := range cols {
		if isPrimaryKeyColumn(col) {
			hasIDCol = true
			break
		}
	}

	if !hasIDCol {
		return make(map[int64][]any), nil
	}

	// Load source ID -> key values mapping
	keyList := strings.Join(quoteCols(keyColumns), ", ")
	q := fmt.Sprintf(`SELECT id, %s FROM %s.%s`, keyList, pq(schema), pq(name))

	rows, err := source.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query source mappings for %s: %v", tableName, err)
	}
	defer rows.Close()

	sourceMapping := make(map[int64][]any)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}

		if len(vals) < len(keyColumns)+1 {
			continue
		}

		sourceID, ok := vals[0].(int64)
		if !ok {
			// Try different integer types
			switch v := vals[0].(type) {
			case int32:
				sourceID = int64(v)
			case int:
				sourceID = int64(v)
			default:
				continue
			}
		}

		keyValues := vals[1:]
		sourceMapping[sourceID] = keyValues
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return sourceMapping, nil
}

// updateIDMappingWithInsertedRecords updates the ID mapping cache with newly inserted records
func updateIDMappingWithInsertedRecords(ctx context.Context, target *pgxpool.Pool, tableName string, keyColumns []string, insertedIDs []int64, originalRows [][]any, originalCols []string, translatedRows [][]any, translatedCols []string, idCache IDMappingCache) {
	if len(insertedIDs) == 0 {
		return
	}

	// Find the ID column index in original columns
	idColIdx := -1
	for i, col := range originalCols {
		if isPrimaryKeyColumn(col) {
			idColIdx = i
			break
		}
	}

	if idColIdx == -1 {
		log.Printf("Warning: No ID column found in %s for mapping update", tableName)
		return
	}

	// Get key column indices from translated columns (excluding ID columns)
	keyColIndices := make([]int, 0, len(keyColumns))
	for _, keyCol := range keyColumns {
		for i, col := range translatedCols {
			if strings.EqualFold(col, keyCol) && !isPrimaryKeyColumn(col) {
				keyColIndices = append(keyColIndices, i)
				break
			}
		}
	}

	// Update the mapping cache
	mapping := idCache[tableName]
	if mapping == nil {
		log.Printf("Warning: No mapping found for table %s", tableName)
		return
	}

	updatedCount := 0
	// Match inserted IDs with rows by order
	// The insertedIDs array is in the same order as the rows were inserted
	for i, targetID := range insertedIDs {
		if i >= len(originalRows) || i >= len(translatedRows) {
			log.Printf("Warning: Index %d out of bounds for rows in %s", i, tableName)
			break
		}

		originalRow := originalRows[i]
		translatedRow := translatedRows[i]

		if idColIdx >= len(originalRow) {
			log.Printf("Warning: ID column index out of bounds for row %d in %s", i, tableName)
			continue
		}

		// Get source ID from original row
		sourceIDRaw := originalRow[idColIdx]
		sourceID, ok := sourceIDRaw.(int64)
		if !ok {
			// Try different integer types
			switch v := sourceIDRaw.(type) {
			case int32:
				sourceID = int64(v)
			case int:
				sourceID = int64(v)
			default:
				log.Printf("Warning: Could not convert source ID for row %d in %s: %T", i, tableName, sourceIDRaw)
				continue
			}
		}

		// Extract key values from translated row (excluding ID columns)
		// This is important because the translated row contains the FK values that were actually inserted
		keyValues := make([]any, 0, len(keyColIndices))
		for _, keyIdx := range keyColIndices {
			if keyIdx < len(translatedRow) {
				keyValues = append(keyValues, translatedRow[keyIdx])
			}
		}

		// Update source ID to key values mapping for reverse lookup
		mapping.sourceIDToKeyValues[sourceID] = keyValues

		// Update key hash to target ID mapping
		keyHash := buildKeyHash(keyValues)
		mapping.mapping[keyHash] = targetID
		updatedCount++

		log.Printf("Updated mapping for %s: source_id=%d -> target_id=%d (key_hash=%s)", tableName, sourceID, targetID, keyHash)
	}

	log.Printf("Updated %d mappings for %s with newly inserted records", updatedCount, tableName)
}

// buildIDMappingCache creates ID mapping cache for all referenced tables
func buildIDMappingCache(ctx context.Context, source *pgxpool.Pool, target *pgxpool.Pool, tables []TableConfig) (IDMappingCache, error) {
	cache := make(IDMappingCache)

	// First pass: Load all target mappings
	for _, table := range tables {
		mapping, err := loadIDMapping(ctx, target, table.Name, table.KeyColumns)
		if err != nil {
			return nil, fmt.Errorf("load target ID mapping for %s: %v", table.Name, err)
		}
		cache[table.Name] = mapping
	}

	// Sort tables by dependency order (tables with no FK deps first)
	orderedTables := orderTablesByDependencies(tables)

	// Second pass: Load source mappings in dependency order and apply FK translation
	for _, table := range orderedTables {
		sourceMapping, err := loadSourceIDMapping(ctx, source, table.Name, table.KeyColumns)
		if err != nil {
			return nil, fmt.Errorf("load source ID mapping for %s: %v", table.Name, err)
		}

		// Apply FK translation to source key values
		translatedSourceMapping := make(map[int64][]any)
		for sourceID, keyValues := range sourceMapping {
			// Translate FK values in key columns
			translatedKeyValues := make([]any, len(keyValues))
			copy(translatedKeyValues, keyValues)

			log.Printf("Debug cache build: Table %s, sourceID=%d, original keyValues=%v", table.Name, sourceID, keyValues)

			// Check each key column for FK translation
			for i, keyCol := range table.KeyColumns {
				if i >= len(translatedKeyValues) {
					continue
				}

				log.Printf("Debug cache build: Checking key column %s (index %d) for FK translation: isForeignKeyColumn=%t", keyCol, i, isForeignKeyColumn(keyCol))

				if isForeignKeyColumn(keyCol) {
					// Get the original FK value
					log.Printf("Debug cache build: FK value type for %s: %T, value: %v", keyCol, translatedKeyValues[i], translatedKeyValues[i])
					var fkValue int64
					var ok bool
					// Handle both int32 and int64 types
					switch v := translatedKeyValues[i].(type) {
					case int64:
						fkValue = v
						ok = true
					case int32:
						fkValue = int64(v)
						ok = true
					case int:
						fkValue = int64(v)
						ok = true
					}

					if ok {
						log.Printf("Debug cache build: Translating FK %s=%d in table %s", keyCol, fkValue, table.Name)
						// Determine referenced table
						var referencedTable string
						refTableName := strings.TrimSuffix(strings.ToLower(keyCol), "_id")
						switch refTableName {
						case "company":
							referencedTable = "public.company"
						case "report":
							referencedTable = "public.report"
						case "report_ip":
							referencedTable = "public.report_ip"
						default:
							// Try to find matching table
							for tableName := range cache {
								if strings.Contains(strings.ToLower(tableName), refTableName) {
									referencedTable = tableName
									break
								}
							}
						}

						log.Printf("Debug cache build: Referenced table for %s is %s", keyCol, referencedTable)

						// Translate FK if referenced table exists in cache and has source mapping
						if referencedTable != "" {
							if refMapping, exists := cache[referencedTable]; exists && refMapping.sourceIDToKeyValues != nil {
								if targetID := refMapping.translateSourceIDToTargetID(fkValue); targetID > 0 {
									log.Printf("Debug cache build: Translated %s: %d -> %d", keyCol, fkValue, targetID)
									translatedKeyValues[i] = targetID
								} else {
									log.Printf("Debug cache build: Translation failed for %s=%d", keyCol, fkValue)
								}
							} else {
								log.Printf("Debug cache build: No mapping found for referenced table %s or source mapping not yet built", referencedTable)
							}
						}
					}
				}
			}
			log.Printf("Debug cache build: Table %s, sourceID=%d, translated keyValues=%v", table.Name, sourceID, translatedKeyValues)
			translatedSourceMapping[sourceID] = translatedKeyValues
		}

		cache[table.Name].sourceIDToKeyValues = translatedSourceMapping
	}

	return cache, nil
}

// orderTablesByDependencies sorts tables so that tables with no FK dependencies come first
func orderTablesByDependencies(tables []TableConfig) []TableConfig {
	// Simple heuristic: put tables without FK columns first
	var independent []TableConfig
	var dependent []TableConfig

	for _, table := range tables {
		hasFKCols := false
		for _, keyCol := range table.KeyColumns {
			if isForeignKeyColumn(keyCol) {
				hasFKCols = true
				break
			}
		}

		if hasFKCols {
			dependent = append(dependent, table)
		} else {
			independent = append(independent, table)
		}
	}

	// Return independent tables first, then dependent ones
	result := append(independent, dependent...)
	return result
}

func ensureMetaTable(ctx context.Context, target *pgxpool.Pool) error {
	_, err := target.Exec(ctx, metaTableDDL)
	return err
}

func getLastSync(ctx context.Context, target *pgxpool.Pool, table string) (time.Time, error) {
	const q = `SELECT last_sync_ts FROM public.sync_meta WHERE table_name = $1;`
	var ts time.Time
	err := target.QueryRow(ctx, q, table).Scan(&ts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Unix(0, 0).UTC(), nil // epoch
		}
		return time.Time{}, err
	}
	return ts.UTC(), nil
}

func setLastSync(ctx context.Context, target *pgxpool.Pool, table string, ts time.Time) error {
	const q = `
INSERT INTO public.sync_meta(table_name, last_sync_ts)
VALUES ($1, $2)
ON CONFLICT (table_name) DO UPDATE SET last_sync_ts = EXCLUDED.last_sync_ts;
`
	_, err := target.Exec(ctx, q, table, ts.UTC())
	return err
}

// filterColumnsForInsert excludes primary key ID columns from insert columns
func filterColumnsForInsert(cols []string) []string {
	var filtered []string
	for _, col := range cols {
		if !isPrimaryKeyColumn(col) {
			filtered = append(filtered, col)
		}
	}
	return filtered
}

// translateForeignKeys translates foreign key IDs using the ID mapping cache
func translateForeignKeys(row []any, cols []string, keyColumns []string, idCache IDMappingCache, currentTable string) ([]any, error) {
	// Create a copy of the row
	translatedRow := make([]any, len(row))
	copy(translatedRow, row)

	// For each foreign key column in key_columns, try to translate its value
	for _, keyCol := range keyColumns {
		if !isForeignKeyColumn(keyCol) {
			continue
		}

		// Find the column index
		colIdx := -1
		for i, col := range cols {
			if strings.EqualFold(col, keyCol) {
				colIdx = i
				break
			}
		}

		if colIdx == -1 || translatedRow[colIdx] == nil {
			continue
		}

		// Get source foreign key ID
		sourceID, ok := translatedRow[colIdx].(int64)
		if !ok {
			// Try different integer types
			switch v := translatedRow[colIdx].(type) {
			case int32:
				sourceID = int64(v)
			case int:
				sourceID = int64(v)
			default:
				continue
			}
		}

		// Determine the referenced table name by removing _id suffix
		refTableName := strings.TrimSuffix(strings.ToLower(keyCol), "_id")

		// Map common foreign key patterns to actual table names
		var referencedTable string
		switch refTableName {
		case "company":
			referencedTable = "public.company"
		case "report":
			referencedTable = "public.report"
		case "report_ip":
			referencedTable = "public.report_ip"
		case "report_email":
			referencedTable = "public.report_email"
		case "encrypt_protocol":
			referencedTable = "public.encrypt_protocol"
		case "vulnerability":
			referencedTable = "public.vulnerability"
		case "search_loc":
			referencedTable = "public.search_loc"
		case "leak":
			referencedTable = "public.leak"
		default:
			// Try to find a matching table in the cache
			found := false
			for tableName := range idCache {
				if strings.Contains(strings.ToLower(tableName), refTableName) {
					referencedTable = tableName
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Get the mapping for the referenced table
		mapping, exists := idCache[referencedTable]
		if !exists {
			continue
		}

		// Debug FK translation process
		log.Printf("Debug: Attempting to translate FK %s=%d in table %s (refs %s)", keyCol, sourceID, currentTable, referencedTable)
		log.Printf("Debug: Referenced table mapping has %d source entries, %d target entries",
			len(mapping.sourceIDToKeyValues), len(mapping.mapping))

		// Translate source ID to target ID
		targetID := mapping.translateSourceIDToTargetID(sourceID)
		if targetID > 0 {
			translatedRow[colIdx] = targetID
			log.Printf("Translated FK %s: %d -> %d (table %s -> %s)",
				keyCol, sourceID, targetID, currentTable, referencedTable)
		} else {
			// Debug why translation failed
			keyValues, hasSourceMapping := mapping.sourceIDToKeyValues[sourceID]
			if hasSourceMapping {
				keyHash := buildKeyHash(keyValues)
				log.Printf("Debug: Found source mapping for ID %d -> key_values=%v (hash=%s)", sourceID, keyValues, keyHash)
				if targetIDFromHash, hasTargetMapping := mapping.mapping[keyHash]; hasTargetMapping {
					log.Printf("Debug: Found target mapping for hash %s -> target_id=%d", keyHash, targetIDFromHash)
				} else {
					log.Printf("Debug: No target mapping found for hash %s", keyHash)
				}
			} else {
				log.Printf("Debug: No source mapping found for source_id=%d", sourceID)
			}
			log.Printf("Warning: Could not translate FK %s=%d in table %s (referenced table %s)",
				keyCol, sourceID, currentTable, referencedTable)
		}
	}

	return translatedRow, nil
}

// BatchResult contains both filtered rows for insert and original rows for timestamp tracking
type BatchResult struct {
	FilteredRows [][]any  // Rows with ID columns removed for insert
	OriginalRows [][]any  // Original rows for timestamp calculation
	InsertCols   []string // Column names after filtering out ID columns
	OriginalCols []string // Original column names
}

// fetchBatch pulls a batch of rows > lastTs ordered by (syncCol, keyColumns[0]) for stable paging.
func fetchBatch(ctx context.Context, src *pgxpool.Pool, table string, cols []string, syncCol string, keyColumns []string, lastTs time.Time, limit int, idCache IDMappingCache) (*BatchResult, error) {
	// Filter out ID columns for the insert
	insertCols := filterColumnsForInsert(cols)

	// Build SELECT list and query - still select all columns to have complete data for translation
	colList := strings.Join(quoteCols(cols), ", ")
	orderBy := fmt.Sprintf("%s, %s", pq(syncCol), pq(keyColumns[0]))
	schema, name := splitTable(table)

	q := fmt.Sprintf(`SELECT %s FROM %s.%s WHERE %s > $1 ORDER BY %s LIMIT $2;`,
		colList, pq(schema), pq(name), pq(syncCol), orderBy)

	rows, err := src.Query(ctx, q, lastTs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var filteredRows [][]any
	var originalRows [][]any

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}

		// Keep original for timestamp calculation
		originalRows = append(originalRows, vals)

		// Translate foreign keys
		translatedVals, err := translateForeignKeys(vals, cols, keyColumns, idCache, table)
		if err != nil {
			return nil, fmt.Errorf("translate foreign keys: %v", err)
		}

		// Filter out ID columns for insert
		filteredVals := make([]any, 0, len(insertCols))
		for i, col := range cols {
			if !isPrimaryKeyColumn(col) {
				filteredVals = append(filteredVals, translatedVals[i])
			}
		}

		filteredRows = append(filteredRows, filteredVals)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return &BatchResult{
		FilteredRows: filteredRows,
		OriginalRows: originalRows,
		InsertCols:   insertCols,
		OriginalCols: cols,
	}, nil
}

// InsertResult contains information about inserted records
type InsertResult struct {
	RowsAffected int64
	InsertedIDs  []int64 // Only for tables with ID columns
}

// insertBatch inserts rows with proper error logging and ID tracking.
func insertBatch(ctx context.Context, dst *pgxpool.Pool, table string, cols []string, keyColumns []string, rowsVals [][]any, originalCols []string, dryRun bool) (*InsertResult, time.Time, error) {
	if len(rowsVals) == 0 {
		return &InsertResult{}, time.Time{}, nil
	}
	// Build INSERT statement with multi-row values.
	schema, name := splitTable(table)
	colList := strings.Join(quoteCols(cols), ", ")
	conflict := strings.Join(quoteCols(keyColumns), ", ")

	// Check if table has ID column for RETURNING clause (use original columns, not filtered ones)
	hasIDCol := false
	for _, col := range originalCols {
		if isPrimaryKeyColumn(col) {
			hasIDCol = true
			break
		}
	}

	// Build placeholders ($1...$N) for each row.
	var (
		placeholders []string
		args         []any
	)
	argPos := 1
	for _, row := range rowsVals {
		ph := make([]string, len(row))
		for i := range row {
			ph[i] = fmt.Sprintf("$%d", argPos)
			args = append(args, row[i])
			argPos++
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(ph, ", ")))
	}

	// Build query with RETURNING clause if table has ID
	var q string
	if hasIDCol {
		q = fmt.Sprintf(`INSERT INTO %s.%s (%s) VALUES %s ON CONFLICT (%s) DO NOTHING RETURNING id;`,
			pq(schema), pq(name), colList, strings.Join(placeholders, ", "), conflict)
	} else {
		q = fmt.Sprintf(`INSERT INTO %s.%s (%s) VALUES %s ON CONFLICT (%s) DO NOTHING;`,
			pq(schema), pq(name), colList, strings.Join(placeholders, ", "), conflict)
	}

	if dryRun {
		log.Printf("DRY_RUN insert %d rows into %s.%s", len(rowsVals), schema, name)
		if hasIDCol {
			log.Printf("DRY_RUN query: %s", q)
		}
		return &InsertResult{RowsAffected: int64(len(rowsVals))}, time.Time{}, nil
	}

	result := &InsertResult{}

	if hasIDCol {
		// Execute with RETURNING to get actual inserted IDs
		rows, err := dst.Query(ctx, q, args...)
		if err != nil {
			log.Printf("ERROR inserting into %s.%s: %v", schema, name, err)
			log.Printf("Failed query: %s", q)
			return nil, time.Time{}, fmt.Errorf("insert into %s.%s failed: %v", schema, name, err)
		}
		defer rows.Close()

		var insertedIDs []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				log.Printf("ERROR scanning inserted ID from %s.%s: %v", schema, name, err)
				return nil, time.Time{}, fmt.Errorf("scan inserted ID failed: %v", err)
			}
			insertedIDs = append(insertedIDs, id)
		}
		if rows.Err() != nil {
			log.Printf("ERROR reading inserted IDs from %s.%s: %v", schema, name, rows.Err())
			return nil, time.Time{}, fmt.Errorf("read inserted IDs failed: %v", rows.Err())
		}

		result.RowsAffected = int64(len(insertedIDs))
		result.InsertedIDs = insertedIDs
		log.Printf("Successfully inserted %d rows into %s.%s with IDs: %v", len(insertedIDs), schema, name, insertedIDs)
	} else {
		// Execute without RETURNING for tables without ID
		ct, err := dst.Exec(ctx, q, args...)
		if err != nil {
			log.Printf("ERROR inserting into %s.%s: %v", schema, name, err)
			log.Printf("Failed query: %s", q)
			return nil, time.Time{}, fmt.Errorf("insert into %s.%s failed: %v", schema, name, err)
		}
		result.RowsAffected = ct.RowsAffected()
		log.Printf("Successfully inserted %d rows into %s.%s (no ID tracking)", result.RowsAffected, schema, name)
	}

	return result, time.Time{}, nil
}

// maxSyncTs scans rows to get the maximum value of sync column by name.
func maxSyncTs(rows [][]any, cols []string, syncCol string) (time.Time, error) {
	idx := -1
	for i, c := range cols {
		if strings.EqualFold(c, syncCol) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return time.Time{}, fmt.Errorf("sync column %s not found in cols", syncCol)
	}
	var maxT time.Time
	for _, r := range rows {
		// Support timestamptz/time types returned by pgx.
		switch v := r[idx].(type) {
		case time.Time:
			if v.After(maxT) {
				maxT = v
			}
		default:
			return time.Time{}, fmt.Errorf("sync column %s is not a timestamp (got %T)", syncCol, v)
		}
	}
	return maxT, nil
}

func pq(ident string) string {
	// Very light identifier quoting (no embedded quotes expected in normal identifiers)
	return fmt.Sprintf("\"%s\"", strings.ReplaceAll(ident, "\"", ""))
}

func quoteCols(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = pq(c)
	}
	return out
}

func main() {
	ctx := context.Background()

	sourceDSN := mustEnv("SOURCE_DSN")
	targetDSN := mustEnv("TARGET_DSN")
	cfgPath := getenv("CONFIG_PATH", "./config.json")
	batchSizeStr := getenv("BATCH_SIZE", "1000")
	dryRun := strings.EqualFold(getenv("DRY_RUN", "false"), "true")
	resetSync := strings.EqualFold(getenv("RESET_SYNC", "false"), "true")

	batchSize64, err := strconv.ParseInt(batchSizeStr, 10, 32)
	if err != nil || batchSize64 <= 0 {
		log.Fatalf("invalid BATCH_SIZE: %s", batchSizeStr)
	}
	batchSize := int(batchSize64)

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	sp, err := pgxpool.New(ctx, sourceDSN)
	if err != nil {
		log.Fatalf("connect source: %v", err)
	}
	defer sp.Close()

	tp, err := pgxpool.New(ctx, targetDSN)
	if err != nil {
		log.Fatalf("connect target: %v", err)
	}
	defer tp.Close()

	if err := ensureMetaTable(ctx, tp); err != nil {
		log.Fatalf("ensure meta table: %v", err)
	}

	// Build ID mapping cache for foreign key translation
	log.Printf("Building ID mapping cache...")
	idCache, err := buildIDMappingCache(ctx, sp, tp, cfg.Tables)
	if err != nil {
		log.Fatalf("build ID mapping cache: %v", err)
	}
	log.Printf("ID mapping cache built for %d tables", len(idCache))

	// Debug: Print mapping details for company table
	if companyMapping, exists := idCache["public.company"]; exists {
		log.Printf("Company mapping - Target mappings: %d entries", len(companyMapping.mapping))
		for keyHash, targetID := range companyMapping.mapping {
			log.Printf("  Target: key_hash=%s -> target_id=%d", keyHash, targetID)
		}
		log.Printf("Company mapping - Source mappings: %d entries", len(companyMapping.sourceIDToKeyValues))
		for sourceID, keyValues := range companyMapping.sourceIDToKeyValues {
			log.Printf("  Source: source_id=%d -> key_values=%v", sourceID, keyValues)
		}
	}

	// Reset sync watermarks if requested
	if resetSync {
		log.Printf("Resetting sync watermarks...")
		for _, table := range cfg.Tables {
			if err := setLastSync(ctx, tp, table.Name, time.Unix(0, 0).UTC()); err != nil {
				log.Fatalf("reset sync for %s: %v", table.Name, err)
			}
		}
		log.Printf("Sync watermarks reset")
	}

	start := time.Now()
	totalInserted := int64(0)

	for _, t := range cfg.Tables {
		log.Printf("=== Table %s (sync by %s; keys=%v) ===", t.Name, t.SyncColumn, t.KeyColumns)
		lastTs, err := getLastSync(ctx, tp, t.Name)
		if err != nil {
			log.Fatalf("get last sync for %s: %v", t.Name, err)
		}
		log.Printf("last_sync_ts: %s", lastTs.Format(time.RFC3339))

		cols, err := getAllColumns(ctx, sp, t.Name)
		if err != nil {
			log.Fatalf("discover columns for %s: %v", t.Name, err)
		}
		// Quick sanity: ensure sync column exists
		found := false
		for _, c := range cols {
			if strings.EqualFold(c, t.SyncColumn) {
				found = true
				break
			}
		}
		if !found {
			log.Fatalf("table %s missing sync_column %s", t.Name, t.SyncColumn)
		}

		// Page through data
		for {
			batchResult, err := fetchBatch(ctx, sp, t.Name, cols, t.SyncColumn, t.KeyColumns, lastTs, batchSize, idCache)
			if err != nil {
				log.Fatalf("fetch batch %s: %v", t.Name, err)
			}
			if len(batchResult.FilteredRows) == 0 {
				log.Printf("no more new rows for %s", t.Name)
				break
			}

			// Insert in chunks to avoid too many bind params in one statement
			// Postgres has a limit ~65535 parameters; estimate per-row params = len(insertCols)
			maxRows := int(math.Max(1, math.Min(float64(len(batchResult.FilteredRows)), 60000.0/float64(len(batchResult.InsertCols)))))
			offset := 0
			var lastBatchTs time.Time
			for offset < len(batchResult.FilteredRows) {
				end := offset + maxRows
				if end > len(batchResult.FilteredRows) {
					end = len(batchResult.FilteredRows)
				}
				filteredSub := batchResult.FilteredRows[offset:end]
				originalSub := batchResult.OriginalRows[offset:end]

				// Filter key columns to remove ID columns for conflict resolution
				filteredKeyColumns := make([]string, 0, len(t.KeyColumns))
				for _, keyCol := range t.KeyColumns {
					if !isPrimaryKeyColumn(keyCol) {
						filteredKeyColumns = append(filteredKeyColumns, keyCol)
					}
				}

				insertResult, _, err := insertBatch(ctx, tp, t.Name, batchResult.InsertCols, filteredKeyColumns, filteredSub, batchResult.OriginalCols, dryRun)
				if err != nil {
					log.Fatalf("insert batch into %s: %v", t.Name, err)
				}
				totalInserted += insertResult.RowsAffected

				// Update ID mapping cache with actual inserted IDs
				if len(insertResult.InsertedIDs) > 0 {
					// For each inserted ID, we need to map it back to the source ID
					// Pass both original and translated data to correlate source IDs with translated key values
					updateIDMappingWithInsertedRecords(ctx, tp, t.Name, t.KeyColumns, insertResult.InsertedIDs, originalSub, batchResult.OriginalCols, filteredSub, batchResult.InsertCols, idCache)
				}

				// Use original rows for timestamp calculation
				ts, err := maxSyncTs(originalSub, batchResult.OriginalCols, t.SyncColumn)
				if err != nil {
					log.Fatalf("maxSyncTs: %v", err)
				}
				if ts.After(lastBatchTs) {
					lastBatchTs = ts
				}

				offset = end
			}

			if !dryRun && !lastBatchTs.IsZero() {
				if err := setLastSync(ctx, tp, t.Name, lastBatchTs); err != nil {
					log.Fatalf("set last sync for %s: %v", t.Name, err)
				}
				lastTs = lastBatchTs
				log.Printf("%s: advanced last_sync_ts -> %s", t.Name, lastTs.Format(time.RFC3339))
			}

			// If batch was smaller than requested, likely no more rows
			if len(batchResult.FilteredRows) < batchSize {
				break
			}
		}

		// Reload target mapping for this table after sync (for FK references)
		log.Printf("Reloading target mapping for %s...", t.Name)
		updatedMapping, err := loadIDMapping(ctx, tp, t.Name, t.KeyColumns)
		if err != nil {
			log.Printf("Warning: could not reload mapping for %s: %v", t.Name, err)
		} else {
			// Preserve the updated sourceIDToKeyValues mapping from cache
			// Don't overwrite it with old data - updateIDMappingWithInsertedRecords already updated it
			updatedMapping.sourceIDToKeyValues = idCache[t.Name].sourceIDToKeyValues
			idCache[t.Name] = updatedMapping
			log.Printf("Reloaded mapping for %s: %d target entries", t.Name, len(updatedMapping.mapping))

			// Debug: show some mappings for verification
			if len(updatedMapping.mapping) <= 10 {
				for keyHash, targetID := range updatedMapping.mapping {
					log.Printf("  Target mapping: key_hash=%s -> target_id=%d", keyHash, targetID)
				}
			}
		}
	}

	dur := time.Since(start)
	log.Printf("DONE. Inserted %d rows in %s (dry_run=%v)", totalInserted, dur, dryRun)
}
