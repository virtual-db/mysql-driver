// Package schema provides table schema resolution backed by the source MySQL
// database's INFORMATION_SCHEMA. It is internal to the driver module; no
// external package should import it directly.
package schema

import (
	"database/sql"
	"fmt"
)

// ColumnDescriptor carries the full per-column metadata returned by
// INFORMATION_SCHEMA.COLUMNS for a single column. All fields map directly to
// INFORMATION_SCHEMA column names.
type ColumnDescriptor struct {
	// Name is the COLUMN_NAME — e.g. "id", "username".
	Name string

	// DataType is the bare DATA_TYPE without length or precision — e.g. "int",
	// "varchar", "decimal". Use this for type-family switching in
	// columnTypeToGMS.
	DataType string

	// ColumnType is the full COLUMN_TYPE including any length/precision/scale
	// qualifier — e.g. "varchar(64)", "decimal(10,2)", "int". Useful when
	// DataType alone is ambiguous (e.g. "int" vs "int unsigned").
	ColumnType string

	// IsNullable is the IS_NULLABLE value: "YES" or "NO".
	IsNullable string

	// ColumnKey is the COLUMN_KEY value: "PRI", "UNI", "MUL", or "".
	ColumnKey string

	// ColumnDefault is the COLUMN_DEFAULT value. nil means the column has no
	// declared DEFAULT clause. A non-nil pointer to an empty string means the
	// column was declared DEFAULT ''.
	ColumnDefault *string

	// Extra is the EXTRA value — e.g. "auto_increment",
	// "on update CURRENT_TIMESTAMP", or "".
	Extra string

	// CharMaxLength is the CHARACTER_MAXIMUM_LENGTH value for character-type
	// columns (VARCHAR, CHAR, TEXT variants). nil for non-character types.
	CharMaxLength *int64
}

// Provider queries the source database for table column metadata.
// GMS invokes the provider during query planning whenever it needs to
// understand the shape of a table.
//
// Caching is deliberately deferred. The correct invalidation signal is
// api.SchemaInvalidated(), a vdb-core concern; a caching layer is added by
// wrapping Provider with a NotifyingProvider or a future caching decorator.
//
// All implementations must be safe for concurrent use.
type Provider interface {
	// GetSchema returns full column descriptors and the primary-key column
	// (first column in the PRIMARY KEY constraint by ordinal position) for the
	// given table. Returns a non-nil error if the table does not exist.
	//
	// Limitation: composite primary keys are not supported. Only the first
	// primary-key column by ordinal position is returned.
	GetSchema(table string) (columns []ColumnDescriptor, pkCol string, err error)
}

// queryColumns retrieves full column metadata for a table in ordinal order.
const queryColumns = `
    SELECT
        COLUMN_NAME,
        DATA_TYPE,
        COLUMN_TYPE,
        IS_NULLABLE,
        COLUMN_KEY,
        COLUMN_DEFAULT,
        EXTRA,
        CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION`

// queryPrimaryKey retrieves the first primary-key column by ordinal position.
//
// TODO: composite PK — this query returns only the first column of a composite
// PRIMARY KEY. When composite key support is added to the framework interface,
// update this query to return all key columns in order.
const queryPrimaryKey = `
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      AND CONSTRAINT_NAME = 'PRIMARY'
    ORDER BY ORDINAL_POSITION
    LIMIT 1`

// SQLProvider implements Provider by querying INFORMATION_SCHEMA on the source
// MySQL database.
//
// The *sql.DB is opened by the caller and passed in. SQLProvider does not open
// or close the connection. *sql.DB is internally thread-safe, so GetSchema is
// safe for concurrent use without additional locking.
type SQLProvider struct {
	db     *sql.DB
	dbName string
}

// NewSQLProvider constructs a Provider backed by db, scoped to dbName.
// db must already be open; NewSQLProvider performs no I/O.
func NewSQLProvider(db *sql.DB, dbName string) *SQLProvider {
	return &SQLProvider{db: db, dbName: dbName}
}

// GetSchema queries INFORMATION_SCHEMA for the full column descriptors and
// primary-key column of the given table.
//
// Error conditions:
//   - Table not found: returns a descriptive non-nil error. An empty column
//     list is never returned without an error.
//   - No PRIMARY KEY: returns pkCol = "" and err = nil. A table without a
//     PRIMARY KEY is valid in MySQL.
//   - Database errors: wrapped with %w and returned.
//
// GetSchema is safe for concurrent use.
func (p *SQLProvider) GetSchema(table string) ([]ColumnDescriptor, string, error) {
	rows, err := p.db.Query(queryColumns, p.dbName, table)
	if err != nil {
		return nil, "", fmt.Errorf("schema: query for table %q in %q: %w", table, p.dbName, err)
	}
	defer rows.Close()

	var columns []ColumnDescriptor
	for rows.Next() {
		var col ColumnDescriptor
		if err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.ColumnType,
			&col.IsNullable,
			&col.ColumnKey,
			&col.ColumnDefault,
			&col.Extra,
			&col.CharMaxLength,
		); err != nil {
			return nil, "", fmt.Errorf("schema: scan column for table %q: %w", table, err)
		}
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("schema: iterating columns for table %q: %w", table, err)
	}
	if len(columns) == 0 {
		return nil, "", fmt.Errorf("schema: table %q not found in schema %q", table, p.dbName)
	}

	var pkCol string
	err = p.db.QueryRow(queryPrimaryKey, p.dbName, table).Scan(&pkCol)
	if err == sql.ErrNoRows {
		pkCol = ""
	} else if err != nil {
		return nil, "", fmt.Errorf("schema: primary key query for table %q in %q: %w", table, p.dbName, err)
	}

	return columns, pkCol, nil
}
