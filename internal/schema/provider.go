// Package schema provides table schema resolution backed by the source MySQL
// database's INFORMATION_SCHEMA. It is internal to the driver module; no
// external package should import it directly.
package schema

import (
	"database/sql"
	"fmt"
)

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
	// GetSchema returns the ordered column names and the primary-key column
	// (first column in the PRIMARY KEY constraint by ordinal position) for the
	// given table. Returns a non-nil error if the table does not exist.
	//
	// Limitation: composite primary keys are not supported. Only the first
	// primary-key column by ordinal position is returned.
	GetSchema(table string) (columns []string, pkCol string, err error)
}

// queryColumns retrieves column names for a table in ordinal order.
const queryColumns = `
    SELECT COLUMN_NAME
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

// GetSchema queries INFORMATION_SCHEMA for the column names and primary-key
// column of the given table.
//
// Error conditions:
//   - Table not found: returns a descriptive non-nil error. An empty column
//     list is never returned without an error.
//   - No PRIMARY KEY: returns pkCol = "" and err = nil. A table without a
//     PRIMARY KEY is valid in MySQL.
//   - Database errors: wrapped with %w and returned.
//
// GetSchema is safe for concurrent use.
func (p *SQLProvider) GetSchema(table string) ([]string, string, error) {
	rows, err := p.db.Query(queryColumns, p.dbName, table)
	if err != nil {
		return nil, "", fmt.Errorf("schema: query for table %q in %q: %w", table, p.dbName, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, "", fmt.Errorf("schema: scan column name for table %q: %w", table, err)
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
