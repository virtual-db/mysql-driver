package driver

import (
	"database/sql"
	"fmt"

	gmssql "github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
)

// schemaProvider queries the source database for table column metadata.
// GMS invokes the schema provider during query planning whenever it needs to
// understand the shape of a table.
//
// Caching is deliberately deferred. The correct invalidation signal is
// api.SchemaInvalidated(), a vdb-core concern; a caching layer will be added
// by wrapping schemaProvider with an invalidation-aware decorator.
//
// All implementations must be safe for concurrent use.
type schemaProvider interface {
	// GetSchema returns the ordered column names and the primary-key column
	// (first column in the PRIMARY KEY constraint by ordinal position) for the
	// given table. Returns a non-nil error if the table does not exist.
	//
	// Limitation: composite primary keys are not supported. Only the first
	// primary-key column by ordinal position is returned.
	GetSchema(table string) (columns []string, pkCol string, err error)
}

// ---------------------------------------------------------------------------
// wrappedSchemaProvider
// ---------------------------------------------------------------------------

// wrappedSchemaProvider delegates GetSchema to an inner schemaProvider and
// additionally calls onLoad with the result, allowing SetDriverAPI to forward
// each successful schema load to core.DriverAPI.SchemaLoaded without the
// internal GMS layer knowing anything about vdb-core.
type wrappedSchemaProvider struct {
	inner  schemaProvider
	onLoad func(table string, cols []string, pkCol string)
}

// GetSchema satisfies schemaProvider.
func (w *wrappedSchemaProvider) GetSchema(table string) ([]string, string, error) {
	cols, pkCol, err := w.inner.GetSchema(table)
	if err != nil {
		return nil, "", err
	}
	if w.onLoad != nil {
		w.onLoad(table, cols, pkCol)
	}
	return cols, pkCol, nil
}

// ---------------------------------------------------------------------------
// sqlSchemaProvider
// ---------------------------------------------------------------------------

// sqlSchemaProvider implements schemaProvider by querying INFORMATION_SCHEMA
// on the source MySQL database.
//
// The *sql.DB is opened by the caller (SetDriverAPI) and passed in.
// sqlSchemaProvider does not open or close the connection. *sql.DB is
// internally thread-safe, so GetSchema is safe for concurrent use without
// additional locking.
type sqlSchemaProvider struct {
	db     *sql.DB
	dbName string
}

// newSQLSchemaProvider constructs a schemaProvider backed by db, scoped to
// dbName. db must already be open; newSQLSchemaProvider performs no I/O.
func newSQLSchemaProvider(db *sql.DB, dbName string) schemaProvider {
	return &sqlSchemaProvider{db: db, dbName: dbName}
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
func (p *sqlSchemaProvider) GetSchema(table string) ([]string, string, error) {
	rows, err := p.db.Query(queryColumns, p.dbName, table)
	if err != nil {
		return nil, "", fmt.Errorf("driver: schema query for table %q in %q: %w", table, p.dbName, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, "", fmt.Errorf("driver: scan column name for table %q: %w", table, err)
		}
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("driver: iterating columns for table %q: %w", table, err)
	}
	if len(columns) == 0 {
		return nil, "", fmt.Errorf("driver: table %q not found in schema %q", table, p.dbName)
	}

	var pkCol string
	err = p.db.QueryRow(queryPrimaryKey, p.dbName, table).Scan(&pkCol)
	if err == sql.ErrNoRows {
		pkCol = ""
	} else if err != nil {
		return nil, "", fmt.Errorf("driver: primary key query for table %q in %q: %w", table, p.dbName, err)
	}

	return columns, pkCol, nil
}

// ---------------------------------------------------------------------------
// toGMSSchema
// ---------------------------------------------------------------------------

// toGMSSchema converts a column name list to a GMS sql.Schema. All columns
// are typed as LongText; GMS coerces values at execution time. Precise type
// mapping via INFORMATION_SCHEMA.COLUMNS.DATA_TYPE is a later optimisation.
func toGMSSchema(table string, columns []string) gmssql.Schema {
	schema := make(gmssql.Schema, len(columns))
	for i, col := range columns {
		schema[i] = &gmssql.Column{
			Name:     col,
			Type:     gmstypes.LongText,
			Source:   table,
			Nullable: true,
		}
	}
	return schema
}
