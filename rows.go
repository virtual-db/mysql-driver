package driver

import (
	"fmt"
	"io"

	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// rowProvider translates GMS row representations into the map[string]any form
// expected by the callbacks struct, and invokes the appropriate callback at
// each row lifecycle moment.
//
// All methods operate without knowledge of vdb-core, pipelines, or events.
type rowProvider interface {
	// FetchRows is called after rows have been read from the source database.
	FetchRows(ctx *gmssql.Context, table string, rows []gmssql.Row, schema gmssql.Schema) ([]map[string]any, error)

	// CommitRows is called after the delta overlay has been applied and the
	// final row set is ready to return to the client.
	CommitRows(ctx *gmssql.Context, table string, records []map[string]any) ([]map[string]any, error)

	// InsertRow is called when GMS executes an INSERT for a single row.
	InsertRow(ctx *gmssql.Context, table string, row gmssql.Row, schema gmssql.Schema) (map[string]any, error)

	// UpdateRow is called when GMS executes an UPDATE, providing both the old
	// and new row values.
	UpdateRow(ctx *gmssql.Context, table string, old, new gmssql.Row, schema gmssql.Schema) (map[string]any, error)

	// DeleteRow is called when GMS executes a DELETE for a single row.
	DeleteRow(ctx *gmssql.Context, table string, row gmssql.Row, schema gmssql.Schema) error
}

// gmsRowProvider is the concrete rowProvider implementation. It holds a
// schemaProvider for column-name resolution and a callbacks value for
// callback invocation.
type gmsRowProvider struct {
	schema schemaProvider
	cbs    callbacks // value copy; function values are reference types internally
}

// newGMSRowProvider constructs a rowProvider backed by schema and cbs.
// cbs must be fully populated; nil function fields panic at invocation time.
// validateCallbacks in SetDriverAPI ensures this never occurs in a correctly-
// wired process.
func newGMSRowProvider(schema schemaProvider, cbs callbacks) rowProvider {
	return &gmsRowProvider{schema: schema, cbs: cbs}
}

// ---------------------------------------------------------------------------
// Row conversion helpers
// ---------------------------------------------------------------------------

// rowToMap converts a GMS sql.Row ([]interface{}) into a map[string]any using
// the ordered column name slice.
//
// Precondition: len(cols) == len(row). A mismatch is a programming error in
// schema resolution and panics rather than silently producing corrupt data.
func rowToMap(row gmssql.Row, cols []string) map[string]any {
	if len(row) != len(cols) {
		panic(fmt.Sprintf(
			"driver: rowToMap: column count mismatch: got %d columns, want %d",
			len(row), len(cols),
		))
	}
	m := make(map[string]any, len(cols))
	for i, col := range cols {
		m[col] = row[i]
	}
	return m
}

// schemaColumns extracts the ordered column name slice from a GMS sql.Schema.
func schemaColumns(schema gmssql.Schema) []string {
	cols := make([]string, len(schema))
	for i, col := range schema {
		cols[i] = col.Name
	}
	return cols
}

// mapToRow converts a map[string]any back to a GMS sql.Row using the ordered
// column name slice. It is the exact inverse of rowToMap.
//
// Precondition: cols ordering must match the table schema's column ordering.
// Any transposition produces silently corrupt data.
func mapToRow(record map[string]any, cols []string) gmssql.Row {
	row := make(gmssql.Row, len(cols))
	for i, col := range cols {
		row[i] = record[col]
	}
	return row
}

// ---------------------------------------------------------------------------
// gmsRowProvider method implementations
// ---------------------------------------------------------------------------

// FetchRows converts raw GMS rows to []map[string]any and invokes rowsFetched.
func (p *gmsRowProvider) FetchRows(
	ctx *gmssql.Context, table string, rows []gmssql.Row, schema gmssql.Schema,
) ([]map[string]any, error) {
	cols := schemaColumns(schema)
	records := make([]map[string]any, len(rows))
	for i, row := range rows {
		records[i] = rowToMap(row, cols)
	}
	if p.cbs.rowsFetched == nil {
		return records, nil
	}
	return p.cbs.rowsFetched(connIDFromCtx(ctx), table, records)
}

// CommitRows invokes rowsReady with the final record set.
func (p *gmsRowProvider) CommitRows(
	ctx *gmssql.Context, table string, records []map[string]any,
) ([]map[string]any, error) {
	if p.cbs.rowsReady == nil {
		return records, nil
	}
	return p.cbs.rowsReady(connIDFromCtx(ctx), table, records)
}

// InsertRow converts the row to a record and invokes rowInserted.
func (p *gmsRowProvider) InsertRow(
	ctx *gmssql.Context, table string, row gmssql.Row, schema gmssql.Schema,
) (map[string]any, error) {
	if p.cbs.rowInserted == nil {
		return nil, nil
	}
	record := rowToMap(row, schemaColumns(schema))
	return p.cbs.rowInserted(connIDFromCtx(ctx), table, record)
}

// UpdateRow converts old and new rows to records and invokes rowUpdated.
func (p *gmsRowProvider) UpdateRow(
	ctx *gmssql.Context, table string, old, new gmssql.Row, schema gmssql.Schema,
) (map[string]any, error) {
	if p.cbs.rowUpdated == nil {
		return nil, nil
	}
	cols := schemaColumns(schema)
	oldRec := rowToMap(old, cols)
	newRec := rowToMap(new, cols)
	return p.cbs.rowUpdated(connIDFromCtx(ctx), table, oldRec, newRec)
}

// DeleteRow converts the row to a record and invokes rowDeleted.
func (p *gmsRowProvider) DeleteRow(
	ctx *gmssql.Context, table string, row gmssql.Row, schema gmssql.Schema,
) error {
	if p.cbs.rowDeleted == nil {
		return nil
	}
	record := rowToMap(row, schemaColumns(schema))
	return p.cbs.rowDeleted(connIDFromCtx(ctx), table, record)
}

// ---------------------------------------------------------------------------
// vdbRowIter — wraps a []gmssql.Row slice to implement gmssql.RowIter
// ---------------------------------------------------------------------------

type vdbRowIter struct {
	rows []gmssql.Row
	pos  int
}

func (i *vdbRowIter) Next(_ *gmssql.Context) (gmssql.Row, error) {
	if i.pos >= len(i.rows) {
		return nil, io.EOF
	}
	row := i.rows[i.pos]
	i.pos++
	return row, nil
}

func (i *vdbRowIter) Close(_ *gmssql.Context) error { return nil }
