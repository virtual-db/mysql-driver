// Package engine provides the GMS DatabaseProvider, Database, Table, and
// RowWriter implementations that wire the VirtualDB row and schema providers
// into the go-mysql-server execution layer.
package engine

import (
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/AnqorDX/vdb-mysql-driver/internal/gms/rows"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// Table implements gmssql.Table and the write interfaces InsertableTable,
// UpdatableTable, and DeletableTable. It delegates row reads to fetchFromSource
// (against the source MySQL database) followed by the rows.Provider lifecycle
// hooks, and delegates writes directly to the rows.Provider.
type Table struct {
	name   string
	dbName string // needed for the fetchFromSource SQL query
	schema gmssql.Schema
	rows   rows.Provider
	db     *sql.DB // source database connection; may be nil in tests
}

var _ gmssql.Table = (*Table)(nil)
var _ gmssql.InsertableTable = (*Table)(nil)
var _ gmssql.UpdatableTable = (*Table)(nil)
var _ gmssql.DeletableTable = (*Table)(nil)

func (t *Table) Name() string                  { return t.name }
func (t *Table) String() string                { return t.name }
func (t *Table) Schema() gmssql.Schema         { return t.schema }
func (t *Table) Collation() gmssql.CollationID { return gmssql.Collation_Default }

// Partitions satisfies gmssql.Table. This implementation always has exactly one
// logical partition.
func (t *Table) Partitions(_ *gmssql.Context) (gmssql.PartitionIter, error) {
	return &singlePartitionIter{}, nil
}

// PartitionRows reads rows from the source database, passes them through
// FetchRows and CommitRows on the rows.Provider, and returns a RowIter over
// the final record set.
func (t *Table) PartitionRows(ctx *gmssql.Context, _ gmssql.Partition) (gmssql.RowIter, error) {
	rawRows, err := t.fetchFromSource(ctx)
	if err != nil {
		return nil, err
	}

	if t.rows == nil {
		return rows.NewIter(rawRows), nil
	}

	merged, err := t.rows.FetchRows(ctx, t.name, rawRows, t.schema)
	if err != nil {
		return nil, err
	}

	final, err := t.rows.CommitRows(ctx, t.name, merged)
	if err != nil {
		return nil, err
	}

	cols := rows.SchemaColumns(t.schema)
	sqlRows := make([]gmssql.Row, len(final))
	for i, rec := range final {
		sqlRows[i] = rows.MapToRow(rec, cols)
	}
	return rows.NewIter(sqlRows), nil
}

// fetchFromSource queries the source MySQL database for all rows of the table.
// If db is nil (e.g. in unit tests), it returns an empty row slice without
// error so that higher-level code can still exercise the overlay logic.
func (t *Table) fetchFromSource(ctx *gmssql.Context) ([]gmssql.Row, error) {
	if t.db == nil {
		return nil, nil
	}

	cols := rows.SchemaColumns(t.schema)
	if len(cols) == 0 {
		return nil, nil
	}

	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = "`" + c + "`"
	}
	query := "SELECT " + strings.Join(quotedCols, ", ") +
		" FROM `" + t.dbName + "`.`" + t.name + "`"

	dbRows, err := t.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("engine: fetch from %q.%q: %w", t.dbName, t.name, err)
	}
	defer dbRows.Close()

	var result []gmssql.Row
	for dbRows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := dbRows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("engine: scan row from %q.%q: %w", t.dbName, t.name, err)
		}
		row := make(gmssql.Row, len(cols))
		for i, v := range vals {
			row[i] = v
		}
		result = append(result, row)
	}
	if err := dbRows.Err(); err != nil {
		return nil, fmt.Errorf("engine: rows error from %q.%q: %w", t.dbName, t.name, err)
	}
	return result, nil
}

// Inserter satisfies gmssql.InsertableTable.
func (t *Table) Inserter(_ *gmssql.Context) gmssql.RowInserter { return &RowWriter{table: t} }

// Updater satisfies gmssql.UpdatableTable.
func (t *Table) Updater(_ *gmssql.Context) gmssql.RowUpdater { return &RowWriter{table: t} }

// Deleter satisfies gmssql.DeletableTable.
func (t *Table) Deleter(_ *gmssql.Context) gmssql.RowDeleter { return &RowWriter{table: t} }

// ---------------------------------------------------------------------------
// singlePartition — trivial single-partition implementation
// ---------------------------------------------------------------------------

type singlePartition struct{}

func (singlePartition) Key() []byte { return []byte("single") }

type singlePartitionIter struct {
	done bool
}

func (i *singlePartitionIter) Next(_ *gmssql.Context) (gmssql.Partition, error) {
	if i.done {
		return nil, io.EOF
	}
	i.done = true
	return singlePartition{}, nil
}

func (i *singlePartitionIter) Close(_ *gmssql.Context) error { return nil }
