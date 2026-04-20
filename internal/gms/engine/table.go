// Package engine provides the GMS DatabaseProvider, Database, Table, and
// RowWriter implementations that wire the VirtualDB row and schema providers
// into the go-mysql-server execution layer.
package engine

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/virtual-db/vdb-mysql-driver/internal/gms/rows"
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

	// ai is the shared auto-increment state for this table. It is owned by the
	// DatabaseProvider registry so that the counter survives across Table
	// instances (which are created fresh on every GetTableInsensitive call).
	// nil only in unit tests that construct Table directly.
	ai *autoIncrState
}

// autoIncrState holds the per-table auto-increment counter. It is allocated
// once per table name in DatabaseProvider and shared across all Table
// instances for that table.
type autoIncrState struct {
	mu     sync.Mutex
	next   uint64
	loaded bool
}

var _ gmssql.Table = (*Table)(nil)
var _ gmssql.InsertableTable = (*Table)(nil)
var _ gmssql.UpdatableTable = (*Table)(nil)
var _ gmssql.DeletableTable = (*Table)(nil)
var _ gmssql.AutoIncrementTable = (*Table)(nil)

// aiState returns the table's autoIncrState, creating a throwaway one if ai
// is nil (unit-test path where no DatabaseProvider is wired up).
func (t *Table) aiState() *autoIncrState {
	if t.ai != nil {
		return t.ai
	}
	// Allocate a fresh state so tests that don't wire a provider still work.
	t.ai = &autoIncrState{}
	return t.ai
}

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
		copy(row, vals)
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
// AutoIncrementTable — gmssql.AutoIncrementTable implementation
// ---------------------------------------------------------------------------

// loadAutoIncrCounter seeds ai.next from information_schema.TABLES the first
// time it is needed. Must be called with ai.mu held.
func (t *Table) loadAutoIncrCounter(ai *autoIncrState) {
	if ai.loaded {
		return
	}
	ai.loaded = true
	ai.next = 1 // safe fallback when db is nil or query fails

	if t.db == nil {
		return
	}

	var next uint64
	err := t.db.QueryRow(
		"SELECT COALESCE(AUTO_INCREMENT, 1) FROM information_schema.TABLES"+
			" WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		t.dbName, t.name,
	).Scan(&next)
	if err == nil && next > 0 {
		ai.next = next
	}
}

// PeekNextAutoIncrementValue satisfies gmssql.AutoIncrementGetter.
// It returns the next value that would be assigned without advancing the
// counter.
func (t *Table) PeekNextAutoIncrementValue(_ *gmssql.Context) (uint64, error) {
	ai := t.aiState()
	ai.mu.Lock()
	defer ai.mu.Unlock()
	t.loadAutoIncrCounter(ai)
	return ai.next, nil
}

// GetNextAutoIncrementValue satisfies gmssql.AutoIncrementTable. GMS calls
// this once per inserted row after evaluating the AUTO_INCREMENT expression.
//
//   - insertVal == nil means the user omitted the column (or wrote DEFAULT /
//     NULL / 0): we consume and return the next counter value, then advance.
//   - insertVal != nil means the user supplied an explicit id: we advance the
//     counter past it (if necessary) so subsequent unspecified inserts don't
//     collide, then return the explicit value so GMS uses it unchanged.
func (t *Table) GetNextAutoIncrementValue(_ *gmssql.Context, insertVal interface{}) (uint64, error) {
	ai := t.aiState()
	ai.mu.Lock()
	defer ai.mu.Unlock()
	t.loadAutoIncrCounter(ai)

	if insertVal == nil {
		seq := ai.next
		ai.next++
		return seq, nil
	}

	// Convert the caller-supplied value to uint64. GMS may deliver it as any
	// of the integer types depending on the column's declared type.
	var given uint64
	switch v := insertVal.(type) {
	case int64:
		if v > 0 {
			given = uint64(v)
		}
	case uint64:
		given = v
	case int32:
		if v > 0 {
			given = uint64(v)
		}
	case uint32:
		given = uint64(v)
	case int:
		if v > 0 {
			given = uint64(v)
		}
	}

	// Keep the counter ahead of any explicitly inserted id so the next
	// auto-assigned id does not collide with a row the caller already wrote.
	if given >= ai.next {
		ai.next = given + 1
	}
	return given, nil
}

// AutoIncrementSetter satisfies gmssql.AutoIncrementTable. The returned setter
// allows GMS (and ALTER TABLE … AUTO_INCREMENT = n) to update the in-memory
// counter.
func (t *Table) AutoIncrementSetter(_ *gmssql.Context) gmssql.AutoIncrementSetter {
	return &autoIncrSetter{ai: t.aiState()}
}

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

// ---------------------------------------------------------------------------
// autoIncrSetter — gmssql.AutoIncrementSetter implementation
// ---------------------------------------------------------------------------

// autoIncrSetter implements gmssql.AutoIncrementSetter for Table.
type autoIncrSetter struct {
	ai *autoIncrState
}

// SetAutoIncrementValue updates the shared auto-increment counter.
// Called by GMS for ALTER TABLE … AUTO_INCREMENT = n statements.
func (s *autoIncrSetter) SetAutoIncrementValue(_ *gmssql.Context, val uint64) error {
	s.ai.mu.Lock()
	defer s.ai.mu.Unlock()
	s.ai.next = val
	s.ai.loaded = true // suppress the lazy load; val is authoritative
	return nil
}

// AcquireAutoIncrementLock is a no-op for the proxy. The proxy is
// single-writer per connection so no cross-connection locking is required.
func (s *autoIncrSetter) AcquireAutoIncrementLock(_ *gmssql.Context) (func(), error) {
	return func() {}, nil
}

// Close satisfies gmssql.Closer (embedded in gmssql.AutoIncrementSetter).
// No persistent state to flush.
func (s *autoIncrSetter) Close(_ *gmssql.Context) error { return nil }
