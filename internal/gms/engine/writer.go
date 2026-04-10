package engine

import (
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// RowWriter dispatches INSERT, UPDATE, and DELETE operations to the row
// Provider held by the owning Table. It satisfies the three GMS write
// interfaces so that GMS can use a single concrete type for all DML.
type RowWriter struct {
	table *Table
}

var _ gmssql.RowInserter = (*RowWriter)(nil)
var _ gmssql.RowUpdater = (*RowWriter)(nil)
var _ gmssql.RowDeleter = (*RowWriter)(nil)

// StatementBegin is called at the start of each DML statement. No-op here;
// the event bridge handles transaction-level signalling.
func (w *RowWriter) StatementBegin(_ *gmssql.Context) {}

// DiscardChanges is called when a statement is rolled back mid-execution.
// No-op: we have no in-memory buffer to discard.
func (w *RowWriter) DiscardChanges(_ *gmssql.Context, _ error) error { return nil }

// StatementComplete is called when a DML statement finishes successfully.
// No-op here.
func (w *RowWriter) StatementComplete(_ *gmssql.Context) error { return nil }

// Close is called when GMS is done with this writer instance. No resources
// are owned directly; the underlying DB connection belongs to the Table.
func (w *RowWriter) Close(_ *gmssql.Context) error { return nil }

// Insert satisfies gmssql.RowInserter. It delegates to the row Provider,
// which converts the GMS row to a map and fires the RowInserted event.
func (w *RowWriter) Insert(ctx *gmssql.Context, row gmssql.Row) error {
	if w.table.rows == nil {
		return nil
	}
	_, err := w.table.rows.InsertRow(ctx, w.table.name, row, w.table.schema)
	return err
}

// Update satisfies gmssql.RowUpdater. It delegates to the row Provider,
// which converts old and new rows to maps and fires the RowUpdated event.
func (w *RowWriter) Update(ctx *gmssql.Context, old, new gmssql.Row) error {
	if w.table.rows == nil {
		return nil
	}
	_, err := w.table.rows.UpdateRow(ctx, w.table.name, old, new, w.table.schema)
	return err
}

// Delete satisfies gmssql.RowDeleter. It delegates to the row Provider,
// which converts the row to a map and fires the RowDeleted event.
func (w *RowWriter) Delete(ctx *gmssql.Context, row gmssql.Row) error {
	if w.table.rows == nil {
		return nil
	}
	return w.table.rows.DeleteRow(ctx, w.table.name, row, w.table.schema)
}
