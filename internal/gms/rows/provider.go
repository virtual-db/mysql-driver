// Package rows translates between GMS row representations and the map[string]any
// form used by the EventBridge interface.
package rows

import (
	"github.com/virtual-db/vdb-mysql-driver/internal/bridge"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// Provider translates GMS row representations into map[string]any form and
// invokes the appropriate EventBridge method at each row lifecycle moment.
//
// All implementations must be safe for concurrent use.
type Provider interface {
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

// GMSProvider is the concrete Provider implementation backed by an EventBridge.
// It holds no schema state — column names are derived from the GMS schema
// parameter passed to each method.
type GMSProvider struct {
	events bridge.EventBridge
}

// NewGMSProvider constructs a Provider backed by the given EventBridge.
// events must not be nil.
func NewGMSProvider(events bridge.EventBridge) *GMSProvider {
	return &GMSProvider{events: events}
}

// FetchRows converts raw GMS rows to []map[string]any and delegates to
// events.RowsFetched.
func (p *GMSProvider) FetchRows(
	ctx *gmssql.Context, table string, rows []gmssql.Row, schema gmssql.Schema,
) ([]map[string]any, error) {
	cols := SchemaColumns(schema)
	records := make([]map[string]any, len(rows))
	for i, row := range rows {
		records[i] = RowToMap(row, cols)
	}
	return p.events.RowsFetched(connIDFromCtx(ctx), table, records)
}

// CommitRows delegates to events.RowsReady with the final record set.
func (p *GMSProvider) CommitRows(
	ctx *gmssql.Context, table string, records []map[string]any,
) ([]map[string]any, error) {
	return p.events.RowsReady(connIDFromCtx(ctx), table, records)
}

// InsertRow converts the row to a record and delegates to events.RowInserted.
func (p *GMSProvider) InsertRow(
	ctx *gmssql.Context, table string, row gmssql.Row, schema gmssql.Schema,
) (map[string]any, error) {
	record := RowToMap(row, SchemaColumns(schema))
	return p.events.RowInserted(connIDFromCtx(ctx), table, record)
}

// UpdateRow converts old and new rows to records and delegates to events.RowUpdated.
func (p *GMSProvider) UpdateRow(
	ctx *gmssql.Context, table string, old, new gmssql.Row, schema gmssql.Schema,
) (map[string]any, error) {
	cols := SchemaColumns(schema)
	oldRec := RowToMap(old, cols)
	newRec := RowToMap(new, cols)
	return p.events.RowUpdated(connIDFromCtx(ctx), table, oldRec, newRec)
}

// DeleteRow converts the row to a record and delegates to events.RowDeleted.
func (p *GMSProvider) DeleteRow(
	ctx *gmssql.Context, table string, row gmssql.Row, schema gmssql.Schema,
) error {
	record := RowToMap(row, SchemaColumns(schema))
	return p.events.RowDeleted(connIDFromCtx(ctx), table, record)
}

// ---------------------------------------------------------------------------
// Unexported helpers
// ---------------------------------------------------------------------------

// connIDFromCtx extracts the connection ID from a GMS sql.Context.
// Returns 0 if the context or session is nil.
func connIDFromCtx(ctx *gmssql.Context) uint32 {
	if ctx == nil || ctx.Session == nil {
		return 0
	}
	return ctx.Session.ID()
}
