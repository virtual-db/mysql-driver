package driver

import (
	"errors"
	"testing"

	"github.com/AnqorDX/vdb-mysql-driver/internal/gms/rows"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// makeTestContext creates a minimal *gmssql.Context for unit tests.
func makeTestContext(t *testing.T) *gmssql.Context {
	t.Helper()
	return gmssql.NewEmptyContext()
}

// ---------------------------------------------------------------------------
// RowToMap
// ---------------------------------------------------------------------------

func TestRowToMap_RoundTrip(t *testing.T) {
	cols := []string{"id", "name", "balance"}
	row := gmssql.Row{42, "alice", 99.5}
	got := rows.RowToMap(row, cols)

	if got["id"] != 42 {
		t.Errorf("id: got %v, want 42", got["id"])
	}
	if got["name"] != "alice" {
		t.Errorf("name: got %v, want alice", got["name"])
	}
	if got["balance"] != 99.5 {
		t.Errorf("balance: got %v, want 99.5", got["balance"])
	}
}

func TestRowToMap_PanicsOnMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("RowToMap did not panic on column count mismatch")
		}
	}()
	rows.RowToMap(gmssql.Row{1, 2}, []string{"only_one_col"})
}

// ---------------------------------------------------------------------------
// MapToRow
// ---------------------------------------------------------------------------

func TestMapToRow_IsInverseOfRowToMap(t *testing.T) {
	record := map[string]any{"id": 1, "name": "bob"}
	cols := []string{"id", "name"}
	row := rows.MapToRow(record, cols)

	if row[0] != 1 {
		t.Errorf("row[0]: got %v, want 1", row[0])
	}
	if row[1] != "bob" {
		t.Errorf("row[1]: got %v, want bob", row[1])
	}
}

func TestMapToRow_PreservesColumnOrdering(t *testing.T) {
	cols := []string{"c", "b", "a"}
	record := map[string]any{"a": 1, "b": 2, "c": 3}
	row := rows.MapToRow(record, cols)

	if row[0] != 3 { // c
		t.Errorf("row[0] (c): got %v, want 3", row[0])
	}
	if row[1] != 2 { // b
		t.Errorf("row[1] (b): got %v, want 2", row[1])
	}
	if row[2] != 1 { // a
		t.Errorf("row[2] (a): got %v, want 1", row[2])
	}
}

// ---------------------------------------------------------------------------
// SchemaColumns
// ---------------------------------------------------------------------------

func TestSchemaColumns_ExtractsNames(t *testing.T) {
	schema := gmssql.Schema{
		{Name: "id"},
		{Name: "title"},
		{Name: "created_at"},
	}
	cols := rows.SchemaColumns(schema)
	want := []string{"id", "title", "created_at"}
	if len(cols) != len(want) {
		t.Fatalf("length: got %d, want %d", len(cols), len(want))
	}
	for i, c := range cols {
		if c != want[i] {
			t.Errorf("cols[%d]: got %q, want %q", i, c, want[i])
		}
	}
}

// ---------------------------------------------------------------------------
// FetchRows
// ---------------------------------------------------------------------------

func TestFetchRows_InvokesRowsFetchedCallback(t *testing.T) {
	var called bool
	b := fullBridge()
	b.rowsFetched = func(_ uint32, table string, recs []map[string]any) ([]map[string]any, error) {
		called = true
		if table != "orders" {
			t.Errorf("table: got %q, want %q", table, "orders")
		}
		if len(recs) != 2 {
			t.Errorf("len(recs): got %d, want 2", len(recs))
		}
		return recs, nil
	}

	p := rows.NewGMSProvider(b)
	ctx := makeTestContext(t)
	rawRows := []gmssql.Row{{1, "x"}, {2, "y"}}
	schema := gmssql.Schema{{Name: "id"}, {Name: "val"}}
	_, err := p.FetchRows(ctx, "orders", rawRows, schema)
	if err != nil {
		t.Fatalf("FetchRows error: %v", err)
	}
	if !called {
		t.Fatal("rowsFetched callback was not called")
	}
}

func TestFetchRows_NilCallback_ReturnsUnmodified(t *testing.T) {
	// rowsFetched is nil — stub returns records unchanged.
	b := &stubEventBridge{}
	p := rows.NewGMSProvider(b)
	ctx := makeTestContext(t)
	rawRows := []gmssql.Row{{1, "a"}}
	schema := gmssql.Schema{{Name: "id"}, {Name: "v"}}
	got, err := p.FetchRows(ctx, "t", rawRows, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("len(got): got %d, want 1", len(got))
	}
}

func TestFetchRows_CallbackError_IsReturned(t *testing.T) {
	want := errors.New("fetch failed")
	b := &stubEventBridge{
		rowsFetched: func(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
			return nil, want
		},
	}
	p := rows.NewGMSProvider(b)
	ctx := makeTestContext(t)
	schema := gmssql.Schema{{Name: "id"}}
	_, err := p.FetchRows(ctx, "t", []gmssql.Row{{1}}, schema)
	if !errors.Is(err, want) {
		t.Errorf("expected %v, got %v", want, err)
	}
}

// ---------------------------------------------------------------------------
// CommitRows + ordering
// ---------------------------------------------------------------------------

func TestCommitRows_InvokesRowsReadyCallback(t *testing.T) {
	var order []string
	b := fullBridge()
	b.rowsFetched = func(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
		order = append(order, "fetched")
		return r, nil
	}
	b.rowsReady = func(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
		order = append(order, "ready")
		return r, nil
	}

	p := rows.NewGMSProvider(b)
	ctx := makeTestContext(t)
	rawRows := []gmssql.Row{{1, "x"}}
	schema := gmssql.Schema{{Name: "id"}, {Name: "val"}}

	recs, err := p.FetchRows(ctx, "t", rawRows, schema)
	if err != nil {
		t.Fatalf("FetchRows error: %v", err)
	}
	_, err = p.CommitRows(ctx, "t", recs)
	if err != nil {
		t.Fatalf("CommitRows error: %v", err)
	}

	if len(order) != 2 || order[0] != "fetched" || order[1] != "ready" {
		t.Errorf("callback order: got %v, want [fetched ready]", order)
	}
}

func TestCommitRows_NilCallback_ReturnsUnmodified(t *testing.T) {
	// rowsReady is nil — stub returns records unchanged.
	b := &stubEventBridge{}
	p := rows.NewGMSProvider(b)
	ctx := makeTestContext(t)
	records := []map[string]any{{"id": 1}}
	got, err := p.CommitRows(ctx, "t", records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("len(got): got %d, want 1", len(got))
	}
}

// ---------------------------------------------------------------------------
// InsertRow / UpdateRow / DeleteRow — nil callback safety
// ---------------------------------------------------------------------------

func TestInsertRow_NilCallback_ReturnsNilNoError(t *testing.T) {
	// rowInserted is nil — stub returns (nil, nil).
	b := &stubEventBridge{}
	p := rows.NewGMSProvider(b)
	ctx := makeTestContext(t)
	schema := gmssql.Schema{{Name: "id"}, {Name: "val"}}
	rec, err := p.InsertRow(ctx, "t", gmssql.Row{1, "x"}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec != nil {
		t.Errorf("expected nil record, got %v", rec)
	}
}

func TestUpdateRow_NilCallback_ReturnsNilNoError(t *testing.T) {
	// rowUpdated is nil — stub returns (nil, nil).
	b := &stubEventBridge{}
	p := rows.NewGMSProvider(b)
	ctx := makeTestContext(t)
	schema := gmssql.Schema{{Name: "id"}, {Name: "val"}}
	rec, err := p.UpdateRow(ctx, "t", gmssql.Row{1, "old"}, gmssql.Row{1, "new"}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec != nil {
		t.Errorf("expected nil record, got %v", rec)
	}
}

func TestDeleteRow_NilCallback_ReturnsNil(t *testing.T) {
	// rowDeleted is nil — stub returns nil.
	b := &stubEventBridge{}
	p := rows.NewGMSProvider(b)
	ctx := makeTestContext(t)
	schema := gmssql.Schema{{Name: "id"}, {Name: "val"}}
	if err := p.DeleteRow(ctx, "t", gmssql.Row{1, "x"}, schema); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertRow_InvokesCallback(t *testing.T) {
	var called bool
	b := fullBridge()
	b.rowInserted = func(_ uint32, table string, r map[string]any) (map[string]any, error) {
		called = true
		if table != "items" {
			t.Errorf("table: got %q, want items", table)
		}
		if r["id"] != 99 {
			t.Errorf("id: got %v, want 99", r["id"])
		}
		return r, nil
	}
	p := rows.NewGMSProvider(b)
	ctx := makeTestContext(t)
	schema := gmssql.Schema{{Name: "id"}, {Name: "val"}}
	_, err := p.InsertRow(ctx, "items", gmssql.Row{99, "thing"}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("rowInserted callback was not called")
	}
}
