package rows_test

import (
	"errors"
	"testing"

	gmssql "github.com/dolthub/go-mysql-server/sql"
	"github.com/virtual-db/mysql-driver/internal/bridge"
	. "github.com/virtual-db/mysql-driver/internal/rows"
)

// stubEventBridge implements bridge.EventBridge for GMSProvider tests.
// Only the five methods GMSProvider actually calls have active function fields.
// The remaining nine methods are no-op stubs that document the unused surface.
type stubEventBridge struct {
	rowsFetched func(connID uint32, table string, records []map[string]any) ([]map[string]any, error)
	rowsReady   func(connID uint32, table string, records []map[string]any) ([]map[string]any, error)
	rowInserted func(connID uint32, table string, record map[string]any) (map[string]any, error)
	rowUpdated  func(connID uint32, table string, old, new map[string]any) (map[string]any, error)
	rowDeleted  func(connID uint32, table string, record map[string]any) error
}

var _ bridge.EventBridge = (*stubEventBridge)(nil)

func (s *stubEventBridge) RowsFetched(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
	if s.rowsFetched != nil {
		return s.rowsFetched(connID, table, records)
	}
	return records, nil
}

func (s *stubEventBridge) RowsReady(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
	if s.rowsReady != nil {
		return s.rowsReady(connID, table, records)
	}
	return records, nil
}

func (s *stubEventBridge) RowInserted(connID uint32, table string, record map[string]any) (map[string]any, error) {
	if s.rowInserted != nil {
		return s.rowInserted(connID, table, record)
	}
	return nil, nil
}

func (s *stubEventBridge) RowUpdated(connID uint32, table string, old, new map[string]any) (map[string]any, error) {
	if s.rowUpdated != nil {
		return s.rowUpdated(connID, table, old, new)
	}
	return nil, nil
}

func (s *stubEventBridge) RowDeleted(connID uint32, table string, record map[string]any) error {
	if s.rowDeleted != nil {
		return s.rowDeleted(connID, table, record)
	}
	return nil
}

// Unused EventBridge methods — no-op stubs.
func (s *stubEventBridge) ConnectionOpened(_ uint32, _, _ string) error { return nil }
func (s *stubEventBridge) ConnectionClosed(_ uint32, _, _ string)       {}

func (s *stubEventBridge) TransactionBegun(_ uint32, _ bool) error  { return nil }
func (s *stubEventBridge) TransactionCommitted(_ uint32) error      { return nil }
func (s *stubEventBridge) TransactionRolledBack(_ uint32, _ string) {}

func (s *stubEventBridge) QueryReceived(_ uint32, q, _ string) (string, error) { return q, nil }
func (s *stubEventBridge) QueryCompleted(_ uint32, _ string, _ int64, _ error) {}

func (s *stubEventBridge) SchemaLoaded(_ string, _ []string, _ string) {}
func (s *stubEventBridge) SchemaInvalidated(_ string)                  {}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func makeCtx() *gmssql.Context {
	return gmssql.NewEmptyContext()
}

func twoColSchema() gmssql.Schema {
	return gmssql.Schema{{Name: "id"}, {Name: "val"}}
}

// ---------------------------------------------------------------------------
// FetchRows
// ---------------------------------------------------------------------------

func TestFetchRows_InvokesRowsFetchedCallback(t *testing.T) {
	var called bool
	b := &stubEventBridge{
		rowsFetched: func(_ uint32, table string, recs []map[string]any) ([]map[string]any, error) {
			called = true
			if table != "orders" {
				t.Errorf("table: got %q, want %q", table, "orders")
			}
			if len(recs) != 2 {
				t.Errorf("len(recs): got %d, want 2", len(recs))
			}
			return recs, nil
		},
	}

	p := NewGMSProvider(b)
	rawRows := []gmssql.Row{{1, "x"}, {2, "y"}}
	schema := gmssql.Schema{{Name: "id"}, {Name: "val"}}
	_, err := p.FetchRows(makeCtx(), "orders", rawRows, schema)
	if err != nil {
		t.Fatalf("FetchRows error: %v", err)
	}
	if !called {
		t.Fatal("RowsFetched callback was not called")
	}
}

func TestFetchRows_NilCallback_ReturnsUnmodified(t *testing.T) {
	// rowsFetched field is nil — stub returns records unchanged.
	p := NewGMSProvider(&stubEventBridge{})
	rawRows := []gmssql.Row{{1, "a"}}
	schema := gmssql.Schema{{Name: "id"}, {Name: "v"}}
	got, err := p.FetchRows(makeCtx(), "t", rawRows, schema)
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
		rowsFetched: func(_ uint32, _ string, _ []map[string]any) ([]map[string]any, error) {
			return nil, want
		},
	}
	p := NewGMSProvider(b)
	_, err := p.FetchRows(makeCtx(), "t", []gmssql.Row{{1}}, gmssql.Schema{{Name: "id"}})
	if !errors.Is(err, want) {
		t.Errorf("expected %v, got %v", want, err)
	}
}
