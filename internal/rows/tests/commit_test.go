package rows_test

import (
	"testing"

	. "github.com/virtual-db/mysql-driver/internal/rows"
)

// ---------------------------------------------------------------------------
// CommitRows
// ---------------------------------------------------------------------------

func TestCommitRows_InvokesRowsReadyCallback(t *testing.T) {
	var called bool
	b := &stubEventBridge{
		rowsReady: func(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
			called = true
			return r, nil
		},
	}
	p := NewGMSProvider(b)
	records := []map[string]any{{"id": 1, "val": "x"}}
	_, err := p.CommitRows(makeCtx(), "t", records)
	if err != nil {
		t.Fatalf("CommitRows error: %v", err)
	}
	if !called {
		t.Fatal("RowsReady callback was not called")
	}
}

func TestCommitRows_NilCallback_ReturnsUnmodified(t *testing.T) {
	// rowsReady field is nil — stub returns records unchanged.
	p := NewGMSProvider(&stubEventBridge{})
	records := []map[string]any{{"id": 1}}
	got, err := p.CommitRows(makeCtx(), "t", records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("len(got): got %d, want 1", len(got))
	}
}
