package intercept_test

import (
	"context"
	"testing"

	. "github.com/virtual-db/mysql-driver/internal/intercept"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
	sqltypes "github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

func TestInterceptor_Priority_ReturnsZero(t *testing.T) {
	qi := NewInterceptor(&stubEventBridge{})
	if got := qi.Priority(); got != 0 {
		t.Errorf("Priority: got %d, want 0", got)
	}
}

func TestPrepare_DelegatesToChain(t *testing.T) {
	var called bool

	qi := NewInterceptor(&stubEventBridge{})
	chain := &stubChain{
		comPrepare: func(_ context.Context, _ *vitessmysql.Conn, _ string, _ *vitessmysql.PrepareData) ([]*querypb.Field, error) {
			called = true
			return nil, nil
		},
	}

	if _, err := qi.Prepare(context.Background(), chain, &vitessmysql.Conn{}, "SELECT ?", nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("ComPrepare was not called on the chain")
	}
}

func TestStmtExecute_DelegatesToChain(t *testing.T) {
	var called bool

	qi := NewInterceptor(&stubEventBridge{})
	chain := &stubChain{
		comStmtExecute: func(_ context.Context, _ *vitessmysql.Conn, _ *vitessmysql.PrepareData, _ func(*sqltypes.Result) error) error {
			called = true
			return nil
		},
	}
	noopStmtCb := func(*sqltypes.Result) error { return nil }

	if err := qi.StmtExecute(context.Background(), chain, &vitessmysql.Conn{}, nil, noopStmtCb); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("ComStmtExecute was not called on the chain")
	}
}
