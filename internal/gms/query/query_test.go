package query_test

import (
	"context"
	"errors"
	"testing"

	"github.com/AnqorDX/vdb-mysql-driver/internal/bridge"
	"github.com/AnqorDX/vdb-mysql-driver/internal/gms/query"
	vitessmysql "github.com/dolthub/vitess/go/mysql"
	sqltypes "github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	sqlparser "github.com/dolthub/vitess/go/vt/sqlparser"

	gmssql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/server"
)

// ---------------------------------------------------------------------------
// Local stubs
// ---------------------------------------------------------------------------

// stubEventBridge is a local stub for bridge.EventBridge used only by
// gms/query tests. It exposes function fields for the two methods that
// Interceptor calls; all other methods are no-ops.
type stubEventBridge struct {
	queryReceived func(connID uint32, query, database string) (string, error)
	queryCompleted func(connID uint32, query string, rowsAffected int64, err error)
}

var _ bridge.EventBridge = (*stubEventBridge)(nil)

func (s *stubEventBridge) ConnectionOpened(_ uint32, _, _ string) error { return nil }
func (s *stubEventBridge) ConnectionClosed(_ uint32, _, _ string)       {}
func (s *stubEventBridge) TransactionBegun(_ uint32, _ bool) error      { return nil }
func (s *stubEventBridge) TransactionCommitted(_ uint32) error          { return nil }
func (s *stubEventBridge) TransactionRolledBack(_ uint32, _ string)     {}
func (s *stubEventBridge) RowsFetched(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
	return r, nil
}
func (s *stubEventBridge) RowsReady(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
	return r, nil
}
func (s *stubEventBridge) RowInserted(_ uint32, _ string, r map[string]any) (map[string]any, error) {
	return r, nil
}
func (s *stubEventBridge) RowUpdated(_ uint32, _ string, _, n map[string]any) (map[string]any, error) {
	return n, nil
}
func (s *stubEventBridge) RowDeleted(_ uint32, _ string, _ map[string]any) error { return nil }
func (s *stubEventBridge) SchemaLoaded(_ string, _ []string, _ string)           {}
func (s *stubEventBridge) SchemaInvalidated(_ string)                            {}

func (s *stubEventBridge) QueryReceived(connID uint32, q, database string) (string, error) {
	if s.queryReceived != nil {
		return s.queryReceived(connID, q, database)
	}
	return q, nil
}

func (s *stubEventBridge) QueryCompleted(connID uint32, q string, rowsAffected int64, err error) {
	if s.queryCompleted != nil {
		s.queryCompleted(connID, q, rowsAffected, err)
	}
}

// stubChain implements server.Chain with configurable function fields.
// The comQuery field records the query string it received.
type stubChain struct {
	comQuery      func(ctx context.Context, conn *vitessmysql.Conn, query string, callback vitessmysql.ResultSpoolFn) error
	comMultiQuery func(ctx context.Context, conn *vitessmysql.Conn, query string, callback vitessmysql.ResultSpoolFn) (string, error)
	comPrepare    func(ctx context.Context, conn *vitessmysql.Conn, query string, prepare *vitessmysql.PrepareData) ([]*querypb.Field, error)
	comStmtExec   func(ctx context.Context, conn *vitessmysql.Conn, prepare *vitessmysql.PrepareData, callback func(*sqltypes.Result) error) error
}

var _ server.Chain = (*stubChain)(nil)

func (c *stubChain) ComQuery(ctx context.Context, conn *vitessmysql.Conn, q string, callback vitessmysql.ResultSpoolFn) error {
	if c.comQuery != nil {
		return c.comQuery(ctx, conn, q, callback)
	}
	return nil
}

func (c *stubChain) ComMultiQuery(ctx context.Context, conn *vitessmysql.Conn, q string, callback vitessmysql.ResultSpoolFn) (string, error) {
	if c.comMultiQuery != nil {
		return c.comMultiQuery(ctx, conn, q, callback)
	}
	return "", nil
}

func (c *stubChain) ComPrepare(ctx context.Context, conn *vitessmysql.Conn, q string, prepare *vitessmysql.PrepareData) ([]*querypb.Field, error) {
	if c.comPrepare != nil {
		return c.comPrepare(ctx, conn, q, prepare)
	}
	return nil, nil
}

func (c *stubChain) ComStmtExecute(ctx context.Context, conn *vitessmysql.Conn, prepare *vitessmysql.PrepareData, callback func(*sqltypes.Result) error) error {
	if c.comStmtExec != nil {
		return c.comStmtExec(ctx, conn, prepare, callback)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// noopCallback is a result callback that does nothing.
func noopCallback(_ *sqltypes.Result, _ bool) error { return nil }

// newConn creates a minimal *vitessmysql.Conn for tests.
func newConn(connID uint32) *vitessmysql.Conn {
	return &vitessmysql.Conn{ConnectionID: connID}
}

// newInterceptor returns an Interceptor backed by a stub bridge.
func newInterceptor(events bridge.EventBridge) *query.Interceptor {
	return query.NewInterceptor(events)
}

// ---------------------------------------------------------------------------
// Priority
// ---------------------------------------------------------------------------

func TestInterceptor_Priority_ReturnsZero(t *testing.T) {
	i := newInterceptor(&stubEventBridge{})
	if got := i.Priority(); got != 0 {
		t.Errorf("Priority: got %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// Query — basic lifecycle
// ---------------------------------------------------------------------------

func TestQuery_CallsQueryReceived(t *testing.T) {
	var called bool
	events := &stubEventBridge{
		queryReceived: func(_ uint32, q, _ string) (string, error) {
			called = true
			return q, nil
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{}

	if err := i.Query(context.Background(), chain, newConn(1), "SELECT 1", noopCallback); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if !called {
		t.Fatal("QueryReceived was not called")
	}
}

func TestQuery_CallsQueryCompleted(t *testing.T) {
	var called bool
	events := &stubEventBridge{
		queryCompleted: func(_ uint32, _ string, _ int64, _ error) {
			called = true
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{}

	if err := i.Query(context.Background(), chain, newConn(1), "SELECT 1", noopCallback); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if !called {
		t.Fatal("QueryCompleted was not called")
	}
}

func TestQuery_QueryReceived_ReturnsRewrite_RewriteUsed(t *testing.T) {
	const original = "SELECT 1"
	const rewritten = "SELECT 2"

	var chainGotQuery string
	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, _ string) (string, error) {
			return rewritten, nil
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{
		comQuery: func(_ context.Context, _ *vitessmysql.Conn, q string, _ vitessmysql.ResultSpoolFn) error {
			chainGotQuery = q
			return nil
		},
	}

	if err := i.Query(context.Background(), chain, newConn(1), original, noopCallback); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if chainGotQuery != rewritten {
		t.Errorf("chain received query %q, want %q", chainGotQuery, rewritten)
	}
}

func TestQuery_QueryReceived_ReturnsEmpty_OriginalUsed(t *testing.T) {
	const original = "SELECT 1"
	var chainGotQuery string

	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, _ string) (string, error) {
			return "", nil // empty rewrite → keep original
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{
		comQuery: func(_ context.Context, _ *vitessmysql.Conn, q string, _ vitessmysql.ResultSpoolFn) error {
			chainGotQuery = q
			return nil
		},
	}

	if err := i.Query(context.Background(), chain, newConn(1), original, noopCallback); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if chainGotQuery != original {
		t.Errorf("chain received query %q, want %q", chainGotQuery, original)
	}
}

func TestQuery_QueryReceived_Error_ShortCircuits(t *testing.T) {
	want := errors.New("query rejected")
	var chainCalled bool

	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, _ string) (string, error) {
			return "", want
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{
		comQuery: func(_ context.Context, _ *vitessmysql.Conn, _ string, _ vitessmysql.ResultSpoolFn) error {
			chainCalled = true
			return nil
		},
	}

	err := i.Query(context.Background(), chain, newConn(1), "SELECT 1", noopCallback)
	if !errors.Is(err, want) {
		t.Errorf("expected %v, got %v", want, err)
	}
	if chainCalled {
		t.Fatal("chain was called despite QueryReceived returning an error")
	}
}

func TestQuery_RowsAffected_AccumulatedFromResults(t *testing.T) {
	var gotRows int64
	events := &stubEventBridge{
		queryCompleted: func(_ uint32, _ string, rowsAffected int64, _ error) {
			gotRows = rowsAffected
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{
		comQuery: func(_ context.Context, _ *vitessmysql.Conn, _ string, callback vitessmysql.ResultSpoolFn) error {
			// Simulate two result batches, each with 3 rows.
			res := &sqltypes.Result{Rows: make([][]sqltypes.Value, 3)}
			if err := callback(res, true); err != nil {
				return err
			}
			return callback(res, false)
		},
	}

	if err := i.Query(context.Background(), chain, newConn(1), "SELECT 1", noopCallback); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if gotRows != 6 {
		t.Errorf("rowsAffected: got %d, want 6", gotRows)
	}
}

// ---------------------------------------------------------------------------
// ParsedQuery — basic lifecycle
// ---------------------------------------------------------------------------

func TestParsedQuery_CallsQueryReceived(t *testing.T) {
	var called bool
	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, _ string) (string, error) {
			called = true
			return "", nil
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{}

	stmt, _ := sqlparser.Parse("SELECT 1")
	if err := i.ParsedQuery(chain, newConn(1), "SELECT 1", stmt, noopCallback); err != nil {
		t.Fatalf("ParsedQuery error: %v", err)
	}
	if !called {
		t.Fatal("QueryReceived was not called by ParsedQuery")
	}
}

func TestParsedQuery_CallsQueryCompleted(t *testing.T) {
	var called bool
	events := &stubEventBridge{
		queryCompleted: func(_ uint32, _ string, _ int64, _ error) {
			called = true
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{}

	stmt, _ := sqlparser.Parse("SELECT 1")
	if err := i.ParsedQuery(chain, newConn(1), "SELECT 1", stmt, noopCallback); err != nil {
		t.Fatalf("ParsedQuery error: %v", err)
	}
	if !called {
		t.Fatal("QueryCompleted was not called by ParsedQuery")
	}
}

func TestParsedQuery_QueryReceived_Error_ShortCircuits(t *testing.T) {
	want := errors.New("parsed query rejected")
	var chainCalled bool

	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, _ string) (string, error) {
			return "", want
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{
		comQuery: func(_ context.Context, _ *vitessmysql.Conn, _ string, _ vitessmysql.ResultSpoolFn) error {
			chainCalled = true
			return nil
		},
	}

	stmt, _ := sqlparser.Parse("SELECT 1")
	err := i.ParsedQuery(chain, newConn(1), "SELECT 1", stmt, noopCallback)
	if !errors.Is(err, want) {
		t.Errorf("expected %v, got %v", want, err)
	}
	if chainCalled {
		t.Fatal("chain was called despite QueryReceived returning an error")
	}
}

// ---------------------------------------------------------------------------
// MultiQuery — basic lifecycle
// ---------------------------------------------------------------------------

func TestMultiQuery_CallsQueryReceived(t *testing.T) {
	var called bool
	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, _ string) (string, error) {
			called = true
			return "", nil
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{}

	if _, err := i.MultiQuery(context.Background(), chain, newConn(1), "SELECT 1", noopCallback); err != nil {
		t.Fatalf("MultiQuery error: %v", err)
	}
	if !called {
		t.Fatal("QueryReceived was not called by MultiQuery")
	}
}

func TestMultiQuery_CallsQueryCompleted(t *testing.T) {
	var called bool
	events := &stubEventBridge{
		queryCompleted: func(_ uint32, _ string, _ int64, _ error) {
			called = true
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{}

	if _, err := i.MultiQuery(context.Background(), chain, newConn(1), "SELECT 1", noopCallback); err != nil {
		t.Fatalf("MultiQuery error: %v", err)
	}
	if !called {
		t.Fatal("QueryCompleted was not called by MultiQuery")
	}
}

func TestMultiQuery_QueryReceived_Error_ShortCircuits(t *testing.T) {
	want := errors.New("multi query rejected")
	var chainCalled bool

	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, _ string) (string, error) {
			return "", want
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{
		comMultiQuery: func(_ context.Context, _ *vitessmysql.Conn, _ string, _ vitessmysql.ResultSpoolFn) (string, error) {
			chainCalled = true
			return "", nil
		},
	}

	_, err := i.MultiQuery(context.Background(), chain, newConn(1), "SELECT 1", noopCallback)
	if !errors.Is(err, want) {
		t.Errorf("expected %v, got %v", want, err)
	}
	if chainCalled {
		t.Fatal("chain was called despite QueryReceived returning an error")
	}
}

// ---------------------------------------------------------------------------
// Prepare and StmtExecute — delegation only
// ---------------------------------------------------------------------------

func TestPrepare_DelegatesToChain(t *testing.T) {
	var chainCalled bool
	i := newInterceptor(&stubEventBridge{})
	chain := &stubChain{
		comPrepare: func(_ context.Context, _ *vitessmysql.Conn, _ string, _ *vitessmysql.PrepareData) ([]*querypb.Field, error) {
			chainCalled = true
			return nil, nil
		},
	}

	if _, err := i.Prepare(context.Background(), chain, newConn(1), "SELECT ?", &vitessmysql.PrepareData{}); err != nil {
		t.Fatalf("Prepare error: %v", err)
	}
	if !chainCalled {
		t.Fatal("ComPrepare was not called by Prepare")
	}
}

func TestStmtExecute_DelegatesToChain(t *testing.T) {
	var chainCalled bool
	i := newInterceptor(&stubEventBridge{})
	chain := &stubChain{
		comStmtExec: func(_ context.Context, _ *vitessmysql.Conn, _ *vitessmysql.PrepareData, _ func(*sqltypes.Result) error) error {
			chainCalled = true
			return nil
		},
	}

	if err := i.StmtExecute(context.Background(), chain, newConn(1), &vitessmysql.PrepareData{}, func(_ *sqltypes.Result) error { return nil }); err != nil {
		t.Fatalf("StmtExecute error: %v", err)
	}
	if !chainCalled {
		t.Fatal("ComStmtExecute was not called by StmtExecute")
	}
}

// ---------------------------------------------------------------------------
// Context helpers — tested indirectly via Query
// ---------------------------------------------------------------------------

func TestDatabaseFromContext_WithValidSession_ReturnsDatabase(t *testing.T) {
	var gotDatabase string
	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, database string) (string, error) {
			gotDatabase = database
			return "", nil
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{}

	// Build a GMS sql.Context with a session that has a current database.
	base := gmssql.NewBaseSessionWithClientServer(
		"127.0.0.1:3306",
		gmssql.Client{User: "user", Address: "127.0.0.1:3306"},
		1,
	)
	base.SetCurrentDatabase("mydb")
	ctx := gmssql.NewContext(context.Background(), gmssql.WithSession(base))

	if err := i.Query(ctx, chain, newConn(1), "SELECT 1", noopCallback); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if gotDatabase != "mydb" {
		t.Errorf("database: got %q, want %q", gotDatabase, "mydb")
	}
}

func TestDatabaseFromContext_WithNilContext_ReturnsEmpty(t *testing.T) {
	var gotDatabase string
	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, database string) (string, error) {
			gotDatabase = database
			return "", nil
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{}

	// Pass a plain context.Background() — not a *gmssql.Context.
	if err := i.Query(context.Background(), chain, newConn(1), "SELECT 1", noopCallback); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if gotDatabase != "" {
		t.Errorf("database: got %q, want empty string", gotDatabase)
	}
}

func TestDatabaseFromContext_WithNonSQLContext_ReturnsEmpty(t *testing.T) {
	var gotDatabase string
	events := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, database string) (string, error) {
			gotDatabase = database
			return "", nil
		},
	}
	i := newInterceptor(events)
	chain := &stubChain{}

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "somevalue")

	if err := i.Query(ctx, chain, newConn(1), "SELECT 1", noopCallback); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if gotDatabase != "" {
		t.Errorf("database: got %q, want empty string", gotDatabase)
	}
}
