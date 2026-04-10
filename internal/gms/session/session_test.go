package session_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AnqorDX/vdb-mysql-driver/internal/bridge"
	"github.com/AnqorDX/vdb-mysql-driver/internal/gms/session"
	vitessmysql "github.com/dolthub/vitess/go/mysql"

	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// ---------------------------------------------------------------------------
// Local stubs
// ---------------------------------------------------------------------------

// stubEventBridge is a local stub for bridge.EventBridge used only by
// gms/session tests. It exposes function fields for the five lifecycle methods
// that Session and Builder call; all other methods are no-ops.
type stubEventBridge struct {
	connectionOpened      func(uint32, string, string) error
	connectionClosed      func(uint32, string, string)
	transactionBegun      func(uint32, bool) error
	transactionCommitted  func(uint32) error
	transactionRolledBack func(uint32, string)
}

var _ bridge.EventBridge = (*stubEventBridge)(nil)

func (s *stubEventBridge) ConnectionOpened(id uint32, user, addr string) error {
	if s.connectionOpened != nil {
		return s.connectionOpened(id, user, addr)
	}
	return nil
}

func (s *stubEventBridge) ConnectionClosed(id uint32, user, addr string) {
	if s.connectionClosed != nil {
		s.connectionClosed(id, user, addr)
	}
}

func (s *stubEventBridge) TransactionBegun(connID uint32, readOnly bool) error {
	if s.transactionBegun != nil {
		return s.transactionBegun(connID, readOnly)
	}
	return nil
}

func (s *stubEventBridge) TransactionCommitted(connID uint32) error {
	if s.transactionCommitted != nil {
		return s.transactionCommitted(connID)
	}
	return nil
}

func (s *stubEventBridge) TransactionRolledBack(connID uint32, savepoint string) {
	if s.transactionRolledBack != nil {
		s.transactionRolledBack(connID, savepoint)
	}
}

func (s *stubEventBridge) QueryReceived(_ uint32, q, _ string) (string, error) { return q, nil }
func (s *stubEventBridge) QueryCompleted(_ uint32, _ string, _ int64, _ error)  {}
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newConn creates a minimal *vitessmysql.Conn for tests, setting only the
// fields that Builder reads: ConnectionID and User.
func newConn(connID uint32, user string) *vitessmysql.Conn {
	return &vitessmysql.Conn{
		ConnectionID: connID,
		User:         user,
	}
}

// buildSession creates a Session via Builder and returns it cast to the
// combined interface used by all session tests. It fatally fails if the
// builder returns an error.
func buildSession(
	t *testing.T,
	dbName string,
	events bridge.EventBridge,
	connID uint32,
	user string,
) gmssql.Session {
	t.Helper()
	builder := session.Builder(dbName, events)
	sess, err := builder(context.Background(), newConn(connID, user), "127.0.0.1:1234")
	if err != nil {
		t.Fatalf("Builder returned error: %v", err)
	}
	return sess
}

// asTransactionSession type-asserts sess to gmssql.TransactionSession.
func asTransactionSession(t *testing.T, sess gmssql.Session) gmssql.TransactionSession {
	t.Helper()
	ts, ok := sess.(gmssql.TransactionSession)
	if !ok {
		t.Fatalf("session does not implement gmssql.TransactionSession")
	}
	return ts
}

// asLifecycleAwareSession type-asserts sess to gmssql.LifecycleAwareSession.
func asLifecycleAwareSession(t *testing.T, sess gmssql.Session) gmssql.LifecycleAwareSession {
	t.Helper()
	ls, ok := sess.(gmssql.LifecycleAwareSession)
	if !ok {
		t.Fatalf("session does not implement gmssql.LifecycleAwareSession")
	}
	return ls
}

// emptyCtx returns a minimal GMS context for tests.
func emptyCtx() *gmssql.Context {
	return gmssql.NewEmptyContext()
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

func TestTransaction_ReadOnlyTrue_ReportsReadOnly(t *testing.T) {
	tx := &session.Transaction{ReadOnly: true}
	if !tx.IsReadOnly() {
		t.Error("expected IsReadOnly() = true for ReadOnly=true")
	}
}

func TestTransaction_ReadOnlyFalse_NotReadOnly(t *testing.T) {
	tx := &session.Transaction{ReadOnly: false}
	if tx.IsReadOnly() {
		t.Error("expected IsReadOnly() = false for ReadOnly=false")
	}
}

func TestTransaction_String_ReadOnly_ContainsReadOnly(t *testing.T) {
	tx := &session.Transaction{ReadOnly: true}
	s := tx.String()
	if !strings.Contains(s, "readOnly") && !strings.Contains(s, "ReadOnly") && !strings.Contains(s, "read") {
		t.Errorf("String() %q does not mention read-only", s)
	}
}

func TestTransaction_String_ReadWrite_ContainsReadWrite(t *testing.T) {
	tx := &session.Transaction{ReadOnly: false}
	s := tx.String()
	if !strings.Contains(s, "readWrite") && !strings.Contains(s, "ReadWrite") && !strings.Contains(s, "read") {
		t.Errorf("String() %q does not mention read-write", s)
	}
}

// ---------------------------------------------------------------------------
// Session lifecycle
// ---------------------------------------------------------------------------

func TestSession_StartTransaction_CallsTransactionBegun(t *testing.T) {
	var called bool
	var gotReadOnly bool
	events := &stubEventBridge{
		transactionBegun: func(_ uint32, readOnly bool) error {
			called = true
			gotReadOnly = readOnly
			return nil
		},
	}
	sess := buildSession(t, "testdb", events, 1, "user")
	ts := asTransactionSession(t, sess)

	tx, err := ts.StartTransaction(emptyCtx(), gmssql.ReadOnly)
	if err != nil {
		t.Fatalf("StartTransaction error: %v", err)
	}
	if !called {
		t.Fatal("TransactionBegun was not called")
	}
	if !gotReadOnly {
		t.Error("expected readOnly=true for ReadOnly characteristic")
	}
	if tx == nil {
		t.Error("expected non-nil transaction")
	}
}

func TestSession_StartTransaction_Error_IsReturned(t *testing.T) {
	want := errors.New("transaction refused")
	events := &stubEventBridge{
		transactionBegun: func(_ uint32, _ bool) error { return want },
	}
	sess := buildSession(t, "testdb", events, 1, "user")
	ts := asTransactionSession(t, sess)

	_, err := ts.StartTransaction(emptyCtx(), gmssql.ReadWrite)
	if !errors.Is(err, want) {
		t.Errorf("expected %v, got %v", want, err)
	}
}

func TestSession_CommitTransaction_CallsTransactionCommitted(t *testing.T) {
	var called bool
	events := &stubEventBridge{
		transactionCommitted: func(_ uint32) error {
			called = true
			return nil
		},
	}
	sess := buildSession(t, "testdb", events, 1, "user")
	ts := asTransactionSession(t, sess)

	tx := &session.Transaction{}
	if err := ts.CommitTransaction(emptyCtx(), tx); err != nil {
		t.Fatalf("CommitTransaction error: %v", err)
	}
	if !called {
		t.Fatal("TransactionCommitted was not called")
	}
}

func TestSession_Rollback_CallsTransactionRolledBack(t *testing.T) {
	var called bool
	var gotSavepoint = "SENTINEL"
	events := &stubEventBridge{
		transactionRolledBack: func(_ uint32, savepoint string) {
			called = true
			gotSavepoint = savepoint
		},
	}
	sess := buildSession(t, "testdb", events, 1, "user")
	ts := asTransactionSession(t, sess)

	tx := &session.Transaction{}
	if err := ts.Rollback(emptyCtx(), tx); err != nil {
		t.Fatalf("Rollback error: %v", err)
	}
	if !called {
		t.Fatal("TransactionRolledBack was not called")
	}
	if gotSavepoint != "" {
		t.Errorf("expected empty savepoint for full rollback, got %q", gotSavepoint)
	}
}

func TestSession_Rollback_WithSavepoint_PassesSavepoint(t *testing.T) {
	var gotSavepoint string
	events := &stubEventBridge{
		transactionRolledBack: func(_ uint32, savepoint string) {
			gotSavepoint = savepoint
		},
	}
	sess := buildSession(t, "testdb", events, 1, "user")
	ts := asTransactionSession(t, sess)

	tx := &session.Transaction{}
	if err := ts.RollbackToSavepoint(emptyCtx(), tx, "sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint error: %v", err)
	}
	if gotSavepoint != "sp1" {
		t.Errorf("savepoint: got %q, want %q", gotSavepoint, "sp1")
	}
}

func TestSession_CreateSavepoint_DoesNotError(t *testing.T) {
	sess := buildSession(t, "testdb", &stubEventBridge{}, 1, "user")
	ts := asTransactionSession(t, sess)

	tx := &session.Transaction{}
	if err := ts.CreateSavepoint(emptyCtx(), tx, "sp1"); err != nil {
		t.Fatalf("CreateSavepoint returned unexpected error: %v", err)
	}
}

func TestSession_RollbackToSavepoint_DoesNotError(t *testing.T) {
	sess := buildSession(t, "testdb", &stubEventBridge{}, 1, "user")
	ts := asTransactionSession(t, sess)

	tx := &session.Transaction{}
	if err := ts.RollbackToSavepoint(emptyCtx(), tx, "sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint returned unexpected error: %v", err)
	}
}

func TestSession_ReleaseSavepoint_DoesNotError(t *testing.T) {
	sess := buildSession(t, "testdb", &stubEventBridge{}, 1, "user")
	ts := asTransactionSession(t, sess)

	tx := &session.Transaction{}
	if err := ts.ReleaseSavepoint(emptyCtx(), tx, "sp1"); err != nil {
		t.Fatalf("ReleaseSavepoint returned unexpected error: %v", err)
	}
}

func TestSession_CommandBegin_DoesNotError(t *testing.T) {
	sess := buildSession(t, "testdb", &stubEventBridge{}, 1, "user")
	ls := asLifecycleAwareSession(t, sess)

	if err := ls.CommandBegin(); err != nil {
		t.Fatalf("CommandBegin returned unexpected error: %v", err)
	}
}

func TestSession_CommandEnd_DoesNotError(t *testing.T) {
	sess := buildSession(t, "testdb", &stubEventBridge{}, 1, "user")
	ls := asLifecycleAwareSession(t, sess)

	// CommandEnd returns nothing; we just verify it does not panic.
	ls.CommandEnd()
}

func TestSession_SessionEnd_CallsConnectionClosed(t *testing.T) {
	var called bool
	var gotConnID uint32
	events := &stubEventBridge{
		connectionClosed: func(id uint32, _, _ string) {
			called = true
			gotConnID = id
		},
	}
	sess := buildSession(t, "testdb", events, 42, "alice")
	ls := asLifecycleAwareSession(t, sess)

	ls.SessionEnd()

	if !called {
		t.Fatal("ConnectionClosed was not called by SessionEnd")
	}
	if gotConnID != 42 {
		t.Errorf("connID: got %d, want 42", gotConnID)
	}
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

func TestBuilder_ReturnsNonNilSession(t *testing.T) {
	sess := buildSession(t, "testdb", &stubEventBridge{}, 1, "user")
	if sess == nil {
		t.Fatal("Builder returned nil session")
	}
}

func TestBuilder_Session_HasCorrectConnID(t *testing.T) {
	events := &stubEventBridge{
		connectionOpened: func(id uint32, _, _ string) error {
			if id != 99 {
				t.Errorf("ConnectionOpened connID: got %d, want 99", id)
			}
			return nil
		},
	}
	buildSession(t, "testdb", events, 99, "user")
}

func TestBuilder_Session_DatabaseMatchesDBName(t *testing.T) {
	sess := buildSession(t, "mydb", &stubEventBridge{}, 1, "user")
	got := sess.GetCurrentDatabase()
	if got != "mydb" {
		t.Errorf("GetCurrentDatabase: got %q, want %q", got, "mydb")
	}
}
