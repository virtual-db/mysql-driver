package schema_test

import (
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/AnqorDX/vdb-mysql-driver/internal/schema"
)

// ---------------------------------------------------------------------------
// Local stubs
// ---------------------------------------------------------------------------

// stubSchemaProvider satisfies schema.Provider for tests with configurable
// return values.
type stubSchemaProvider struct {
	cols  []string
	pkCol string
	err   error
}

func (s *stubSchemaProvider) GetSchema(_ string) ([]string, string, error) {
	return s.cols, s.pkCol, s.err
}

var _ schema.Provider = (*stubSchemaProvider)(nil)

// stubLoadListener satisfies schema.LoadListener for tests. It exposes a
// function field for SchemaLoaded; when nil, SchemaLoaded is a no-op.
type stubLoadListener struct {
	schemaLoaded func(table string, cols []string, pkCol string)
}

func (s *stubLoadListener) SchemaLoaded(table string, cols []string, pkCol string) {
	if s.schemaLoaded != nil {
		s.schemaLoaded(table, cols, pkCol)
	}
}

var _ schema.LoadListener = (*stubLoadListener)(nil)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newMockDB is a helper that returns a *sql.DB backed by sqlmock and the
// associated mock controller. The test fails immediately if setup fails.
func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

// ---------------------------------------------------------------------------
// schema.NewSQLProvider — GetSchema
// ---------------------------------------------------------------------------

func TestGetSchema_ColumnsReturnedInOrdinalOrder(t *testing.T) {
	db, mock := newMockDB(t)

	colRows := sqlmock.NewRows([]string{"COLUMN_NAME"}).
		AddRow("id").
		AddRow("name").
		AddRow("email")
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "users").
		WillReturnRows(colRows)

	pkRows := sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id")
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "users").
		WillReturnRows(pkRows)

	p := schema.NewSQLProvider(db, "testdb")
	cols, _, err := p.GetSchema("users")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"id", "name", "email"}
	if len(cols) != len(want) {
		t.Fatalf("column count: got %d, want %d", len(cols), len(want))
	}
	for i, c := range cols {
		if c != want[i] {
			t.Errorf("col[%d]: got %q, want %q", i, c, want[i])
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestGetSchema_PrimaryKeyColumnIdentified(t *testing.T) {
	db, mock := newMockDB(t)

	colRows := sqlmock.NewRows([]string{"COLUMN_NAME"}).
		AddRow("id").
		AddRow("name")
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "users").
		WillReturnRows(colRows)

	pkRows := sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id")
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "users").
		WillReturnRows(pkRows)

	p := schema.NewSQLProvider(db, "testdb")
	_, pk, err := p.GetSchema("users")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pk != "id" {
		t.Errorf("pkCol: got %q, want %q", pk, "id")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestGetSchema_MissingTable_ReturnsError(t *testing.T) {
	db, mock := newMockDB(t)

	colRows := sqlmock.NewRows([]string{"COLUMN_NAME"})
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "nonexistent_table").
		WillReturnRows(colRows)

	p := schema.NewSQLProvider(db, "testdb")
	_, _, err := p.GetSchema("nonexistent_table")

	if err == nil {
		t.Fatal("expected a non-nil error for missing table, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent_table") {
		t.Errorf("error %q does not mention table name %q", err.Error(), "nonexistent_table")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestGetSchema_NoPrimaryKey_ReturnsEmptyPKColAndNoError(t *testing.T) {
	db, mock := newMockDB(t)

	colRows := sqlmock.NewRows([]string{"COLUMN_NAME"}).
		AddRow("event_id").
		AddRow("message")
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "log_entries").
		WillReturnRows(colRows)

	pkRows := sqlmock.NewRows([]string{"COLUMN_NAME"})
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "log_entries").
		WillReturnRows(pkRows)

	p := schema.NewSQLProvider(db, "testdb")
	cols, pk, err := p.GetSchema("log_entries")

	if err != nil {
		t.Fatalf("unexpected error for table with no pk: %v", err)
	}
	if pk != "" {
		t.Errorf("pkCol: got %q, want empty string for table with no primary key", pk)
	}
	if len(cols) == 0 {
		t.Error("expected non-empty columns for existing table")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestGetSchema_DatabaseError_ReturnsWrappedError(t *testing.T) {
	db, mock := newMockDB(t)

	dbErr := errors.New("connection refused")
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "broken").
		WillReturnError(dbErr)

	p := schema.NewSQLProvider(db, "testdb")
	_, _, err := p.GetSchema("broken")

	if err == nil {
		t.Fatal("expected non-nil error, got nil")
	}
	if !errors.Is(err, dbErr) {
		t.Errorf("error chain does not wrap original error: got %v", err)
	}
}

func TestGetSchema_ConcurrentCallsDoNotRace(t *testing.T) {
	const goroutines = 10
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			db, mock, err := sqlmock.New()
			if err != nil {
				errs <- err
				return
			}
			defer db.Close()

			colRows := sqlmock.NewRows([]string{"COLUMN_NAME"}).
				AddRow("id").AddRow("val")
			mock.ExpectQuery("SELECT COLUMN_NAME").
				WithArgs("testdb", "shared").
				WillReturnRows(colRows)

			pkRows := sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id")
			mock.ExpectQuery("SELECT COLUMN_NAME").
				WithArgs("testdb", "shared").
				WillReturnRows(pkRows)

			p := schema.NewSQLProvider(db, "testdb")
			cols, _, err := p.GetSchema("shared")
			if err != nil {
				errs <- err
				return
			}
			if len(cols) == 0 {
				errs <- errors.New("got empty columns")
			}
		}()
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		t.Errorf("goroutine error: %v", e)
	}
}

func TestNewSQLSchemaProvider_ReturnsSchemaProvider(t *testing.T) {
	db, _ := newMockDB(t)
	var _ schema.Provider = schema.NewSQLProvider(db, "testdb")
}

// ---------------------------------------------------------------------------
// schema.ToGMSSchema
// ---------------------------------------------------------------------------

func TestToGMSSchema_LengthMatchesColumnCount(t *testing.T) {
	cols := []string{"id", "name", "created_at"}
	gmsSchema := schema.ToGMSSchema("orders", cols)

	if len(gmsSchema) != len(cols) {
		t.Fatalf("schema length: got %d, want %d", len(gmsSchema), len(cols))
	}
}

func TestToGMSSchema_ColumnNamesPreserved(t *testing.T) {
	cols := []string{"sku", "price", "stock"}
	gmsSchema := schema.ToGMSSchema("products", cols)

	for i, col := range gmsSchema {
		if col.Name != cols[i] {
			t.Errorf("schema[%d].Name: got %q, want %q", i, col.Name, cols[i])
		}
	}
}

func TestToGMSSchema_SourceSetToTableName(t *testing.T) {
	gmsSchema := schema.ToGMSSchema("users", []string{"id", "email"})

	for i, col := range gmsSchema {
		if col.Source != "users" {
			t.Errorf("schema[%d].Source: got %q, want %q", i, col.Source, "users")
		}
	}
}

func TestToGMSSchema_AllColumnsNullable(t *testing.T) {
	gmsSchema := schema.ToGMSSchema("events", []string{"id", "payload"})

	for i, col := range gmsSchema {
		if !col.Nullable {
			t.Errorf("schema[%d] (%q): expected Nullable=true, got false", i, col.Name)
		}
	}
}

func TestToGMSSchema_EmptyColumns_ReturnsEmptySchema(t *testing.T) {
	gmsSchema := schema.ToGMSSchema("empty_table", nil)

	if gmsSchema == nil {
		t.Fatal("ToGMSSchema returned nil schema for empty column list")
	}
	if len(gmsSchema) != 0 {
		t.Errorf("schema length: got %d, want 0", len(gmsSchema))
	}
}

func TestToGMSSchema_TypeIsNonNil(t *testing.T) {
	gmsSchema := schema.ToGMSSchema("accounts", []string{"id", "balance", "name"})

	for i, col := range gmsSchema {
		if col.Type == nil {
			t.Errorf("schema[%d] (%q): Type is nil", i, col.Name)
		}
	}
}

// ---------------------------------------------------------------------------
// schema.NotifyingProvider
// ---------------------------------------------------------------------------

func TestWrappedSchemaProvider_OnSuccess_CallsOnLoad(t *testing.T) {
	var called bool
	var gotTable string
	var gotCols []string
	var gotPK string

	inner := &stubSchemaProvider{
		cols:  []string{"id", "name", "email"},
		pkCol: "id",
	}

	listener := &stubLoadListener{
		schemaLoaded: func(table string, cols []string, pkCol string) {
			called = true
			gotTable = table
			gotCols = cols
			gotPK = pkCol
		},
	}

	w := schema.NewNotifyingProvider(inner, listener)

	cols, pk, err := w.GetSchema("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("SchemaLoaded was not called on a successful GetSchema")
	}
	if gotTable != "users" {
		t.Errorf("listener table: got %q, want %q", gotTable, "users")
	}
	wantCols := []string{"id", "name", "email"}
	if len(gotCols) != len(wantCols) {
		t.Fatalf("listener cols length: got %d, want %d", len(gotCols), len(wantCols))
	}
	for i, c := range wantCols {
		if gotCols[i] != c {
			t.Errorf("listener cols[%d]: got %q, want %q", i, gotCols[i], c)
		}
	}
	if gotPK != "id" {
		t.Errorf("listener pkCol: got %q, want %q", gotPK, "id")
	}
	if len(cols) != len(wantCols) {
		t.Fatalf("caller cols length: got %d, want %d", len(cols), len(wantCols))
	}
	if pk != "id" {
		t.Errorf("caller pk: got %q, want %q", pk, "id")
	}
}

func TestWrappedSchemaProvider_OnError_SuppressesOnLoad(t *testing.T) {
	innerErr := errors.New("table not found in source database")
	inner := &stubSchemaProvider{err: innerErr}

	var called bool
	listener := &stubLoadListener{
		schemaLoaded: func(_ string, _ []string, _ string) { called = true },
	}

	w := schema.NewNotifyingProvider(inner, listener)

	_, _, err := w.GetSchema("missing_table")
	if err == nil {
		t.Fatal("expected non-nil error, got nil")
	}
	if !errors.Is(err, innerErr) {
		t.Errorf("error chain: got %v, expected it to wrap %v", err, innerErr)
	}
	if called {
		t.Fatal("SchemaLoaded was called despite an error from the inner provider")
	}
}

func TestWrappedSchemaProvider_NilOnLoad_DoesNotPanic(t *testing.T) {
	inner := &stubSchemaProvider{cols: []string{"id"}, pkCol: "id"}
	w := schema.NewNotifyingProvider(inner, nil)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NotifyingProvider panicked with nil listener: %v", r)
		}
	}()

	cols, pk, err := w.GetSchema("accounts")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cols) == 0 {
		t.Error("expected non-empty column slice, got empty")
	}
	if pk != "id" {
		t.Errorf("pk: got %q, want %q", pk, "id")
	}
}

func TestWrappedSchemaProvider_ReturnValuesMatchInner(t *testing.T) {
	inner := &stubSchemaProvider{
		cols:  []string{"sku", "price", "stock"},
		pkCol: "sku",
	}
	listener := &stubLoadListener{}

	w := schema.NewNotifyingProvider(inner, listener)

	cols, pk, err := w.GetSchema("products")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantCols := []string{"sku", "price", "stock"}
	if len(cols) != len(wantCols) {
		t.Fatalf("cols length: got %d, want %d", len(cols), len(wantCols))
	}
	for i, c := range wantCols {
		if cols[i] != c {
			t.Errorf("cols[%d]: got %q, want %q", i, cols[i], c)
		}
	}
	if pk != "sku" {
		t.Errorf("pk: got %q, want %q", pk, "sku")
	}
}

func TestWrappedSchemaProvider_NoPrimaryKey_PassedThrough(t *testing.T) {
	inner := &stubSchemaProvider{
		cols:  []string{"event_id", "message"},
		pkCol: "",
	}
	var gotPK = "SENTINEL"
	listener := &stubLoadListener{
		schemaLoaded: func(_ string, _ []string, pkCol string) {
			gotPK = pkCol
		},
	}

	w := schema.NewNotifyingProvider(inner, listener)

	_, pk, err := w.GetSchema("log_entries")
	if err != nil {
		t.Fatalf("unexpected error for table with no pk: %v", err)
	}
	if pk != "" {
		t.Errorf("caller pk: got %q, want empty string", pk)
	}
	if gotPK != "" {
		t.Errorf("listener pkCol: got %q, want empty string", gotPK)
	}
}
