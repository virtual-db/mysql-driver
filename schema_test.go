package driver

import (
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

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
// GetSchema — column names
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

	p := newSQLSchemaProvider(db, "testdb")
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

	p := newSQLSchemaProvider(db, "testdb")
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

	p := newSQLSchemaProvider(db, "testdb")
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

	p := newSQLSchemaProvider(db, "testdb")
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

	p := newSQLSchemaProvider(db, "testdb")
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

			p := newSQLSchemaProvider(db, "testdb")
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
	var _ schemaProvider = newSQLSchemaProvider(db, "testdb")
}

// ---------------------------------------------------------------------------
// toGMSSchema
// ---------------------------------------------------------------------------

func TestToGMSSchema_LengthMatchesColumnCount(t *testing.T) {
	cols := []string{"id", "name", "created_at"}
	schema := toGMSSchema("orders", cols)

	if len(schema) != len(cols) {
		t.Fatalf("schema length: got %d, want %d", len(schema), len(cols))
	}
}

func TestToGMSSchema_ColumnNamesPreserved(t *testing.T) {
	cols := []string{"sku", "price", "stock"}
	schema := toGMSSchema("products", cols)

	for i, col := range schema {
		if col.Name != cols[i] {
			t.Errorf("schema[%d].Name: got %q, want %q", i, col.Name, cols[i])
		}
	}
}

func TestToGMSSchema_SourceSetToTableName(t *testing.T) {
	schema := toGMSSchema("users", []string{"id", "email"})

	for i, col := range schema {
		if col.Source != "users" {
			t.Errorf("schema[%d].Source: got %q, want %q", i, col.Source, "users")
		}
	}
}

func TestToGMSSchema_AllColumnsNullable(t *testing.T) {
	schema := toGMSSchema("events", []string{"id", "payload"})

	for i, col := range schema {
		if !col.Nullable {
			t.Errorf("schema[%d] (%q): expected Nullable=true, got false", i, col.Name)
		}
	}
}

func TestToGMSSchema_EmptyColumns_ReturnsEmptySchema(t *testing.T) {
	schema := toGMSSchema("empty_table", nil)

	if schema == nil {
		t.Fatal("toGMSSchema returned nil schema for empty column list")
	}
	if len(schema) != 0 {
		t.Errorf("schema length: got %d, want 0", len(schema))
	}
}

func TestToGMSSchema_TypeIsNonNil(t *testing.T) {
	schema := toGMSSchema("accounts", []string{"id", "balance", "name"})

	for i, col := range schema {
		if col.Type == nil {
			t.Errorf("schema[%d] (%q): Type is nil", i, col.Name)
		}
	}
}
