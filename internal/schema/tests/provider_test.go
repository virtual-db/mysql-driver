package schema_test

import (
	"database/sql"
	"errors"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	. "github.com/virtual-db/vdb-mysql-driver/internal/schema"
)

func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

// columnRows returns a sqlmock row set for the full 8-column INFORMATION_SCHEMA
// query used by SQLProvider.GetSchema.
func columnRows(cols ...struct {
	name          string
	dataType      string
	columnType    string
	isNullable    string
	columnKey     string
	columnDefault *string
	extra         string
	charMaxLength *int64
}) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{
		"COLUMN_NAME",
		"DATA_TYPE",
		"COLUMN_TYPE",
		"IS_NULLABLE",
		"COLUMN_KEY",
		"COLUMN_DEFAULT",
		"EXTRA",
		"CHARACTER_MAXIMUM_LENGTH",
	})
	for _, c := range cols {
		rows.AddRow(
			c.name,
			c.dataType,
			c.columnType,
			c.isNullable,
			c.columnKey,
			c.columnDefault,
			c.extra,
			c.charMaxLength,
		)
	}
	return rows
}

func strPtr(s string) *string { return &s }
func i64Ptr(n int64) *int64   { return &n }

// ---------------------------------------------------------------------------
// GetSchema — column ordering and names
// ---------------------------------------------------------------------------

func TestGetSchema_ColumnsReturnedInOrdinalOrder(t *testing.T) {
	db, mock := newMockDB(t)

	type col = struct {
		name          string
		dataType      string
		columnType    string
		isNullable    string
		columnKey     string
		columnDefault *string
		extra         string
		charMaxLength *int64
	}

	mock.ExpectQuery("SELECT").
		WithArgs("testdb", "users").
		WillReturnRows(columnRows(
			col{"id", "int", "int", "NO", "PRI", nil, "auto_increment", nil},
			col{"name", "varchar", "varchar(64)", "NO", "", nil, "", i64Ptr(64)},
			col{"email", "varchar", "varchar(128)", "YES", "", nil, "", i64Ptr(128)},
		))

	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id"))

	p := NewSQLProvider(db, "testdb")
	cols, _, err := p.GetSchema("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"id", "name", "email"}
	if len(cols) != len(want) {
		t.Fatalf("column count: got %d, want %d", len(cols), len(want))
	}
	for i, c := range cols {
		if c.Name != want[i] {
			t.Errorf("col[%d].Name: got %q, want %q", i, c.Name, want[i])
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetSchema — ColumnDescriptor field population
// ---------------------------------------------------------------------------

func TestGetSchema_ColumnDescriptor_TypeFields(t *testing.T) {
	db, mock := newMockDB(t)

	type col = struct {
		name          string
		dataType      string
		columnType    string
		isNullable    string
		columnKey     string
		columnDefault *string
		extra         string
		charMaxLength *int64
	}

	mock.ExpectQuery("SELECT").
		WithArgs("testdb", "items").
		WillReturnRows(columnRows(
			col{"id", "int", "int", "NO", "PRI", nil, "auto_increment", nil},
			col{"name", "varchar", "varchar(80)", "NO", "", nil, "", i64Ptr(80)},
			col{"qty", "int", "int", "NO", "", strPtr("1"), "", nil},
			col{"bio", "text", "text", "YES", "", nil, "", nil},
		))

	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "items").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id"))

	p := NewSQLProvider(db, "testdb")
	cols, pk, err := p.GetSchema("items")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pk != "id" {
		t.Errorf("pkCol: got %q, want %q", pk, "id")
	}

	// id
	if cols[0].DataType != "int" {
		t.Errorf("cols[0].DataType: got %q, want %q", cols[0].DataType, "int")
	}
	if cols[0].IsNullable != "NO" {
		t.Errorf("cols[0].IsNullable: got %q, want %q", cols[0].IsNullable, "NO")
	}
	if cols[0].ColumnKey != "PRI" {
		t.Errorf("cols[0].ColumnKey: got %q, want %q", cols[0].ColumnKey, "PRI")
	}
	if cols[0].Extra != "auto_increment" {
		t.Errorf("cols[0].Extra: got %q, want %q", cols[0].Extra, "auto_increment")
	}
	if cols[0].ColumnDefault != nil {
		t.Errorf("cols[0].ColumnDefault: got %v, want nil", cols[0].ColumnDefault)
	}

	// name — varchar with length
	if cols[1].DataType != "varchar" {
		t.Errorf("cols[1].DataType: got %q, want %q", cols[1].DataType, "varchar")
	}
	if cols[1].CharMaxLength == nil || *cols[1].CharMaxLength != 80 {
		t.Errorf("cols[1].CharMaxLength: got %v, want 80", cols[1].CharMaxLength)
	}

	// qty — int with default
	if cols[2].ColumnDefault == nil {
		t.Errorf("cols[2].ColumnDefault: got nil, want non-nil (default='1')")
	} else if *cols[2].ColumnDefault != "1" {
		t.Errorf("cols[2].ColumnDefault: got %q, want %q", *cols[2].ColumnDefault, "1")
	}

	// bio — nullable text, no default
	if cols[3].IsNullable != "YES" {
		t.Errorf("cols[3].IsNullable: got %q, want %q", cols[3].IsNullable, "YES")
	}
	if cols[3].ColumnDefault != nil {
		t.Errorf("cols[3].ColumnDefault: got %v, want nil", cols[3].ColumnDefault)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetSchema — primary key identification
// ---------------------------------------------------------------------------

func TestGetSchema_PrimaryKeyColumnIdentified(t *testing.T) {
	db, mock := newMockDB(t)

	type col = struct {
		name          string
		dataType      string
		columnType    string
		isNullable    string
		columnKey     string
		columnDefault *string
		extra         string
		charMaxLength *int64
	}

	mock.ExpectQuery("SELECT").
		WithArgs("testdb", "users").
		WillReturnRows(columnRows(
			col{"id", "int", "int", "NO", "PRI", nil, "auto_increment", nil},
			col{"name", "varchar", "varchar(64)", "NO", "", nil, "", i64Ptr(64)},
		))

	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id"))

	p := NewSQLProvider(db, "testdb")
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

// ---------------------------------------------------------------------------
// GetSchema — error conditions
// ---------------------------------------------------------------------------

func TestGetSchema_MissingTable_ReturnsError(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT").
		WithArgs("testdb", "nonexistent_table").
		WillReturnRows(sqlmock.NewRows([]string{
			"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
			"IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT",
			"EXTRA", "CHARACTER_MAXIMUM_LENGTH",
		}))

	p := NewSQLProvider(db, "testdb")
	_, _, err := p.GetSchema("nonexistent_table")
	if err == nil {
		t.Fatal("expected a non-nil error for missing table, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestGetSchema_NoPrimaryKey_ReturnsEmptyPKColAndNoError(t *testing.T) {
	db, mock := newMockDB(t)

	type col = struct {
		name          string
		dataType      string
		columnType    string
		isNullable    string
		columnKey     string
		columnDefault *string
		extra         string
		charMaxLength *int64
	}

	mock.ExpectQuery("SELECT").
		WithArgs("testdb", "log_entries").
		WillReturnRows(columnRows(
			col{"event_id", "bigint", "bigint", "NO", "", nil, "", nil},
			col{"message", "text", "text", "YES", "", nil, "", nil},
		))

	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs("testdb", "log_entries").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}))

	p := NewSQLProvider(db, "testdb")
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
	mock.ExpectQuery("SELECT").
		WithArgs("testdb", "broken").
		WillReturnError(dbErr)

	p := NewSQLProvider(db, "testdb")
	_, _, err := p.GetSchema("broken")
	if err == nil {
		t.Fatal("expected non-nil error, got nil")
	}
	if !errors.Is(err, dbErr) {
		t.Errorf("error chain does not wrap original error: got %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetSchema — concurrency
// ---------------------------------------------------------------------------

func TestGetSchema_ConcurrentCallsDoNotRace(t *testing.T) {
	const goroutines = 10
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	type col = struct {
		name          string
		dataType      string
		columnType    string
		isNullable    string
		columnKey     string
		columnDefault *string
		extra         string
		charMaxLength *int64
	}

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

			mock.ExpectQuery("SELECT").
				WithArgs("testdb", "shared").
				WillReturnRows(columnRows(
					col{"id", "int", "int", "NO", "PRI", nil, "auto_increment", nil},
					col{"val", "varchar", "varchar(32)", "YES", "", nil, "", i64Ptr(32)},
				))

			mock.ExpectQuery("SELECT COLUMN_NAME").
				WithArgs("testdb", "shared").
				WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id"))

			p := NewSQLProvider(db, "testdb")
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
	var _ Provider = NewSQLProvider(db, "testdb")
}
