package schema_test

import (
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	. "github.com/virtual-db/mysql-driver/internal/schema"
)

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
