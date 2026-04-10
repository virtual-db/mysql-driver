package engine_test

import (
	"errors"
	"testing"

	"github.com/AnqorDX/vdb-mysql-driver/internal/gms/engine"
	rowspkg "github.com/AnqorDX/vdb-mysql-driver/internal/gms/rows"
	schemapkg "github.com/AnqorDX/vdb-mysql-driver/internal/schema"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// ---------------------------------------------------------------------------
// Local stubs
// ---------------------------------------------------------------------------

// stubSchemaProvider satisfies schema.Provider for tests.
type stubSchemaProvider struct {
	cols  []string
	pkCol string
	err   error
}

func (s *stubSchemaProvider) GetSchema(_ string) ([]string, string, error) {
	return s.cols, s.pkCol, s.err
}

var _ schemapkg.Provider = (*stubSchemaProvider)(nil)

// stubRowsProvider satisfies rows.Provider for tests with no-op implementations.
type stubRowsProvider struct{}

func (s *stubRowsProvider) FetchRows(_ *gmssql.Context, _ string, _ []gmssql.Row, _ gmssql.Schema) ([]map[string]any, error) {
	return nil, nil
}

func (s *stubRowsProvider) CommitRows(_ *gmssql.Context, _ string, records []map[string]any) ([]map[string]any, error) {
	return records, nil
}

func (s *stubRowsProvider) InsertRow(_ *gmssql.Context, _ string, _ gmssql.Row, _ gmssql.Schema) (map[string]any, error) {
	return nil, nil
}

func (s *stubRowsProvider) UpdateRow(_ *gmssql.Context, _ string, _, _ gmssql.Row, _ gmssql.Schema) (map[string]any, error) {
	return nil, nil
}

func (s *stubRowsProvider) DeleteRow(_ *gmssql.Context, _ string, _ gmssql.Row, _ gmssql.Schema) error {
	return nil
}

var _ rowspkg.Provider = (*stubRowsProvider)(nil)

// ---------------------------------------------------------------------------
// DatabaseProvider
// ---------------------------------------------------------------------------

func TestNewDatabaseProvider_ReturnsNonNil(t *testing.T) {
	p := engine.NewDatabaseProvider("testdb", &stubRowsProvider{}, &stubSchemaProvider{}, nil)
	if p == nil {
		t.Fatal("NewDatabaseProvider returned nil")
	}
}

func TestDatabaseProvider_Database_KnownName_ReturnsDatabase(t *testing.T) {
	p := engine.NewDatabaseProvider("testdb", &stubRowsProvider{}, &stubSchemaProvider{}, nil)
	ctx := gmssql.NewEmptyContext()

	db, err := p.Database(ctx, "testdb")
	if err != nil {
		t.Fatalf("Database(%q) error: %v", "testdb", err)
	}
	if db == nil {
		t.Fatal("Database returned nil for known name")
	}
}

func TestDatabaseProvider_Database_UnknownName_ReturnsError(t *testing.T) {
	p := engine.NewDatabaseProvider("testdb", &stubRowsProvider{}, &stubSchemaProvider{}, nil)
	ctx := gmssql.NewEmptyContext()

	_, err := p.Database(ctx, "unknown")
	if err == nil {
		t.Fatal("expected non-nil error for unknown database name, got nil")
	}
}

func TestDatabaseProvider_AllDatabases_ContainsRegisteredDB(t *testing.T) {
	p := engine.NewDatabaseProvider("mydb", &stubRowsProvider{}, &stubSchemaProvider{}, nil)
	ctx := gmssql.NewEmptyContext()

	all := p.AllDatabases(ctx)
	if len(all) == 0 {
		t.Fatal("AllDatabases returned empty slice")
	}
	found := false
	for _, db := range all {
		if db.Name() == "mydb" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("AllDatabases does not contain %q; got %v", "mydb", dbNames(all))
	}
}

func TestDatabaseProvider_HasSetCollation_ReturnsFalse(t *testing.T) {
	p := engine.NewDatabaseProvider("testdb", &stubRowsProvider{}, &stubSchemaProvider{}, nil)
	// DatabaseProvider must NOT implement gmssql.CollatedDatabaseProvider —
	// it does not support SET COLLATION. A type assertion must return false.
	_, ok := any(p).(gmssql.CollatedDatabaseProvider)
	if ok {
		t.Error("DatabaseProvider unexpectedly implements gmssql.CollatedDatabaseProvider")
	}
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

func TestDatabase_Name_MatchesConstructorArg(t *testing.T) {
	p := engine.NewDatabaseProvider("widgets", &stubRowsProvider{}, &stubSchemaProvider{}, nil)
	ctx := gmssql.NewEmptyContext()

	db, err := p.Database(ctx, "widgets")
	if err != nil {
		t.Fatalf("Database error: %v", err)
	}
	if db.Name() != "widgets" {
		t.Errorf("Name: got %q, want %q", db.Name(), "widgets")
	}
}

func TestDatabase_GetTableInsensitive_KnownTable_ReturnsTable(t *testing.T) {
	schema := &stubSchemaProvider{cols: []string{"id", "name"}, pkCol: "id"}
	p := engine.NewDatabaseProvider("testdb", &stubRowsProvider{}, schema, nil)
	ctx := gmssql.NewEmptyContext()

	db, err := p.Database(ctx, "testdb")
	if err != nil {
		t.Fatalf("Database error: %v", err)
	}

	tbl, ok, err := db.GetTableInsensitive(ctx, "users")
	if err != nil {
		t.Fatalf("GetTableInsensitive error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for known table, got false")
	}
	if tbl == nil {
		t.Fatal("expected non-nil table for known table")
	}
}

func TestDatabase_GetTableInsensitive_UnknownTable_ReturnsNilNoError(t *testing.T) {
	// Schema provider returns an error for unknown tables; Database treats
	// that as "table does not exist" → (nil, false, nil).
	schema := &stubSchemaProvider{err: errTableNotFound}
	p := engine.NewDatabaseProvider("testdb", &stubRowsProvider{}, schema, nil)
	ctx := gmssql.NewEmptyContext()

	db, err := p.Database(ctx, "testdb")
	if err != nil {
		t.Fatalf("Database error: %v", err)
	}

	tbl, ok, err := db.GetTableInsensitive(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("GetTableInsensitive returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for unknown table, got true")
	}
	if tbl != nil {
		t.Errorf("expected nil table for unknown table, got %v", tbl)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

var errTableNotFound = errors.New("table not found")

func dbNames(dbs []gmssql.Database) []string {
	names := make([]string, len(dbs))
	for i, db := range dbs {
		names[i] = db.Name()
	}
	return names
}
