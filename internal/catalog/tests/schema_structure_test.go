package catalog_test

import (
	"testing"

	"github.com/virtual-db/vdb-mysql-driver/internal/catalog"
	intschema "github.com/virtual-db/vdb-mysql-driver/internal/schema"
)

// ---------------------------------------------------------------------------
// Helpers shared across schema_* test files in this package.
// Each file is self-contained; these are reproduced where needed.
// ---------------------------------------------------------------------------

func strPtr(s string) *string { return &s }
func i64Ptr(n int64) *int64   { return &n }

func makeCol(name, dataType string) intschema.ColumnDescriptor {
	return intschema.ColumnDescriptor{
		Name:       name,
		DataType:   dataType,
		ColumnType: dataType,
		IsNullable: "YES",
	}
}

func makeVarchar(name string, length int64) intschema.ColumnDescriptor {
	return intschema.ColumnDescriptor{
		Name:          name,
		DataType:      "varchar",
		ColumnType:    "varchar(" + string(rune('0'+length)) + ")",
		IsNullable:    "YES",
		CharMaxLength: i64Ptr(length),
	}
}

// ---------------------------------------------------------------------------
// ToGMSSchema — structural shape
// ---------------------------------------------------------------------------

func TestToGMSSchema_LengthMatchesColumnCount(t *testing.T) {
	cols := []intschema.ColumnDescriptor{
		makeCol("id", "int"),
		makeCol("name", "varchar"),
		makeCol("created_at", "datetime"),
	}
	s := catalog.ToGMSSchema("orders", cols)
	if len(s) != len(cols) {
		t.Fatalf("schema length: got %d, want %d", len(s), len(cols))
	}
}

func TestToGMSSchema_ColumnNamesPreserved(t *testing.T) {
	cols := []intschema.ColumnDescriptor{
		makeCol("sku", "varchar"),
		makeCol("price", "int"),
		makeCol("stock", "int"),
	}
	s := catalog.ToGMSSchema("products", cols)
	for i, col := range s {
		if col.Name != cols[i].Name {
			t.Errorf("schema[%d].Name: got %q, want %q", i, col.Name, cols[i].Name)
		}
	}
}

func TestToGMSSchema_SourceSetToTableName(t *testing.T) {
	cols := []intschema.ColumnDescriptor{
		makeCol("id", "int"),
		makeCol("email", "varchar"),
	}
	s := catalog.ToGMSSchema("users", cols)
	for i, col := range s {
		if col.Source != "users" {
			t.Errorf("schema[%d].Source: got %q, want %q", i, col.Source, "users")
		}
	}
}

func TestToGMSSchema_EmptyColumns_ReturnsEmptySchema(t *testing.T) {
	s := catalog.ToGMSSchema("empty_table", nil)
	if s == nil {
		t.Fatal("ToGMSSchema returned nil for empty column list")
	}
	if len(s) != 0 {
		t.Errorf("schema length: got %d, want 0", len(s))
	}
}

func TestToGMSSchema_TypeIsNonNil(t *testing.T) {
	cols := []intschema.ColumnDescriptor{
		makeCol("id", "int"),
		makeCol("balance", "decimal"),
		makeCol("name", "varchar"),
	}
	s := catalog.ToGMSSchema("accounts", cols)
	for i, col := range s {
		if col.Type == nil {
			t.Errorf("schema[%d] (%q): Type is nil", i, col.Name)
		}
	}
}
