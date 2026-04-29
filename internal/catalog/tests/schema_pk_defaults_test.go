package catalog_test

import (
	"testing"

	"github.com/virtual-db/mysql-driver/internal/catalog"
	intschema "github.com/virtual-db/mysql-driver/internal/schema"
)

func TestToGMSSchema_PrimaryKey_SetsPrimaryKeyTrue(t *testing.T) {
	col := intschema.ColumnDescriptor{
		Name:       "id",
		DataType:   "int",
		IsNullable: "NO",
		ColumnKey:  "PRI",
		Extra:      "auto_increment",
	}
	s := catalog.ToGMSSchema("users", []intschema.ColumnDescriptor{col})
	if !s[0].PrimaryKey {
		t.Errorf("expected PrimaryKey=true for ColumnKey=PRI, got false")
	}
	if !s[0].AutoIncrement {
		t.Errorf("expected AutoIncrement=true for Extra=auto_increment, got false")
	}
}

func TestToGMSSchema_NonPK_PrimaryKeyFalse(t *testing.T) {
	col := intschema.ColumnDescriptor{
		Name:       "username",
		DataType:   "varchar",
		IsNullable: "NO",
		ColumnKey:  "",
	}
	s := catalog.ToGMSSchema("users", []intschema.ColumnDescriptor{col})
	if s[0].PrimaryKey {
		t.Errorf("expected PrimaryKey=false for non-PK column, got true")
	}
	if s[0].AutoIncrement {
		t.Errorf("expected AutoIncrement=false for non-auto_increment column, got true")
	}
}

func TestToGMSSchema_NoDefault_IsNil(t *testing.T) {
	col := intschema.ColumnDescriptor{
		Name:          "status",
		DataType:      "varchar",
		IsNullable:    "YES",
		ColumnDefault: nil,
	}
	s := catalog.ToGMSSchema("users", []intschema.ColumnDescriptor{col})
	if s[0].Default != nil {
		t.Errorf("expected Default=nil when ColumnDefault is nil, got %v", s[0].Default)
	}
}

func TestToGMSSchema_StringDefault_IsSet(t *testing.T) {
	v := "active"
	col := intschema.ColumnDescriptor{
		Name:          "status",
		DataType:      "varchar",
		IsNullable:    "YES",
		ColumnDefault: &v,
	}
	s := catalog.ToGMSSchema("users", []intschema.ColumnDescriptor{col})
	if s[0].Default == nil {
		t.Errorf("expected non-nil Default for ColumnDefault='active'")
	}
}

func TestToGMSSchema_IntDefault_IsSet(t *testing.T) {
	v := "1"
	col := intschema.ColumnDescriptor{
		Name:          "quantity",
		DataType:      "int",
		IsNullable:    "NO",
		ColumnDefault: &v,
	}
	s := catalog.ToGMSSchema("order_items", []intschema.ColumnDescriptor{col})
	if s[0].Default == nil {
		t.Errorf("expected non-nil Default for integer column with DEFAULT 1")
	}
}
