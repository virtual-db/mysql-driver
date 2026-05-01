package catalog_test

import (
	"testing"

	intschema "github.com/virtual-db/vdb-mysql-driver/internal/schema"

	"github.com/virtual-db/vdb-mysql-driver/internal/catalog"
)

func TestToGMSSchema_NotNull_IsNullableFalse(t *testing.T) {
	col := intschema.ColumnDescriptor{
		Name:       "username",
		DataType:   "varchar",
		IsNullable: "NO",
	}
	s := catalog.ToGMSSchema("users", []intschema.ColumnDescriptor{col})
	if s[0].Nullable {
		t.Errorf("expected Nullable=false for IS_NULLABLE=NO, got true")
	}
}

func TestToGMSSchema_Nullable_IsNullableTrue(t *testing.T) {
	col := intschema.ColumnDescriptor{
		Name:       "bio",
		DataType:   "text",
		IsNullable: "YES",
	}
	s := catalog.ToGMSSchema("users", []intschema.ColumnDescriptor{col})
	if !s[0].Nullable {
		t.Errorf("expected Nullable=true for IS_NULLABLE=YES, got false")
	}
}

func TestToGMSSchema_MixedNullability(t *testing.T) {
	cols := []intschema.ColumnDescriptor{
		{Name: "id", DataType: "int", IsNullable: "NO"},
		{Name: "bio", DataType: "text", IsNullable: "YES"},
	}
	s := catalog.ToGMSSchema("profiles", cols)
	if s[0].Nullable {
		t.Errorf("schema[0] (id): expected Nullable=false")
	}
	if !s[1].Nullable {
		t.Errorf("schema[1] (bio): expected Nullable=true")
	}
}
