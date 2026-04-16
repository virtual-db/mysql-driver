package schema_test

import (
	"strings"
	"testing"

	gmssql "github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	. "github.com/AnqorDX/vdb-mysql-driver/internal/schema"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// strPtr and i64Ptr are declared in provider_test.go (same package).
// They are not redeclared here.

func makeCol(name, dataType string) ColumnDescriptor {
	return ColumnDescriptor{
		Name:       name,
		DataType:   dataType,
		ColumnType: dataType,
		IsNullable: "YES",
	}
}

func makeVarchar(name string, length int64) ColumnDescriptor {
	return ColumnDescriptor{
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
	cols := []ColumnDescriptor{
		makeCol("id", "int"),
		makeCol("name", "varchar"),
		makeCol("created_at", "datetime"),
	}
	s := ToGMSSchema("orders", cols)
	if len(s) != len(cols) {
		t.Fatalf("schema length: got %d, want %d", len(s), len(cols))
	}
}

func TestToGMSSchema_ColumnNamesPreserved(t *testing.T) {
	cols := []ColumnDescriptor{
		makeCol("sku", "varchar"),
		makeCol("price", "int"),
		makeCol("stock", "int"),
	}
	s := ToGMSSchema("products", cols)
	for i, col := range s {
		if col.Name != cols[i].Name {
			t.Errorf("schema[%d].Name: got %q, want %q", i, col.Name, cols[i].Name)
		}
	}
}

func TestToGMSSchema_SourceSetToTableName(t *testing.T) {
	cols := []ColumnDescriptor{
		makeCol("id", "int"),
		makeCol("email", "varchar"),
	}
	s := ToGMSSchema("users", cols)
	for i, col := range s {
		if col.Source != "users" {
			t.Errorf("schema[%d].Source: got %q, want %q", i, col.Source, "users")
		}
	}
}

func TestToGMSSchema_EmptyColumns_ReturnsEmptySchema(t *testing.T) {
	s := ToGMSSchema("empty_table", nil)
	if s == nil {
		t.Fatal("ToGMSSchema returned nil for empty column list")
	}
	if len(s) != 0 {
		t.Errorf("schema length: got %d, want 0", len(s))
	}
}

func TestToGMSSchema_TypeIsNonNil(t *testing.T) {
	cols := []ColumnDescriptor{
		makeCol("id", "int"),
		makeCol("balance", "decimal"),
		makeCol("name", "varchar"),
	}
	s := ToGMSSchema("accounts", cols)
	for i, col := range s {
		if col.Type == nil {
			t.Errorf("schema[%d] (%q): Type is nil", i, col.Name)
		}
	}
}

// ---------------------------------------------------------------------------
// ToGMSSchema — nullability
// ---------------------------------------------------------------------------

func TestToGMSSchema_NotNull_IsNullableFalse(t *testing.T) {
	col := ColumnDescriptor{
		Name:       "username",
		DataType:   "varchar",
		IsNullable: "NO",
	}
	s := ToGMSSchema("users", []ColumnDescriptor{col})
	if s[0].Nullable {
		t.Errorf("expected Nullable=false for IS_NULLABLE=NO, got true")
	}
}

func TestToGMSSchema_Nullable_IsNullableTrue(t *testing.T) {
	col := ColumnDescriptor{
		Name:       "bio",
		DataType:   "text",
		IsNullable: "YES",
	}
	s := ToGMSSchema("users", []ColumnDescriptor{col})
	if !s[0].Nullable {
		t.Errorf("expected Nullable=true for IS_NULLABLE=YES, got false")
	}
}

func TestToGMSSchema_MixedNullability(t *testing.T) {
	cols := []ColumnDescriptor{
		{Name: "id", DataType: "int", IsNullable: "NO"},
		{Name: "bio", DataType: "text", IsNullable: "YES"},
	}
	s := ToGMSSchema("profiles", cols)
	if s[0].Nullable {
		t.Errorf("schema[0] (id): expected Nullable=false")
	}
	if !s[1].Nullable {
		t.Errorf("schema[1] (bio): expected Nullable=true")
	}
}

// ---------------------------------------------------------------------------
// ToGMSSchema — primary key and auto-increment
// ---------------------------------------------------------------------------

func TestToGMSSchema_PrimaryKey_SetsPrimaryKeyTrue(t *testing.T) {
	col := ColumnDescriptor{
		Name:       "id",
		DataType:   "int",
		IsNullable: "NO",
		ColumnKey:  "PRI",
		Extra:      "auto_increment",
	}
	s := ToGMSSchema("users", []ColumnDescriptor{col})
	if !s[0].PrimaryKey {
		t.Errorf("expected PrimaryKey=true for ColumnKey=PRI, got false")
	}
	if !s[0].AutoIncrement {
		t.Errorf("expected AutoIncrement=true for Extra=auto_increment, got false")
	}
}

func TestToGMSSchema_NonPK_PrimaryKeyFalse(t *testing.T) {
	col := ColumnDescriptor{
		Name:       "username",
		DataType:   "varchar",
		IsNullable: "NO",
		ColumnKey:  "",
	}
	s := ToGMSSchema("users", []ColumnDescriptor{col})
	if s[0].PrimaryKey {
		t.Errorf("expected PrimaryKey=false for non-PK column, got true")
	}
	if s[0].AutoIncrement {
		t.Errorf("expected AutoIncrement=false for non-auto_increment column, got true")
	}
}

// ---------------------------------------------------------------------------
// ToGMSSchema — default values
// ---------------------------------------------------------------------------

func TestToGMSSchema_NoDefault_IsNil(t *testing.T) {
	col := ColumnDescriptor{
		Name:          "status",
		DataType:      "varchar",
		IsNullable:    "YES",
		ColumnDefault: nil,
	}
	s := ToGMSSchema("users", []ColumnDescriptor{col})
	if s[0].Default != nil {
		t.Errorf("expected Default=nil when ColumnDefault is nil, got %v", s[0].Default)
	}
}

func TestToGMSSchema_StringDefault_IsSet(t *testing.T) {
	col := ColumnDescriptor{
		Name:          "status",
		DataType:      "varchar",
		IsNullable:    "YES",
		ColumnDefault: strPtr("active"),
	}
	s := ToGMSSchema("users", []ColumnDescriptor{col})
	if s[0].Default == nil {
		t.Errorf("expected non-nil Default for ColumnDefault='active'")
	}
}

func TestToGMSSchema_IntDefault_IsSet(t *testing.T) {
	col := ColumnDescriptor{
		Name:          "quantity",
		DataType:      "int",
		IsNullable:    "NO",
		ColumnDefault: strPtr("1"),
	}
	s := ToGMSSchema("order_items", []ColumnDescriptor{col})
	if s[0].Default == nil {
		t.Errorf("expected non-nil Default for integer column with DEFAULT 1")
	}
}

// ---------------------------------------------------------------------------
// columnTypeToGMS — integer family
// The critical mapping for BUG-001: INT columns must not be typed as LongText.
// When typed as LongText, the GMS sort comparator uses string comparison and
// sorts "1000" after "800" (lexicographic order), breaking ORDER BY on JOINs.
// ---------------------------------------------------------------------------

func TestColumnTypeToGMS_Int_ReturnsInt32(t *testing.T) {
	col := makeCol("total_cents", "int")
	s := ToGMSSchema("orders", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Int32 {
		t.Errorf("INT column: expected gmstypes.Int32, got %T / %v", s[0].Type, s[0].Type)
	}
}

func TestColumnTypeToGMS_Integer_ReturnsInt32(t *testing.T) {
	col := makeCol("count", "integer")
	s := ToGMSSchema("stats", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Int32 {
		t.Errorf("INTEGER column: expected gmstypes.Int32, got %T / %v", s[0].Type, s[0].Type)
	}
}

func TestColumnTypeToGMS_TinyInt_ReturnsInt8(t *testing.T) {
	col := makeCol("active", "tinyint")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Int8 {
		t.Errorf("TINYINT column: expected gmstypes.Int8, got %T / %v", s[0].Type, s[0].Type)
	}
}

func TestColumnTypeToGMS_SmallInt_ReturnsInt16(t *testing.T) {
	col := makeCol("rank", "smallint")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Int16 {
		t.Errorf("SMALLINT column: expected gmstypes.Int16, got %T / %v", s[0].Type, s[0].Type)
	}
}

func TestColumnTypeToGMS_MediumInt_ReturnsInt32(t *testing.T) {
	col := makeCol("score", "mediumint")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Int32 {
		t.Errorf("MEDIUMINT column: expected gmstypes.Int32, got %T / %v", s[0].Type, s[0].Type)
	}
}

func TestColumnTypeToGMS_BigInt_ReturnsInt64(t *testing.T) {
	col := makeCol("population", "bigint")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Int64 {
		t.Errorf("BIGINT column: expected gmstypes.Int64, got %T / %v", s[0].Type, s[0].Type)
	}
}

// ---------------------------------------------------------------------------
// columnTypeToGMS — floating-point family
// ---------------------------------------------------------------------------

func TestColumnTypeToGMS_Float_ReturnsFloat32(t *testing.T) {
	col := makeCol("ratio", "float")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Float32 {
		t.Errorf("FLOAT column: expected gmstypes.Float32, got %T / %v", s[0].Type, s[0].Type)
	}
}

func TestColumnTypeToGMS_Double_ReturnsFloat64(t *testing.T) {
	col := makeCol("weight", "double")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Float64 {
		t.Errorf("DOUBLE column: expected gmstypes.Float64, got %T / %v", s[0].Type, s[0].Type)
	}
}

// ---------------------------------------------------------------------------
// columnTypeToGMS — varchar / char
// ---------------------------------------------------------------------------

func TestColumnTypeToGMS_Varchar_ReturnsStringType(t *testing.T) {
	col := ColumnDescriptor{
		Name:          "username",
		DataType:      "varchar",
		ColumnType:    "varchar(64)",
		IsNullable:    "NO",
		CharMaxLength: i64Ptr(64),
	}
	s := ToGMSSchema("users", []ColumnDescriptor{col})
	got := s[0].Type.String()
	// GMS String() returns "varchar(64)" or "varchar(64) COLLATE …" depending
	// on the GMS version. Assert the prefix so the test is version-stable.
	if !strings.HasPrefix(got, "varchar(64)") {
		t.Errorf("VARCHAR(64) column type String(): got %q, want prefix %q", got, "varchar(64)")
	}
}

func TestColumnTypeToGMS_Varchar_DifferentLengths(t *testing.T) {
	cases := []struct {
		length     int64
		wantPrefix string
	}{
		{16, "varchar(16)"},
		{128, "varchar(128)"},
		{255, "varchar(255)"},
	}
	for _, tc := range cases {
		col := ColumnDescriptor{
			Name:          "field",
			DataType:      "varchar",
			CharMaxLength: i64Ptr(tc.length),
		}
		s := ToGMSSchema("t", []ColumnDescriptor{col})
		got := s[0].Type.String()
		// GMS may append " COLLATE …"; check the prefix is correct.
		if !strings.HasPrefix(got, tc.wantPrefix) {
			t.Errorf("VARCHAR(%d): got type %q, want prefix %q", tc.length, got, tc.wantPrefix)
		}
	}
}

func TestColumnTypeToGMS_Varchar_NotLongText(t *testing.T) {
	// This is the core assertion for BUG-002: VARCHAR columns must not be
	// reported as longtext.
	col := ColumnDescriptor{
		Name:          "email",
		DataType:      "varchar",
		CharMaxLength: i64Ptr(128),
	}
	s := ToGMSSchema("users", []ColumnDescriptor{col})
	if s[0].Type == gmstypes.LongText {
		t.Errorf("VARCHAR column must not map to gmstypes.LongText")
	}
	got := s[0].Type.String()
	if strings.HasPrefix(got, "longtext") {
		t.Errorf("VARCHAR(128) type String(): got %q, must not start with \"longtext\"", got)
	}
	if !strings.HasPrefix(got, "varchar") {
		t.Errorf("VARCHAR(128) type String(): got %q, want prefix \"varchar\"", got)
	}
}

// ---------------------------------------------------------------------------
// columnTypeToGMS — text family
// ---------------------------------------------------------------------------

func TestColumnTypeToGMS_Text_ReturnsText(t *testing.T) {
	col := makeCol("body", "text")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Text {
		t.Errorf("TEXT column: expected gmstypes.Text, got %T / %v", s[0].Type, s[0].Type)
	}
}

func TestColumnTypeToGMS_LongText_ReturnsLongText(t *testing.T) {
	col := makeCol("content", "longtext")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.LongText {
		t.Errorf("LONGTEXT column: expected gmstypes.LongText, got %T / %v", s[0].Type, s[0].Type)
	}
}

// ---------------------------------------------------------------------------
// columnTypeToGMS — date / time family
// ---------------------------------------------------------------------------

func TestColumnTypeToGMS_Date_ReturnsDate(t *testing.T) {
	col := makeCol("dob", "date")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Date {
		t.Errorf("DATE column: expected gmstypes.Date, got %T / %v", s[0].Type, s[0].Type)
	}
}

func TestColumnTypeToGMS_Datetime_ReturnDatetime(t *testing.T) {
	col := makeCol("created_at", "datetime")
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type != gmstypes.Datetime {
		t.Errorf("DATETIME column: expected gmstypes.Datetime, got %T / %v", s[0].Type, s[0].Type)
	}
}

// ---------------------------------------------------------------------------
// columnTypeToGMS — unknown / fallback
// ---------------------------------------------------------------------------

func TestColumnTypeToGMS_UnknownType_FallsBackToLongText(t *testing.T) {
	col := makeCol("x", "geometry") // not in the switch
	s := ToGMSSchema("t", []ColumnDescriptor{col})
	if s[0].Type == nil {
		t.Fatal("expected non-nil type for unknown DataType, got nil")
	}
	// Must fall back gracefully (LongText) rather than panic.
	if s[0].Type != gmstypes.LongText {
		t.Errorf("unknown type: expected LongText fallback, got %T / %v", s[0].Type, s[0].Type)
	}
}

// ---------------------------------------------------------------------------
// INT sort comparator — the root cause of BUG-001
//
// This group directly validates that the GMS type produced for an INT column
// orders 1000 above 800 when comparing in descending order. Before the fix,
// all columns were typed as LongText, whose string comparator treats "1000"
// as less than "800" (lexicographic: "1" < "8"), causing ORDER BY total_cents
// DESC to place 1000 last instead of first in JOIN results.
// ---------------------------------------------------------------------------

func TestInt32Type_ComparesNumerically_1000_GreaterThan_800(t *testing.T) {
	// The GMS type returned for an INT column must compare 1000 > 800 so that
	// ORDER BY col DESC puts 1000 first.
	col := makeCol("total_cents", "int")
	s := ToGMSSchema("orders", []ColumnDescriptor{col})
	gmsType := s[0].Type

	ctx := gmssql.NewEmptyContext()
	cmp, err := gmsType.Compare(ctx, int32(1000), int32(800))
	if err != nil {
		t.Fatalf("Compare(1000, 800) returned error: %v", err)
	}
	if cmp <= 0 {
		t.Errorf("INT Compare(1000, 800): got %d, want > 0 (1000 must be greater than 800)", cmp)
	}
}

func TestInt32Type_ComparesNumerically_800_LessThan_1000(t *testing.T) {
	col := makeCol("total_cents", "int")
	s := ToGMSSchema("orders", []ColumnDescriptor{col})
	gmsType := s[0].Type

	ctx := gmssql.NewEmptyContext()
	cmp, err := gmsType.Compare(ctx, int32(800), int32(1000))
	if err != nil {
		t.Fatalf("Compare(800, 1000) returned error: %v", err)
	}
	if cmp >= 0 {
		t.Errorf("INT Compare(800, 1000): got %d, want < 0 (800 must be less than 1000)", cmp)
	}
}

func TestLongText_ComparesTreats1000LessThan800_ConfirmsOldBug(t *testing.T) {
	// Negative control: this test documents the old (broken) behaviour.
	// LongText compares values as strings: "1000" < "800" lexicographically.
	// If this test starts failing (LongText no longer sorts this way), update
	// or remove it — it is purely documentary.
	ctx := gmssql.NewEmptyContext()
	cmp, err := gmstypes.LongText.Compare(ctx, "1000", "800")
	if err != nil {
		t.Fatalf("LongText.Compare error: %v", err)
	}
	if cmp >= 0 {
		t.Logf("NOTE: LongText.Compare(\"1000\",\"800\") = %d (>= 0). "+
			"If GMS changed its LongText comparator this test should be removed.", cmp)
	}
	// We just document; no t.Errorf here. The real protection is the Int32
	// test above.
}
