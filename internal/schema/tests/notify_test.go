package schema_test

import (
	"errors"
	"testing"

	. "github.com/virtual-db/vdb-mysql-driver/internal/schema"
)

// ---------------------------------------------------------------------------
// Stubs
// ---------------------------------------------------------------------------

// stubSchemaProvider satisfies schema.Provider with configurable return values.
type stubSchemaProvider struct {
	cols  []ColumnDescriptor
	pkCol string
	err   error
}

func (s *stubSchemaProvider) GetSchema(_ string) ([]ColumnDescriptor, string, error) {
	return s.cols, s.pkCol, s.err
}

// stubLoadListener satisfies schema.LoadListener and records every call.
// Note: SchemaLoaded receives []string (column names only), not []ColumnDescriptor.
// The NotifyingProvider extracts names from the descriptors before notifying.
type stubLoadListener struct {
	called bool
	table  string
	cols   []string
	pkCol  string
}

func (l *stubLoadListener) SchemaLoaded(table string, cols []string, pkCol string) {
	l.called = true
	l.table = table
	l.cols = cols
	l.pkCol = pkCol
}

// makeDescriptors is a test helper that builds a []ColumnDescriptor from
// (name, dataType) pairs without requiring callers to fill every field.
func makeDescriptors(pairs ...string) []ColumnDescriptor {
	if len(pairs)%2 != 0 {
		panic("makeDescriptors: pairs must be even (name, dataType, ...)")
	}
	out := make([]ColumnDescriptor, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out[i/2] = ColumnDescriptor{
			Name:       pairs[i],
			DataType:   pairs[i+1],
			ColumnType: pairs[i+1],
			IsNullable: "YES",
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// NotifyingProvider
// ---------------------------------------------------------------------------

func TestWrappedSchemaProvider_OnSuccess_CallsOnLoad(t *testing.T) {
	inner := &stubSchemaProvider{
		cols:  makeDescriptors("id", "int", "name", "varchar", "email", "varchar"),
		pkCol: "id",
	}
	listener := &stubLoadListener{}
	w := NewNotifyingProvider(inner, listener)

	cols, pk, err := w.GetSchema("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Listener must have been called.
	if !listener.called {
		t.Fatal("SchemaLoaded was not called on a successful GetSchema")
	}
	if listener.table != "users" {
		t.Errorf("listener table: got %q, want %q", listener.table, "users")
	}

	// Listener receives plain column names, not descriptors.
	wantNames := []string{"id", "name", "email"}
	if len(listener.cols) != len(wantNames) {
		t.Fatalf("listener cols length: got %d, want %d", len(listener.cols), len(wantNames))
	}
	for i, name := range wantNames {
		if listener.cols[i] != name {
			t.Errorf("listener cols[%d]: got %q, want %q", i, listener.cols[i], name)
		}
	}
	if listener.pkCol != "id" {
		t.Errorf("listener pkCol: got %q, want %q", listener.pkCol, "id")
	}

	// Caller receives full descriptors.
	if len(cols) != len(wantNames) {
		t.Fatalf("caller cols length: got %d, want %d", len(cols), len(wantNames))
	}
	for i, name := range wantNames {
		if cols[i].Name != name {
			t.Errorf("caller cols[%d].Name: got %q, want %q", i, cols[i].Name, name)
		}
	}
	if pk != "id" {
		t.Errorf("caller pk: got %q, want %q", pk, "id")
	}
}

func TestWrappedSchemaProvider_OnError_SuppressesOnLoad(t *testing.T) {
	innerErr := errors.New("table not found in source database")
	inner := &stubSchemaProvider{err: innerErr}
	listener := &stubLoadListener{}
	w := NewNotifyingProvider(inner, listener)

	_, _, err := w.GetSchema("missing_table")
	if err == nil {
		t.Fatal("expected non-nil error, got nil")
	}
	if !errors.Is(err, innerErr) {
		t.Errorf("error chain: got %v, expected it to wrap %v", err, innerErr)
	}
	if listener.called {
		t.Fatal("SchemaLoaded was called despite an error from the inner provider")
	}
}

func TestWrappedSchemaProvider_NilOnLoad_DoesNotPanic(t *testing.T) {
	inner := &stubSchemaProvider{
		cols:  makeDescriptors("id", "int"),
		pkCol: "id",
	}
	w := NewNotifyingProvider(inner, nil)

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
		cols:  makeDescriptors("sku", "varchar", "price", "int", "stock", "int"),
		pkCol: "sku",
	}
	w := NewNotifyingProvider(inner, &stubLoadListener{})

	cols, pk, err := w.GetSchema("products")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantNames := []string{"sku", "price", "stock"}
	if len(cols) != len(wantNames) {
		t.Fatalf("cols length: got %d, want %d", len(cols), len(wantNames))
	}
	for i, name := range wantNames {
		if cols[i].Name != name {
			t.Errorf("cols[%d].Name: got %q, want %q", i, cols[i].Name, name)
		}
	}
	if pk != "sku" {
		t.Errorf("pk: got %q, want %q", pk, "sku")
	}
}

func TestWrappedSchemaProvider_NoPrimaryKey_PassedThrough(t *testing.T) {
	inner := &stubSchemaProvider{
		cols:  makeDescriptors("event_id", "bigint", "message", "text"),
		pkCol: "",
	}
	listener := &stubLoadListener{}
	w := NewNotifyingProvider(inner, listener)

	_, pk, err := w.GetSchema("log_entries")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pk != "" {
		t.Errorf("caller pk: got %q, want empty string", pk)
	}
	if listener.pkCol != "" {
		t.Errorf("listener pkCol: got %q, want empty string", listener.pkCol)
	}
}

func TestWrappedSchemaProvider_ListenerReceivesNamesNotDescriptors(t *testing.T) {
	// Explicitly verify that the listener's cols slice contains plain strings
	// (column names), not some stringified form of the descriptor structs.
	// This confirms that NotifyingProvider correctly extracts names before
	// calling SchemaLoaded, preserving the vdb-core DriverAPI.SchemaLoaded
	// signature of (table string, cols []string, pkCol string).
	inner := &stubSchemaProvider{
		cols:  makeDescriptors("alpha", "int", "beta", "varchar", "gamma", "text"),
		pkCol: "alpha",
	}
	listener := &stubLoadListener{}
	w := NewNotifyingProvider(inner, listener)

	if _, _, err := w.GetSchema("t"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"alpha", "beta", "gamma"}
	if len(listener.cols) != len(want) {
		t.Fatalf("listener.cols length: got %d, want %d", len(listener.cols), len(want))
	}
	for i, name := range want {
		if listener.cols[i] != name {
			t.Errorf("listener.cols[%d]: got %q, want %q", i, listener.cols[i], name)
		}
	}
}
