package schema_test

import (
	"errors"
	"testing"

	. "github.com/virtual-db/mysql-driver/internal/schema"
)

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
