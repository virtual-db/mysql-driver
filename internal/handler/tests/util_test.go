package handler_test

import (
	"errors"
	"testing"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"

	handler "github.com/virtual-db/mysql-driver/internal/handler"
)

func TestCastError_NilReturnsNil(t *testing.T) {
	if got := handler.CastError(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestCastError_NonNilReturnsNonNil(t *testing.T) {
	if got := handler.CastError(errors.New("x")); got == nil {
		t.Fatal("expected non-nil error, got nil")
	}
}

func TestCastError_AlreadySQLError_PassedThrough(t *testing.T) {
	orig := &vitessmysql.SQLError{Num: 1234, Message: "test sql error"}
	got := handler.CastError(orig)
	if got == nil {
		t.Fatal("expected non-nil error")
	}
	sqlErr, ok := got.(*vitessmysql.SQLError)
	if !ok {
		t.Fatalf("expected *vitessmysql.SQLError, got %T", got)
	}
	if sqlErr.Num != orig.Num || sqlErr.Message != orig.Message {
		t.Fatalf("expected code=%d msg=%q, got code=%d msg=%q",
			orig.Num, orig.Message, sqlErr.Num, sqlErr.Message)
	}
}

func TestBindingsToExprs_EmptyBindings_ReturnsNil(t *testing.T) {
	exprs, err := handler.BindingsToExprs(map[string]*querypb.BindVariable{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exprs != nil {
		t.Fatalf("expected nil exprs, got %v", exprs)
	}
}

func TestBindingsToExprs_SingleIntBinding(t *testing.T) {
	bindings := map[string]*querypb.BindVariable{
		"v1": {
			Type:  querypb.Type_INT64,
			Value: []byte("42"),
		},
	}
	exprs, err := handler.BindingsToExprs(bindings)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exprs == nil {
		t.Fatal("expected non-nil exprs")
	}
	if _, ok := exprs["v1"]; !ok {
		t.Fatal("expected key 'v1' in exprs")
	}
}

func TestBindingsToExprs_SingleStringBinding(t *testing.T) {
	bindings := map[string]*querypb.BindVariable{
		"name": {
			Type:  querypb.Type_VARCHAR,
			Value: []byte("hello"),
		},
	}
	exprs, err := handler.BindingsToExprs(bindings)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exprs == nil {
		t.Fatal("expected non-nil exprs")
	}
	if _, ok := exprs["name"]; !ok {
		t.Fatal("expected key 'name' in exprs")
	}
}

func TestBindingsToExprs_MultipleBindings_AllKeysPresent(t *testing.T) {
	bindings := map[string]*querypb.BindVariable{
		"a": {Type: querypb.Type_INT64, Value: []byte("1")},
		"b": {Type: querypb.Type_INT64, Value: []byte("2")},
		"c": {Type: querypb.Type_VARCHAR, Value: []byte("three")},
	}
	exprs, err := handler.BindingsToExprs(bindings)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(exprs) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(exprs))
	}
	for _, key := range []string{"a", "b", "c"} {
		if _, ok := exprs[key]; !ok {
			t.Fatalf("expected key %q in exprs", key)
		}
	}
}
