package driver

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// wrappedSchemaProvider
// ---------------------------------------------------------------------------

func TestWrappedSchemaProvider_OnSuccess_CallsOnLoad(t *testing.T) {
	var called bool
	var gotTable string
	var gotCols []string
	var gotPK string

	inner := &stubSchemaProvider{
		cols:  []string{"id", "name", "email"},
		pkCol: "id",
	}
	w := &wrappedSchemaProvider{
		inner: inner,
		onLoad: func(table string, cols []string, pkCol string) {
			called = true
			gotTable = table
			gotCols = cols
			gotPK = pkCol
		},
	}

	cols, pk, err := w.GetSchema("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("onLoad was not called on a successful GetSchema")
	}
	if gotTable != "users" {
		t.Errorf("onLoad table: got %q, want %q", gotTable, "users")
	}
	wantCols := []string{"id", "name", "email"}
	if len(gotCols) != len(wantCols) {
		t.Fatalf("onLoad cols length: got %d, want %d", len(gotCols), len(wantCols))
	}
	for i, c := range wantCols {
		if gotCols[i] != c {
			t.Errorf("onLoad cols[%d]: got %q, want %q", i, gotCols[i], c)
		}
	}
	if gotPK != "id" {
		t.Errorf("onLoad pkCol: got %q, want %q", gotPK, "id")
	}
	if len(cols) != len(wantCols) {
		t.Fatalf("caller cols length: got %d, want %d", len(cols), len(wantCols))
	}
	if pk != "id" {
		t.Errorf("caller pk: got %q, want %q", pk, "id")
	}
}

func TestWrappedSchemaProvider_OnError_SuppressesOnLoad(t *testing.T) {
	innerErr := errors.New("table not found in source database")
	inner := &stubSchemaProvider{err: innerErr}

	var called bool
	w := &wrappedSchemaProvider{
		inner:  inner,
		onLoad: func(_ string, _ []string, _ string) { called = true },
	}

	_, _, err := w.GetSchema("missing_table")
	if err == nil {
		t.Fatal("expected non-nil error, got nil")
	}
	if !errors.Is(err, innerErr) {
		t.Errorf("error chain: got %v, expected it to wrap %v", err, innerErr)
	}
	if called {
		t.Fatal("onLoad was called despite an error from the inner provider")
	}
}

func TestWrappedSchemaProvider_NilOnLoad_DoesNotPanic(t *testing.T) {
	inner := &stubSchemaProvider{cols: []string{"id"}, pkCol: "id"}
	w := &wrappedSchemaProvider{inner: inner, onLoad: nil}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("wrappedSchemaProvider panicked with nil onLoad: %v", r)
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
		cols:  []string{"sku", "price", "stock"},
		pkCol: "sku",
	}
	w := &wrappedSchemaProvider{
		inner:  inner,
		onLoad: func(_ string, _ []string, _ string) {},
	}

	cols, pk, err := w.GetSchema("products")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantCols := []string{"sku", "price", "stock"}
	if len(cols) != len(wantCols) {
		t.Fatalf("cols length: got %d, want %d", len(cols), len(wantCols))
	}
	for i, c := range wantCols {
		if cols[i] != c {
			t.Errorf("cols[%d]: got %q, want %q", i, cols[i], c)
		}
	}
	if pk != "sku" {
		t.Errorf("pk: got %q, want %q", pk, "sku")
	}
}

func TestWrappedSchemaProvider_NoPrimaryKey_PassedThrough(t *testing.T) {
	inner := &stubSchemaProvider{
		cols:  []string{"event_id", "message"},
		pkCol: "",
	}
	var gotPK = "SENTINEL"
	w := &wrappedSchemaProvider{
		inner: inner,
		onLoad: func(_ string, _ []string, pkCol string) {
			gotPK = pkCol
		},
	}

	_, pk, err := w.GetSchema("log_entries")
	if err != nil {
		t.Fatalf("unexpected error for table with no pk: %v", err)
	}
	if pk != "" {
		t.Errorf("caller pk: got %q, want empty string", pk)
	}
	if gotPK != "" {
		t.Errorf("onLoad pkCol: got %q, want empty string", gotPK)
	}
}

// ---------------------------------------------------------------------------
// Driver lifecycle
// ---------------------------------------------------------------------------

func TestRun_BeforeSetDriverAPI_ReturnsError(t *testing.T) {
	d := New(Config{
		Addr:      ":0",
		DBName:    "testdb",
		SourceDSN: "user:pass@tcp(127.0.0.1:3306)/testdb",
	})

	err := d.Run()
	if err == nil {
		t.Fatal("Run() before SetDriverAPI returned nil — expected a non-nil error")
	}
	if !strings.Contains(err.Error(), "SetDriverAPI") {
		t.Errorf("error message %q does not mention SetDriverAPI", err.Error())
	}
}

func TestStop_BeforeRun_ReturnsNil(t *testing.T) {
	d := New(Config{
		Addr:      ":0",
		DBName:    "testdb",
		SourceDSN: "user:pass@tcp(127.0.0.1:3306)/testdb",
	})

	if err := d.Stop(); err != nil {
		t.Fatalf("Stop() before Run returned a non-nil error: %v", err)
	}
}

func TestStop_BeforeRun_DoesNotPanic(t *testing.T) {
	d := New(Config{})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Stop() before Run panicked: %v", r)
		}
	}()

	_ = d.Stop()
}

// ---------------------------------------------------------------------------
// New — construction defaults
// ---------------------------------------------------------------------------

func TestNew_DefaultsConnTimeouts(t *testing.T) {
	d := New(Config{Addr: ":3306", DBName: "db", SourceDSN: "dsn"})

	if d.cfg.ConnReadTimeout == 0 {
		t.Error("ConnReadTimeout was not defaulted — got zero duration")
	}
	if d.cfg.ConnWriteTimeout == 0 {
		t.Error("ConnWriteTimeout was not defaulted — got zero duration")
	}
}

func TestNew_ExplicitTimeouts_NotOverridden(t *testing.T) {
	const want = 5 * time.Second
	d := New(Config{
		Addr:             ":3306",
		DBName:           "db",
		SourceDSN:        "dsn",
		ConnReadTimeout:  want,
		ConnWriteTimeout: want,
	})

	if d.cfg.ConnReadTimeout != want {
		t.Errorf("ConnReadTimeout: got %v, want %v", d.cfg.ConnReadTimeout, want)
	}
	if d.cfg.ConnWriteTimeout != want {
		t.Errorf("ConnWriteTimeout: got %v, want %v", d.cfg.ConnWriteTimeout, want)
	}
}

func TestNew_GMSNilUntilSetDriverAPI(t *testing.T) {
	d := New(Config{Addr: ":3306", DBName: "db", SourceDSN: "dsn"})
	if d.gms != nil {
		t.Error("gms should be nil before SetDriverAPI is called")
	}
}

func TestNew_SrvNilUntilRun(t *testing.T) {
	d := New(Config{Addr: ":3306", DBName: "db", SourceDSN: "dsn"})
	if d.srv != nil {
		t.Error("srv should be nil before Run is called")
	}
}
