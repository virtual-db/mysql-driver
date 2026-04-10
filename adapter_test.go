package driver

import (
	"errors"
	"testing"

	core "github.com/AnqorDX/vdb-core"
)

// ---------------------------------------------------------------------------
// recordingCoreAPI — satisfies core.DriverAPI and records method calls
// ---------------------------------------------------------------------------

// recordingCoreAPI implements core.DriverAPI. Each method records whether it
// was called and captures any arguments or return values needed by the tests.
type recordingCoreAPI struct {
	connectionOpenedCalled      bool
	connectionClosedCalled      bool
	transactionBegunCalled      bool
	transactionCommittedCalled  bool
	transactionRolledBackCalled bool
	queryReceivedCalled         bool
	queryCompletedCalled        bool
	recordsSourceCalled         bool
	recordsMergedCalled         bool
	recordInsertedCalled        bool
	recordUpdatedCalled         bool
	recordDeletedCalled         bool
	schemaLoadedCalled          bool
	schemaInvalidatedCalled     bool

	// Configurable return values for methods that return errors or records.
	connectionOpenedErr error
	transactionBegunErr error
	queryReceivedRewrite string
	queryReceivedErr     error
	recordsSourceErr     error
	recordsMergedErr     error
	recordInsertedErr    error
	recordUpdatedErr     error
	recordDeletedErr     error
}

var _ core.DriverAPI = (*recordingCoreAPI)(nil)

func (r *recordingCoreAPI) ConnectionOpened(_ uint32, _, _ string) error {
	r.connectionOpenedCalled = true
	return r.connectionOpenedErr
}

func (r *recordingCoreAPI) ConnectionClosed(_ uint32, _, _ string) {
	r.connectionClosedCalled = true
}

func (r *recordingCoreAPI) TransactionBegun(_ uint32, _ bool) error {
	r.transactionBegunCalled = true
	return r.transactionBegunErr
}

func (r *recordingCoreAPI) TransactionCommitted(_ uint32) error {
	r.transactionCommittedCalled = true
	return nil
}

func (r *recordingCoreAPI) TransactionRolledBack(_ uint32, _ string) {
	r.transactionRolledBackCalled = true
}

func (r *recordingCoreAPI) QueryReceived(_ uint32, query, _ string) (string, error) {
	r.queryReceivedCalled = true
	if r.queryReceivedErr != nil {
		return "", r.queryReceivedErr
	}
	if r.queryReceivedRewrite != "" {
		return r.queryReceivedRewrite, nil
	}
	return query, nil
}

func (r *recordingCoreAPI) QueryCompleted(_ uint32, _ string, _ int64, _ error) {
	r.queryCompletedCalled = true
}

func (r *recordingCoreAPI) RecordsSource(_ uint32, _ string, records []map[string]any) ([]map[string]any, error) {
	r.recordsSourceCalled = true
	return records, r.recordsSourceErr
}

func (r *recordingCoreAPI) RecordsMerged(_ uint32, _ string, records []map[string]any) ([]map[string]any, error) {
	r.recordsMergedCalled = true
	return records, r.recordsMergedErr
}

func (r *recordingCoreAPI) RecordInserted(_ uint32, _ string, record map[string]any) (map[string]any, error) {
	r.recordInsertedCalled = true
	return record, r.recordInsertedErr
}

func (r *recordingCoreAPI) RecordUpdated(_ uint32, _ string, _, new map[string]any) (map[string]any, error) {
	r.recordUpdatedCalled = true
	return new, r.recordUpdatedErr
}

func (r *recordingCoreAPI) RecordDeleted(_ uint32, _ string, _ map[string]any) error {
	r.recordDeletedCalled = true
	return r.recordDeletedErr
}

func (r *recordingCoreAPI) SchemaLoaded(_ string, _ []string, _ string) {
	r.schemaLoadedCalled = true
}

func (r *recordingCoreAPI) SchemaInvalidated(_ string) {
	r.schemaInvalidatedCalled = true
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newAdapter returns a fresh apiAdapter backed by the given core.DriverAPI.
func newAdapter(api core.DriverAPI) *apiAdapter {
	return &apiAdapter{api: api}
}

// ---------------------------------------------------------------------------
// apiAdapter delegation tests
// ---------------------------------------------------------------------------

func TestAPIAdapter_ConnectionOpened_DelegatesToCoreAPI(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	if err := a.ConnectionOpened(1, "user", "127.0.0.1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.connectionOpenedCalled {
		t.Fatal("RecordingCoreAPI.ConnectionOpened was not called")
	}
}

func TestAPIAdapter_ConnectionOpened_PropagatesError(t *testing.T) {
	want := errors.New("refused")
	rec := &recordingCoreAPI{connectionOpenedErr: want}
	a := newAdapter(rec)

	err := a.ConnectionOpened(1, "user", "127.0.0.1")
	if !errors.Is(err, want) {
		t.Errorf("expected %v, got %v", want, err)
	}
}

func TestAPIAdapter_ConnectionClosed_DelegatesToCoreAPI(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	a.ConnectionClosed(1, "user", "127.0.0.1")
	if !rec.connectionClosedCalled {
		t.Fatal("RecordingCoreAPI.ConnectionClosed was not called")
	}
}

func TestAPIAdapter_TransactionBegun_DelegatesToCoreAPI(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	if err := a.TransactionBegun(1, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.transactionBegunCalled {
		t.Fatal("RecordingCoreAPI.TransactionBegun was not called")
	}
}

func TestAPIAdapter_TransactionCommitted_DelegatesToCoreAPI(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	if err := a.TransactionCommitted(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.transactionCommittedCalled {
		t.Fatal("RecordingCoreAPI.TransactionCommitted was not called")
	}
}

func TestAPIAdapter_TransactionRolledBack_DelegatesToCoreAPI(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	a.TransactionRolledBack(1, "")
	if !rec.transactionRolledBackCalled {
		t.Fatal("RecordingCoreAPI.TransactionRolledBack was not called")
	}
}

func TestAPIAdapter_QueryReceived_DelegatesToCoreAPI(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	got, err := a.QueryReceived(1, "SELECT 1", "db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.queryReceivedCalled {
		t.Fatal("RecordingCoreAPI.QueryReceived was not called")
	}
	if got != "SELECT 1" {
		t.Errorf("rewritten query: got %q, want %q", got, "SELECT 1")
	}
}

func TestAPIAdapter_QueryCompleted_DelegatesToCoreAPI(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	a.QueryCompleted(1, "SELECT 1", 0, nil)
	if !rec.queryCompletedCalled {
		t.Fatal("RecordingCoreAPI.QueryCompleted was not called")
	}
}

// TestAPIAdapter_RowsFetched_MapsToRecordsSource verifies that
// bridge.EventBridge.RowsFetched delegates to core.DriverAPI.RecordsSource
// (not RecordsMerged or any other method).
func TestAPIAdapter_RowsFetched_MapsToRecordsSource(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	records := []map[string]any{{"id": 1}}
	got, err := a.RowsFetched(1, "orders", records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.recordsSourceCalled {
		t.Fatal("RecordingCoreAPI.RecordsSource was not called by RowsFetched")
	}
	if rec.recordsMergedCalled {
		t.Fatal("RecordingCoreAPI.RecordsMerged was unexpectedly called by RowsFetched")
	}
	if len(got) != 1 {
		t.Errorf("records: got %d, want 1", len(got))
	}
}

// TestAPIAdapter_RowsReady_MapsToRecordsMerged verifies that
// bridge.EventBridge.RowsReady delegates to core.DriverAPI.RecordsMerged.
func TestAPIAdapter_RowsReady_MapsToRecordsMerged(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	records := []map[string]any{{"id": 2}}
	got, err := a.RowsReady(1, "orders", records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.recordsMergedCalled {
		t.Fatal("RecordingCoreAPI.RecordsMerged was not called by RowsReady")
	}
	if rec.recordsSourceCalled {
		t.Fatal("RecordingCoreAPI.RecordsSource was unexpectedly called by RowsReady")
	}
	if len(got) != 1 {
		t.Errorf("records: got %d, want 1", len(got))
	}
}

func TestAPIAdapter_RowInserted_MapsToRecordInserted(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	record := map[string]any{"id": 3}
	got, err := a.RowInserted(1, "orders", record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.recordInsertedCalled {
		t.Fatal("RecordingCoreAPI.RecordInserted was not called by RowInserted")
	}
	if got["id"] != 3 {
		t.Errorf("record id: got %v, want 3", got["id"])
	}
}

func TestAPIAdapter_RowUpdated_MapsToRecordUpdated(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	old := map[string]any{"id": 1, "name": "old"}
	new := map[string]any{"id": 1, "name": "new"}
	got, err := a.RowUpdated(1, "orders", old, new)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.recordUpdatedCalled {
		t.Fatal("RecordingCoreAPI.RecordUpdated was not called by RowUpdated")
	}
	if got["name"] != "new" {
		t.Errorf("record name: got %v, want new", got["name"])
	}
}

func TestAPIAdapter_RowDeleted_MapsToRecordDeleted(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	if err := a.RowDeleted(1, "orders", map[string]any{"id": 4}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.recordDeletedCalled {
		t.Fatal("RecordingCoreAPI.RecordDeleted was not called by RowDeleted")
	}
}

func TestAPIAdapter_SchemaLoaded_DelegatesToCoreAPI(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	a.SchemaLoaded("users", []string{"id", "name"}, "id")
	if !rec.schemaLoadedCalled {
		t.Fatal("RecordingCoreAPI.SchemaLoaded was not called")
	}
}

func TestAPIAdapter_SchemaInvalidated_DelegatesToCoreAPI(t *testing.T) {
	rec := &recordingCoreAPI{}
	a := newAdapter(rec)

	a.SchemaInvalidated("users")
	if !rec.schemaInvalidatedCalled {
		t.Fatal("RecordingCoreAPI.SchemaInvalidated was not called")
	}
}
