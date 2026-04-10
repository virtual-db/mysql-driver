package driver

import (
	core "github.com/AnqorDX/vdb-core"
	"github.com/AnqorDX/vdb-mysql-driver/internal/bridge"
	"github.com/AnqorDX/vdb-mysql-driver/internal/schema"
)

// ---------------------------------------------------------------------------
// stubSchemaProvider — satisfies schema.Provider for tests
// ---------------------------------------------------------------------------

// stubSchemaProvider satisfies schema.Provider for tests with configurable
// return values.
type stubSchemaProvider struct {
	cols  []string
	pkCol string
	err   error
}

func (s *stubSchemaProvider) GetSchema(_ string) ([]string, string, error) {
	return s.cols, s.pkCol, s.err
}

var _ schema.Provider = (*stubSchemaProvider)(nil)

// ---------------------------------------------------------------------------
// stubEventBridge — satisfies bridge.EventBridge for tests
// ---------------------------------------------------------------------------

// stubEventBridge implements bridge.EventBridge for tests. Each of the 14
// methods is backed by an optional function field; when a field is nil the
// method falls back to a safe no-op default (pass-through for record slices,
// nil for single-record results, nil for errors).
type stubEventBridge struct {
	connectionOpened      func(id uint32, user, addr string) error
	connectionClosed      func(id uint32, user, addr string)
	transactionBegun      func(connID uint32, readOnly bool) error
	transactionCommitted  func(connID uint32) error
	transactionRolledBack func(connID uint32, savepoint string)
	queryReceived         func(connID uint32, query, database string) (string, error)
	queryCompleted        func(connID uint32, query string, rowsAffected int64, err error)
	rowsFetched           func(connID uint32, table string, records []map[string]any) ([]map[string]any, error)
	rowsReady             func(connID uint32, table string, records []map[string]any) ([]map[string]any, error)
	rowInserted           func(connID uint32, table string, record map[string]any) (map[string]any, error)
	rowUpdated            func(connID uint32, table string, old, new map[string]any) (map[string]any, error)
	rowDeleted            func(connID uint32, table string, record map[string]any) error
	schemaLoaded          func(table string, cols []string, pkCol string)
	schemaInvalidated     func(table string)
}

var _ bridge.EventBridge = (*stubEventBridge)(nil)

func (s *stubEventBridge) ConnectionOpened(id uint32, user, addr string) error {
	if s.connectionOpened != nil {
		return s.connectionOpened(id, user, addr)
	}
	return nil
}

func (s *stubEventBridge) ConnectionClosed(id uint32, user, addr string) {
	if s.connectionClosed != nil {
		s.connectionClosed(id, user, addr)
	}
}

func (s *stubEventBridge) TransactionBegun(connID uint32, readOnly bool) error {
	if s.transactionBegun != nil {
		return s.transactionBegun(connID, readOnly)
	}
	return nil
}

func (s *stubEventBridge) TransactionCommitted(connID uint32) error {
	if s.transactionCommitted != nil {
		return s.transactionCommitted(connID)
	}
	return nil
}

func (s *stubEventBridge) TransactionRolledBack(connID uint32, savepoint string) {
	if s.transactionRolledBack != nil {
		s.transactionRolledBack(connID, savepoint)
	}
}

func (s *stubEventBridge) QueryReceived(connID uint32, query, database string) (string, error) {
	if s.queryReceived != nil {
		return s.queryReceived(connID, query, database)
	}
	return query, nil
}

func (s *stubEventBridge) QueryCompleted(connID uint32, query string, rowsAffected int64, err error) {
	if s.queryCompleted != nil {
		s.queryCompleted(connID, query, rowsAffected, err)
	}
}

// RowsFetched returns the records unchanged when the field is nil, preserving
// the semantics of the original nil-callback no-op guard.
func (s *stubEventBridge) RowsFetched(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
	if s.rowsFetched != nil {
		return s.rowsFetched(connID, table, records)
	}
	return records, nil
}

// RowsReady returns the records unchanged when the field is nil.
func (s *stubEventBridge) RowsReady(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
	if s.rowsReady != nil {
		return s.rowsReady(connID, table, records)
	}
	return records, nil
}

// RowInserted returns (nil, nil) when the field is nil, matching the original
// nil-callback behaviour in gmsRowProvider.
func (s *stubEventBridge) RowInserted(connID uint32, table string, record map[string]any) (map[string]any, error) {
	if s.rowInserted != nil {
		return s.rowInserted(connID, table, record)
	}
	return nil, nil
}

// RowUpdated returns (nil, nil) when the field is nil.
func (s *stubEventBridge) RowUpdated(connID uint32, table string, old, new map[string]any) (map[string]any, error) {
	if s.rowUpdated != nil {
		return s.rowUpdated(connID, table, old, new)
	}
	return nil, nil
}

// RowDeleted returns nil when the field is nil.
func (s *stubEventBridge) RowDeleted(connID uint32, table string, record map[string]any) error {
	if s.rowDeleted != nil {
		return s.rowDeleted(connID, table, record)
	}
	return nil
}

func (s *stubEventBridge) SchemaLoaded(table string, cols []string, pkCol string) {
	if s.schemaLoaded != nil {
		s.schemaLoaded(table, cols, pkCol)
	}
}

func (s *stubEventBridge) SchemaInvalidated(table string) {
	if s.schemaInvalidated != nil {
		s.schemaInvalidated(table)
	}
}

// fullBridge returns a *stubEventBridge with all fourteen fields populated with
// minimal no-op functions. Used by any test that needs a fully-wired bridge
// (e.g. to replace a single field and verify it is called).
func fullBridge() *stubEventBridge {
	return &stubEventBridge{
		connectionOpened:      func(_ uint32, _, _ string) error { return nil },
		connectionClosed:      func(_ uint32, _, _ string) {},
		transactionBegun:      func(_ uint32, _ bool) error { return nil },
		transactionCommitted:  func(_ uint32) error { return nil },
		transactionRolledBack: func(_ uint32, _ string) {},
		queryReceived:         func(_ uint32, q, _ string) (string, error) { return q, nil },
		queryCompleted:        func(_ uint32, _ string, _ int64, _ error) {},
		rowsFetched: func(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
			return r, nil
		},
		rowsReady: func(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
			return r, nil
		},
		rowInserted: func(_ uint32, _ string, r map[string]any) (map[string]any, error) {
			return r, nil
		},
		rowUpdated: func(_ uint32, _ string, _, n map[string]any) (map[string]any, error) {
			return n, nil
		},
		rowDeleted:        func(_ uint32, _ string, _ map[string]any) error { return nil },
		schemaLoaded:      func(_ string, _ []string, _ string) {},
		schemaInvalidated: func(_ string) {},
	}
}

// ---------------------------------------------------------------------------
// stubCoreAPI — satisfies core.DriverAPI for driver-level tests
// ---------------------------------------------------------------------------

// stubCoreAPI implements core.DriverAPI with all-no-op methods. It is used by
// driver-level tests that need to call NewDriver without a real framework
// instance (e.g. TestNew_DefaultsConnTimeouts).
type stubCoreAPI struct{}

var _ core.DriverAPI = (*stubCoreAPI)(nil)

func (s *stubCoreAPI) ConnectionOpened(_ uint32, _, _ string) error { return nil }
func (s *stubCoreAPI) ConnectionClosed(_ uint32, _, _ string)       {}

func (s *stubCoreAPI) TransactionBegun(_ uint32, _ bool) error  { return nil }
func (s *stubCoreAPI) TransactionCommitted(_ uint32) error      { return nil }
func (s *stubCoreAPI) TransactionRolledBack(_ uint32, _ string) {}

func (s *stubCoreAPI) QueryReceived(_ uint32, query, _ string) (string, error) { return query, nil }
func (s *stubCoreAPI) QueryCompleted(_ uint32, _ string, _ int64, _ error)     {}

func (s *stubCoreAPI) RecordsSource(_ uint32, _ string, records []map[string]any) ([]map[string]any, error) {
	return records, nil
}
func (s *stubCoreAPI) RecordsMerged(_ uint32, _ string, records []map[string]any) ([]map[string]any, error) {
	return records, nil
}
func (s *stubCoreAPI) RecordInserted(_ uint32, _ string, record map[string]any) (map[string]any, error) {
	return record, nil
}
func (s *stubCoreAPI) RecordUpdated(_ uint32, _ string, _, new map[string]any) (map[string]any, error) {
	return new, nil
}
func (s *stubCoreAPI) RecordDeleted(_ uint32, _ string, _ map[string]any) error { return nil }

func (s *stubCoreAPI) SchemaLoaded(_ string, _ []string, _ string) {}
func (s *stubCoreAPI) SchemaInvalidated(_ string)                  {}
