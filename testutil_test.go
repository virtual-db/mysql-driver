package driver

import (
	core "github.com/AnqorDX/vdb-core"
)

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
