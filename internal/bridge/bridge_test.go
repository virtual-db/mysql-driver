package bridge_test

import (
	"testing"

	"github.com/AnqorDX/vdb-mysql-driver/internal/bridge"
)

// noopBridge implements bridge.EventBridge with all-no-op methods. Its sole
// purpose is the compile-time assertion below: if EventBridge gains a new
// method or changes a signature, this file will fail to compile and the error
// will be reported against internal/bridge rather than any consumer package.
type noopBridge struct{}

var _ bridge.EventBridge = (*noopBridge)(nil)

func (noopBridge) ConnectionOpened(_ uint32, _, _ string) error                                { return nil }
func (noopBridge) ConnectionClosed(_ uint32, _, _ string)                                      {}
func (noopBridge) TransactionBegun(_ uint32, _ bool) error                                     { return nil }
func (noopBridge) TransactionCommitted(_ uint32) error                                         { return nil }
func (noopBridge) TransactionRolledBack(_ uint32, _ string)                                    {}
func (noopBridge) QueryReceived(_ uint32, q, _ string) (string, error)                         { return q, nil }
func (noopBridge) QueryCompleted(_ uint32, _ string, _ int64, _ error)                         {}
func (noopBridge) RowsFetched(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
	return r, nil
}
func (noopBridge) RowsReady(_ uint32, _ string, r []map[string]any) ([]map[string]any, error) {
	return r, nil
}
func (noopBridge) RowInserted(_ uint32, _ string, r map[string]any) (map[string]any, error) {
	return r, nil
}
func (noopBridge) RowUpdated(_ uint32, _ string, _, n map[string]any) (map[string]any, error) {
	return n, nil
}
func (noopBridge) RowDeleted(_ uint32, _ string, _ map[string]any) error { return nil }
func (noopBridge) SchemaLoaded(_ string, _ []string, _ string)           {}
func (noopBridge) SchemaInvalidated(_ string)                            {}

// TestEventBridge_CompileTimeAssertionViaLocalNoOp verifies that noopBridge
// fully satisfies the bridge.EventBridge interface. The compile-time assertion
// above is the actual test; the function body is intentionally empty.
func TestEventBridge_CompileTimeAssertionViaLocalNoOp(t *testing.T) {}
