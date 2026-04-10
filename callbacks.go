package driver

import (
	"fmt"
	"reflect"
)

// callbacks is a flat struct of typed function fields, one per database
// lifecycle event that GMS can signal. SetDriverAPI populates every field by
// mapping it to the corresponding core.DriverAPI method call.
//
// All fields use map[string]any rather than types.Record to avoid importing
// vdb-core. types.Record is defined as `type Record = map[string]any` (a type
// alias), so the two are assignment-compatible without an explicit cast.
//
// A nil function field is a programming error. validateCallbacks panics if any
// field is nil so that the mistake surfaces at wiring time, not under load.
type callbacks struct {
	// connectionOpened is called once per authenticated connection, after the
	// MySQL handshake succeeds. Return a non-nil error to refuse the connection.
	connectionOpened func(id uint32, user, addr string) error

	// connectionClosed is called when a connection is torn down. The return
	// value is ignored; a close cannot be vetoed.
	connectionClosed func(id uint32, user, addr string)

	// transactionBegun is called at BEGIN / START TRANSACTION. Return a non-nil
	// error to refuse the transaction.
	transactionBegun func(connID uint32, readOnly bool) error

	// transactionCommitted is called at COMMIT. Return a non-nil error to leave
	// the transaction open.
	transactionCommitted func(connID uint32) error

	// transactionRolledBack is called at ROLLBACK or ROLLBACK TO SAVEPOINT.
	// savepoint is the empty string for a full rollback.
	transactionRolledBack func(connID uint32, savepoint string)

	// queryReceived is called for each incoming text query before parsing. The
	// handler may rewrite the query by returning a different string. Return a
	// non-nil error to reject the query.
	queryReceived func(connID uint32, query, database string) (string, error)

	// queryCompleted is called after each SQL command finishes executing and all
	// rows have been sent to the client.
	queryCompleted func(connID uint32, query string, rowsAffected int64, err error)

	// rowsFetched is called with the full row slice after rows have been read
	// from the source database. The handler may inspect, augment, filter, or
	// replace the slice. Return a non-nil error to fall back to the original
	// slice unchanged.
	rowsFetched func(connID uint32, table string, records []map[string]any) ([]map[string]any, error)

	// rowsReady is called with the final row slice after the delta overlay has
	// been applied. Return a non-nil error to fall back to the pre-call slice
	// unchanged.
	rowsReady func(connID uint32, table string, records []map[string]any) ([]map[string]any, error)

	// rowInserted is called for each row GMS has determined should be inserted.
	// The handler may transform the row. Return a non-nil error to abort.
	rowInserted func(connID uint32, table string, record map[string]any) (map[string]any, error)

	// rowUpdated is called for each row GMS has determined should be updated.
	// old is the row before the update; new is the intended replacement. Return
	// a non-nil error to abort.
	rowUpdated func(connID uint32, table string, old, new map[string]any) (map[string]any, error)

	// rowDeleted is called for each row GMS has determined should be deleted.
	// Return a non-nil error to abort.
	rowDeleted func(connID uint32, table string, record map[string]any) error
}

// validateCallbacks panics with a descriptive message for each nil callbacks
// field. Panicking at construction time (inside SetDriverAPI) rather than at
// first invocation surfaces wiring errors immediately, not under load.
func validateCallbacks(cbs callbacks) {
	type check struct {
		name string
		fn   any
	}
	checks := []check{
		{"connectionOpened", cbs.connectionOpened},
		{"connectionClosed", cbs.connectionClosed},
		{"transactionBegun", cbs.transactionBegun},
		{"transactionCommitted", cbs.transactionCommitted},
		{"transactionRolledBack", cbs.transactionRolledBack},
		{"queryReceived", cbs.queryReceived},
		{"queryCompleted", cbs.queryCompleted},
		{"rowsFetched", cbs.rowsFetched},
		{"rowsReady", cbs.rowsReady},
		{"rowInserted", cbs.rowInserted},
		{"rowUpdated", cbs.rowUpdated},
		{"rowDeleted", cbs.rowDeleted},
	}
	for _, c := range checks {
		if reflect.ValueOf(c.fn).IsNil() {
			panic(fmt.Sprintf("driver: callbacks.%s must not be nil", c.name))
		}
	}
}
