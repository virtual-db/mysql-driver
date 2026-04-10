package driver

import (
	"strings"
	"testing"
)

// TestValidateCallbacks_FullCallbacks_NoPanic verifies that a fully populated
// callbacks struct passes validation without panicking.
func TestValidateCallbacks_FullCallbacks_NoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("validateCallbacks panicked on full callbacks: %v", r)
		}
	}()
	validateCallbacks(fullCallbacks())
}

// TestValidateCallbacks_EmptyCallbacks_Panics verifies that a zero-value
// callbacks struct causes validateCallbacks to panic.
func TestValidateCallbacks_EmptyCallbacks_Panics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("validateCallbacks(empty callbacks) did not panic as expected")
		}
	}()
	validateCallbacks(callbacks{})
}

// TestValidateCallbacks_PanicMessageContainsFieldName verifies that the panic
// message from validateCallbacks identifies the nil field by name, so operators
// can immediately locate the mis-wired slot without reading source code.
func TestValidateCallbacks_PanicMessageContainsFieldName(t *testing.T) {
	cases := []struct {
		field  string
		mutate func(*callbacks)
	}{
		{"connectionOpened", func(c *callbacks) { c.connectionOpened = nil }},
		{"connectionClosed", func(c *callbacks) { c.connectionClosed = nil }},
		{"transactionBegun", func(c *callbacks) { c.transactionBegun = nil }},
		{"transactionCommitted", func(c *callbacks) { c.transactionCommitted = nil }},
		{"transactionRolledBack", func(c *callbacks) { c.transactionRolledBack = nil }},
		{"queryReceived", func(c *callbacks) { c.queryReceived = nil }},
		{"queryCompleted", func(c *callbacks) { c.queryCompleted = nil }},
		{"rowsFetched", func(c *callbacks) { c.rowsFetched = nil }},
		{"rowsReady", func(c *callbacks) { c.rowsReady = nil }},
		{"rowInserted", func(c *callbacks) { c.rowInserted = nil }},
		{"rowUpdated", func(c *callbacks) { c.rowUpdated = nil }},
		{"rowDeleted", func(c *callbacks) { c.rowDeleted = nil }},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.field, func(t *testing.T) {
			cbs := fullCallbacks()
			tc.mutate(&cbs)

			var panicVal any
			func() {
				defer func() { panicVal = recover() }()
				validateCallbacks(cbs)
			}()

			if panicVal == nil {
				t.Fatalf("validateCallbacks did not panic when %s is nil", tc.field)
			}
			msg, ok := panicVal.(string)
			if !ok {
				t.Fatalf("panic value is %T, want string", panicVal)
			}
			if !strings.Contains(msg, tc.field) {
				t.Errorf("panic message %q does not mention field name %q", msg, tc.field)
			}
		})
	}
}

// mustPanicWithField asserts that fn panics with a string containing fieldName.
func mustPanicWithField(t *testing.T, fieldName string, fn func()) {
	t.Helper()
	var panicVal any
	func() {
		defer func() { panicVal = recover() }()
		fn()
	}()
	if panicVal == nil {
		t.Fatalf("expected panic mentioning %q, but no panic occurred", fieldName)
	}
	msg, ok := panicVal.(string)
	if !ok {
		t.Fatalf("panic value is %T, want string", panicVal)
	}
	if !strings.Contains(msg, fieldName) {
		t.Errorf("panic message %q does not contain field name %q", msg, fieldName)
	}
}

func TestValidateCallbacks_NilConnectionOpened_Panics(t *testing.T) {
	mustPanicWithField(t, "connectionOpened", func() {
		cbs := fullCallbacks()
		cbs.connectionOpened = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilConnectionClosed_Panics(t *testing.T) {
	mustPanicWithField(t, "connectionClosed", func() {
		cbs := fullCallbacks()
		cbs.connectionClosed = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilTransactionBegun_Panics(t *testing.T) {
	mustPanicWithField(t, "transactionBegun", func() {
		cbs := fullCallbacks()
		cbs.transactionBegun = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilTransactionCommitted_Panics(t *testing.T) {
	mustPanicWithField(t, "transactionCommitted", func() {
		cbs := fullCallbacks()
		cbs.transactionCommitted = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilTransactionRolledBack_Panics(t *testing.T) {
	mustPanicWithField(t, "transactionRolledBack", func() {
		cbs := fullCallbacks()
		cbs.transactionRolledBack = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilQueryReceived_Panics(t *testing.T) {
	mustPanicWithField(t, "queryReceived", func() {
		cbs := fullCallbacks()
		cbs.queryReceived = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilQueryCompleted_Panics(t *testing.T) {
	mustPanicWithField(t, "queryCompleted", func() {
		cbs := fullCallbacks()
		cbs.queryCompleted = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilRowsFetched_Panics(t *testing.T) {
	mustPanicWithField(t, "rowsFetched", func() {
		cbs := fullCallbacks()
		cbs.rowsFetched = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilRowsReady_Panics(t *testing.T) {
	mustPanicWithField(t, "rowsReady", func() {
		cbs := fullCallbacks()
		cbs.rowsReady = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilRowInserted_Panics(t *testing.T) {
	mustPanicWithField(t, "rowInserted", func() {
		cbs := fullCallbacks()
		cbs.rowInserted = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilRowUpdated_Panics(t *testing.T) {
	mustPanicWithField(t, "rowUpdated", func() {
		cbs := fullCallbacks()
		cbs.rowUpdated = nil
		validateCallbacks(cbs)
	})
}

func TestValidateCallbacks_NilRowDeleted_Panics(t *testing.T) {
	mustPanicWithField(t, "rowDeleted", func() {
		cbs := fullCallbacks()
		cbs.rowDeleted = nil
		validateCallbacks(cbs)
	})
}
