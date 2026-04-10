package driver

// stubSchemaProvider satisfies schemaProvider for tests with configurable return values.
type stubSchemaProvider struct {
	cols  []string
	pkCol string
	err   error
}

func (s *stubSchemaProvider) GetSchema(_ string) ([]string, string, error) {
	return s.cols, s.pkCol, s.err
}

// fullCallbacks returns a callbacks struct with all twelve fields populated
// with minimal no-op functions. Used by any test that needs a valid callbacks.
func fullCallbacks() callbacks {
	return callbacks{
		connectionOpened:      func(id uint32, user, addr string) error { return nil },
		connectionClosed:      func(id uint32, user, addr string) {},
		transactionBegun:      func(connID uint32, ro bool) error { return nil },
		transactionCommitted:  func(connID uint32) error { return nil },
		transactionRolledBack: func(connID uint32, sp string) {},
		queryReceived: func(connID uint32, q, db string) (string, error) {
			return q, nil
		},
		queryCompleted: func(connID uint32, q string, n int64, err error) {},
		rowsFetched: func(connID uint32, t string, r []map[string]any) ([]map[string]any, error) {
			return r, nil
		},
		rowsReady: func(connID uint32, t string, r []map[string]any) ([]map[string]any, error) {
			return r, nil
		},
		rowInserted: func(connID uint32, t string, r map[string]any) (map[string]any, error) {
			return r, nil
		},
		rowUpdated: func(connID uint32, t string, o, n map[string]any) (map[string]any, error) {
			return n, nil
		},
		rowDeleted: func(connID uint32, t string, r map[string]any) error { return nil },
	}
}
