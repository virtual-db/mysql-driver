package schema

// LoadListener is notified when a schema is successfully loaded from the source
// database. Implementations receive the table name, ordered column names, and
// the primary-key column (empty string when the table has no primary key).
type LoadListener interface {
	SchemaLoaded(table string, cols []string, pkCol string)
}

// NotifyingProvider wraps an inner Provider and calls a LoadListener after each
// successful GetSchema call. It allows the driver layer to forward schema-load
// events to the framework (e.g. core.DriverAPI.SchemaLoaded) without coupling
// the internal GMS layer to vdb-core.
//
// If listener is nil, GetSchema still delegates to the inner provider without
// panicking — the notify step is simply skipped.
type NotifyingProvider struct {
	inner    Provider
	listener LoadListener
}

// NewNotifyingProvider wraps inner with listener. listener may be nil; in that
// case, GetSchema behaves identically to calling inner.GetSchema directly.
func NewNotifyingProvider(inner Provider, listener LoadListener) *NotifyingProvider {
	return &NotifyingProvider{inner: inner, listener: listener}
}

// GetSchema satisfies Provider. It delegates to the inner provider; on success
// it calls listener.SchemaLoaded before returning.
func (w *NotifyingProvider) GetSchema(table string) ([]string, string, error) {
	cols, pkCol, err := w.inner.GetSchema(table)
	if err != nil {
		return nil, "", err
	}
	if w.listener != nil {
		w.listener.SchemaLoaded(table, cols, pkCol)
	}
	return cols, pkCol, nil
}
