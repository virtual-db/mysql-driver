package schema

// LoadListener is notified when a schema is successfully loaded from the source
// database. Implementations receive the table name, ordered column names, and
// the primary-key column (empty string when the table has no primary key).
//
// The column list is a plain []string of names rather than []ColumnDescriptor
// so that the vdb-core DriverAPI.SchemaLoaded signature (which uses []string)
// does not need to import the schema package.
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

// GetSchema satisfies Provider. It delegates to the inner provider and on
// success extracts the column name slice to pass to listener.SchemaLoaded
// before returning the full []ColumnDescriptor to the caller.
func (w *NotifyingProvider) GetSchema(table string) ([]ColumnDescriptor, string, error) {
	cols, pkCol, err := w.inner.GetSchema(table)
	if err != nil {
		return nil, "", err
	}
	if w.listener != nil {
		w.listener.SchemaLoaded(table, descriptorNames(cols), pkCol)
	}
	return cols, pkCol, nil
}

// descriptorNames extracts the Name field from each ColumnDescriptor and
// returns them as an ordered []string. This is used to satisfy the
// LoadListener.SchemaLoaded signature without changing vdb-core's interface.
func descriptorNames(cols []ColumnDescriptor) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}
