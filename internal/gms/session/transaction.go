package session

// Transaction is the GMS transaction token for vdb connections.
// It satisfies gmssql.Transaction (String, IsReadOnly).
type Transaction struct {
	ReadOnly bool
}

// String satisfies gmssql.Transaction.
func (t *Transaction) String() string {
	if t.ReadOnly {
		return "Transaction(readOnly)"
	}
	return "Transaction(readWrite)"
}

// IsReadOnly satisfies gmssql.Transaction.
func (t *Transaction) IsReadOnly() bool { return t.ReadOnly }
