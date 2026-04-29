# core.DriverAPI Reference

## No-op Stub

A complete no-op implementation you can copy and fill in:

```go
type MyAPI struct{}

func (a *MyAPI) ConnectionOpened(id uint32, user, addr string) error   { return nil }
func (a *MyAPI) ConnectionClosed(id uint32, user, addr string)         {}
func (a *MyAPI) TransactionBegun(connID uint32, readOnly bool) error   { return nil }
func (a *MyAPI) TransactionCommitted(connID uint32) error              { return nil }
func (a *MyAPI) TransactionRolledBack(connID uint32, savepoint string) {}
func (a *MyAPI) QueryReceived(connID uint32, query, database string) (string, error) {
    return "", nil
}
func (a *MyAPI) QueryCompleted(connID uint32, query string, rowsAffected int64, err error) {}
func (a *MyAPI) RecordsSource(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
    return records, nil
}
func (a *MyAPI) RecordsMerged(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
    return records, nil
}
func (a *MyAPI) RecordInserted(connID uint32, table string, record map[string]any) (map[string]any, error) {
    return record, nil
}
func (a *MyAPI) RecordUpdated(connID uint32, table string, old, new map[string]any) (map[string]any, error) {
    return new, nil
}
func (a *MyAPI) RecordDeleted(connID uint32, table string, record map[string]any) error {
    return nil
}
func (a *MyAPI) SchemaLoaded(table string, columns []string, pkCol string) {}
func (a *MyAPI) SchemaInvalidated(table string)                            {}
```

---


`core.DriverAPI` is the interface your product implements and passes to `NewDriver`. The driver calls it at every lifecycle event — connections, transactions, queries, reads, writes, and schema changes.

```go
type DriverAPI interface {
    ConnectionOpened(id uint32, user, addr string) error
    ConnectionClosed(id uint32, user, addr string)

    TransactionBegun(connID uint32, readOnly bool) error
    TransactionCommitted(connID uint32) error
    TransactionRolledBack(connID uint32, savepoint string)

    QueryReceived(connID uint32, query, database string) (string, error)
    QueryCompleted(connID uint32, query string, rowsAffected int64, err error)

    RecordsSource(connID uint32, table string, records []map[string]any) ([]map[string]any, error)
    RecordsMerged(connID uint32, table string, records []map[string]any) ([]map[string]any, error)

    RecordInserted(connID uint32, table string, record map[string]any) (map[string]any, error)
    RecordUpdated(connID uint32, table string, old, new map[string]any) (map[string]any, error)
    RecordDeleted(connID uint32, table string, record map[string]any) error

    SchemaLoaded(table string, columns []string, pkCol string)
    SchemaInvalidated(table string)
}
```

`connID` is the Vitess connection ID (`uint32`), unique for the lifetime of each connection.

Records are always `[]map[string]any` keyed by column name, with Go-native values (`int64`, `float64`, `string`, `[]byte`, `time.Time`).

---

## Connection

### `ConnectionOpened(id uint32, user, addr string) error`

Called after the MySQL handshake and auth probe both succeed.

- `id` — connection ID.
- `user` — authenticated MySQL username.
- `addr` — client remote address.

Return non-nil to refuse the connection. The client receives a generic error, the connection is closed, and all subsequent queries on it return an error.

---

### `ConnectionClosed(id uint32, user, addr string)`

Called when the TCP connection tears down — whether closed by the client, the server, or a network error. Cannot be vetoed.

---

## Transactions

### `TransactionBegun(connID uint32, readOnly bool) error`

Called at `BEGIN` or `START TRANSACTION`.

- `readOnly` — true if `START TRANSACTION READ ONLY` was used.

Return non-nil to refuse the transaction. The client receives the error.

---

### `TransactionCommitted(connID uint32) error`

Called at `COMMIT`.

Return non-nil to leave the transaction open. The error is returned to the client.

---

### `TransactionRolledBack(connID uint32, savepoint string)`

Called at `ROLLBACK` or `ROLLBACK TO SAVEPOINT <name>`.

- `savepoint` — savepoint name for `ROLLBACK TO SAVEPOINT`; empty string for a full rollback.

Return value is ignored. Cannot be vetoed.

---

## Queries

### `QueryReceived(connID uint32, query, database string) (string, error)`

Called before each SQL statement, before GMS parses or plans it.

- `query` — the raw SQL string from the client.
- `database` — the current database name for this connection.

Return a non-empty string to substitute a different query for execution. The original query — not the rewritten one — is what gets passed to `QueryCompleted`.

Return a non-nil error to reject the query entirely. The client receives the error and execution is skipped.

Return `("", nil)` to execute the original query unchanged.

---

### `QueryCompleted(connID uint32, query string, rowsAffected int64, err error)`

Called after the statement finishes and all rows have been sent to the client.

- `query` — the original query as received from the client, before any rewrite.
- `rowsAffected` — rows affected for DML; `0` for `SELECT`.
- `err` — any execution error, or `nil` on success.

Return value is ignored.

---

## Row Reads

### `RecordsSource(connID uint32, table string, records []map[string]any) ([]map[string]any, error)`

Called after rows are fetched from the source database, before any delta overlay is applied.

- `records` — the raw rows from the source, each as a `map[string]any` keyed by column name.

Return the slice to use going forward. You may add, remove, reorder, or modify records.

Return non-nil to abort the read. The client receives the error.

---

### `RecordsMerged(connID uint32, table string, records []map[string]any) ([]map[string]any, error)`

Called after `RecordsSource` and after the framework has applied its delta overlay. This is the final opportunity to filter or transform the result set before rows are sent to the client.

- `records` — the post-overlay slice.

Return the final slice to send to the client.

Return non-nil to abort. The client receives the error.

---

## Row Writes

The driver calls these methods for every row affected by a DML statement. The driver does **not** write to the source database — persistence is entirely your responsibility.

### `RecordInserted(connID uint32, table string, record map[string]any) (map[string]any, error)`

Called for each row in an `INSERT` statement.

- `record` — the row being inserted, as a `map[string]any` keyed by column name.

Return a (possibly modified) record. The returned value is what GMS treats as canonical for subsequent operations within the same query (e.g. `ON DUPLICATE KEY UPDATE` resolution).

Return non-nil to abort the insert. The client receives the error.

---

### `RecordUpdated(connID uint32, table string, old, new map[string]any) (map[string]any, error)`

Called for each row matched by an `UPDATE` statement.

- `old` — the row's values before the update.
- `new` — the row's values after the update.

Return a (possibly modified) new record.

Return non-nil to abort. The client receives the error.

---

### `RecordDeleted(connID uint32, table string, record map[string]any) error`

Called for each row matched by a `DELETE` statement.

- `record` — the row being deleted.

Return non-nil to abort. The client receives the error.

---

## Schema

### `SchemaLoaded(table string, columns []string, pkCol string)`

Called after a table's schema is resolved from `INFORMATION_SCHEMA`. Informational only.

- `columns` — ordered list of column names.
- `pkCol` — name of the first primary-key column, or empty string if none.

Return value is ignored.

---

### `SchemaInvalidated(table string)`

Signals that any cached schema for the given table should be discarded. The driver itself **never** calls this method — it exists for the framework to invoke externally when schema invalidation is needed.