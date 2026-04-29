# Lifecycle & Data Flow

- [Connection Lifecycle](#connection-lifecycle)
- [Query Execution](#query-execution)
- [Row Read Path](#row-read-path)
- [Row Write Path](#row-write-path)
- [Transaction Lifecycle](#transaction-lifecycle)
- [Authentication](#authentication)
- [TLS](#tls)

---

## Connection Lifecycle

Each TCP connection from a MySQL client goes through the following sequence:

1. **TCP accept** — the Vitess listener accepts the connection.
2. **Auth probe** — the driver opens a short-lived connection to `AuthSourceAddr`, issues `PING` + `SHOW GRANTS FOR CURRENT_USER()`, and closes it. Bad credentials produce MySQL error `1045` and the connection is refused.
3. **`ConnectionOpened`** — fires after the probe succeeds. Return a non-nil error to refuse the connection at the application level.
4. **Session created** — a GMS session is attached to the connection, carrying the authenticated user's grants.
5. **Query loop** — the connection accepts SQL statements until the client disconnects or the driver is stopped.
6. **`ConnectionClosed`** — fires when the TCP connection tears down for any reason. Cannot be vetoed.

---

## Query Execution

For every SQL statement received from a client:

1. **`QueryReceived`** fires before parsing. You may rewrite the query (return a non-empty string) or reject it entirely (return a non-nil error). The original query string — not the rewritten one — is always passed to `QueryCompleted`.
2. The (possibly rewritten) SQL is handed to the GMS engine for parsing and planning.
3. The engine executes the plan. For `SELECT`, this triggers the [Row Read Path](#row-read-path). For `INSERT`/`UPDATE`/`DELETE`, this triggers the [Row Write Path](#row-write-path).
4. All result rows are buffered and sent to the client in a single MySQL result packet.
5. **`QueryCompleted`** fires after all rows are sent, carrying the original query, affected row count, and any execution error.

**Prepared statements** (`COM_STMT_PREPARE` / `COM_STMT_EXECUTE`) are supported. `COM_STMT_PREPARE` returns no parameter field metadata.

**Multi-statement queries** (semicolon-separated) execute only the first statement. The rest is silently discarded.

---

## Row Read Path

Triggered for any query that requires reading rows from a table.

```
Source DB
   │
   │  SELECT col1, col2, ... FROM <db>.<table>
   ▼
[]map[string]any  (all columns, all rows)
   │
   │  RecordsSource(connID, table, records)
   ▼
[]map[string]any  (framework applies delta overlay here)
   │
   │  RecordsMerged(connID, table, records)
   ▼
[]map[string]any  (final result)
   │
   ▼
MySQL client
```

- The driver always fetches all columns and all rows. There is no predicate pushdown to the source database — `WHERE` clauses are evaluated in memory by GMS after `RecordsMerged` returns.
- `RecordsSource` receives the raw rows from the source. You may add, remove, or modify records.
- `RecordsMerged` receives the slice returned by `RecordsSource` (after any overlay the framework applied). This is the last opportunity to filter or transform before rows are sent to the client.
- A non-nil error from either hook aborts the read and sends the error to the client.

---

## Row Write Path

Triggered for `INSERT`, `UPDATE`, and `DELETE`. The driver calls the appropriate `DriverAPI` method for **every affected row**. The driver never executes any write against the source database.

```
GMS engine (DML plan)
   │
   ├── INSERT → RecordInserted(connID, table, record)
   │              └── return modified record, or error to abort
   │
   ├── UPDATE → RecordUpdated(connID, table, old, new)
   │              └── return modified new record, or error to abort
   │
   └── DELETE → RecordDeleted(connID, table, record)
                  └── return error to abort
```

Duplicate-key detection for `INSERT ... ON DUPLICATE KEY UPDATE` is performed by reading the full merged view (through `RecordsSource` → `RecordsMerged`) and comparing primary key values in memory. Only single-column primary keys are supported.

---

## Transaction Lifecycle

```
BEGIN / START TRANSACTION
   │
   │  TransactionBegun(connID, readOnly)
   │  └── return error to refuse
   │
   ▼
[query loop — reads and writes operate against the live + tx delta]
   │
   ├── COMMIT
   │     │  TransactionCommitted(connID)
   │     │  └── return error to leave the transaction open
   │
   ├── ROLLBACK
   │     │  TransactionRolledBack(connID, "")
   │     │  (cannot be vetoed)
   │
   └── ROLLBACK TO SAVEPOINT <name>
         │  TransactionRolledBack(connID, name)
         │  (cannot be vetoed)
```

`CREATE SAVEPOINT` and `RELEASE SAVEPOINT` are accepted by the parser but are no-ops at the engine level.

---

## Authentication

The driver contains no credential store. On every new connection:

1. A probe connection is opened to `Config.AuthSourceAddr` using the client's supplied credentials.
2. `PING` is issued to validate the credentials.
3. `SHOW GRANTS FOR CURRENT_USER()` is issued to collect the user's grant statements.
4. The probe connection is closed.

The full probe must complete within `Config.AuthProbeTimeout` (default `5s`). Connections that exceed this timeout are refused.

On a successful probe, the user's grants are attached to the session for its lifetime. If `SHOW GRANTS` fails after a successful `PING`, authentication still succeeds but the session carries no grant information.

Credential validation errors produce MySQL error `1045 (Access denied for user ...)`. Network errors or timeout produce a connection error.

---

## TLS

TLS behaviour is determined by `Config.TLSConfig`.

| `TLSConfig` | Auth method advertised | Behaviour |
|---|---|---|
| `nil` | `mysql_clear_password` | Plaintext passwords accepted without TLS. `AllowClearTextWithoutTLS` is set on the listener. Suitable for local development or trusted private networks. |
| non-nil `*tls.Config` | `caching_sha2_password` | The listener requires TLS. The SHA-2 cache fast-path is permanently disabled so the plaintext password is always available for the auth probe. |

In both modes the plaintext password is forwarded to `AuthSourceAddr` for validation and is never stored by the driver.