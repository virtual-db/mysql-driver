# vdb-mysql-driver

A MySQL protocol driver for the [VirtualDB](https://github.com/AnqorDX/vdb-core) framework. It exposes a standard MySQL TCP endpoint that any MySQL-compatible client can connect to, intercepts the full query and row lifecycle, and delegates every event to a `core.DriverAPI` implementation provided by the caller.

---

## What It Does

`vdb-mysql-driver` sits between a MySQL client and a real MySQL source database. It:

- Accepts MySQL protocol connections on a configurable TCP address.
- Authenticates clients by proxying the MySQL handshake to the source database — no credential storage of its own.
- Parses and plans SQL using [go-mysql-server](https://github.com/dolthub/go-mysql-server) (GMS).
- Reads row data from the source MySQL database, then passes those rows through two hook points (`RecordsSource`, `RecordsMerged`) so the framework can apply a delta overlay before returning rows to the client.
- Routes every DML write (`INSERT`, `UPDATE`, `DELETE`) through the framework (`RecordInserted`, `RecordUpdated`, `RecordDeleted`) instead of writing directly to the source.
- Fires lifecycle events for connections, transactions, queries, and schema changes so the framework layer can observe and control every step.

The driver itself does not store, cache, or transform any data. It is a pure event relay. All business logic lives in the `core.DriverAPI` implementation the caller supplies.

---

## How It Works

### Authentication

When a client connects, the driver opens a parallel TCP connection to the real MySQL source (`AuthSourceAddr`) and replays the MySQL handshake byte-for-byte. The source database validates credentials. On success the driver extracts the authenticated username, database name, and client capability flags, then closes the probe connection and takes over the session internally. GMS never sees real credentials — it receives a synthetic handshake response constructed from the values the source already verified.

### Query Execution

All SQL is parsed and planned by GMS. The `Interceptor` fires `QueryReceived` before each statement (allowing the framework to rewrite or reject the query) and `QueryCompleted` after execution. DML statements are routed through the `Table` implementation, which calls the framework for every affected row.

### Row Read Path

1. The `Table` fetches all rows from the source MySQL database via a plain `SELECT`.
2. Those rows are converted to `[]map[string]any` and passed to `RecordsSource` — the framework may add, remove, or modify records.
3. The result is passed to `RecordsMerged` — a second hook for applying a final delta overlay before rows are returned to the client.

### Row Write Path

For each row in an `INSERT`, `UPDATE`, or `DELETE` statement, the driver calls the corresponding `DriverAPI` method. The driver does **not** execute any write against the source database. Writes are entirely the framework's responsibility.

### Schema

Table schemas are loaded on demand by querying `INFORMATION_SCHEMA.COLUMNS` and `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` on the source database. The resolved schema is converted to GMS types and forwarded to `SchemaLoaded`. Downstream code can call `SchemaInvalidated` to force a refresh on the next access.

### Lifecycle Events

| Event | When |
|---|---|
| `ConnectionOpened` | After the MySQL handshake completes. Return an error to refuse. |
| `ConnectionClosed` | When a connection is torn down. |
| `TransactionBegun` | At `BEGIN` / `START TRANSACTION`. Return an error to refuse. |
| `TransactionCommitted` | At `COMMIT`. Return an error to leave the transaction open. |
| `TransactionRolledBack` | At `ROLLBACK` or `ROLLBACK TO SAVEPOINT`. |
| `QueryReceived` | Before each SQL statement. Return a rewritten query string to substitute it. |
| `QueryCompleted` | After each SQL statement finishes and all rows are sent. |
| `RecordsSource` | After rows are read from the source database. |
| `RecordsMerged` | After the framework delta overlay is applied, before rows are sent to the client. |
| `RecordInserted` | For each row in an `INSERT`. |
| `RecordUpdated` | For each row in an `UPDATE`, with both old and new values. |
| `RecordDeleted` | For each row in a `DELETE`. |
| `SchemaLoaded` | After a table schema is resolved from `INFORMATION_SCHEMA`. |
| `SchemaInvalidated` | When cached schema should be discarded. |

---

## Installation

Requires Go 1.23.3 or later.

```

go get github.com/AnqorDX/vdb-mysql-driver

```

---

## Usage

```go
import (
    driver "github.com/AnqorDX/vdb-mysql-driver"
    core   "github.com/AnqorDX/vdb-core"
)

// Implement core.DriverAPI with your business logic.
type myAPI struct{}

// ... implement all DriverAPI methods ...

func main() {
    cfg := driver.Config{
        Addr:             ":3307",
        DBName:           "mydb",
        SourceDSN:        "user:pass@tcp(localhost:3306)/mydb",
        AuthSourceAddr:   "localhost:3306",
        AuthProbeTimeout: 5 * time.Second,
        ConnReadTimeout:  time.Minute,
        ConnWriteTimeout: time.Minute,
    }

    d := driver.NewDriver(cfg, &myAPI{})

    // Run blocks until Stop is called or a fatal error occurs.
    if err := d.Run(); err != nil {
        log.Fatal(err)
    }
}
```

`NewDriver` panics if `Config.AuthSourceAddr` is empty. All timeout fields default to sensible values if left at zero.

---

## Configuration Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `Addr` | `string` | — | TCP address for the driver to listen on (e.g. `":3307"`). |
| `DBName` | `string` | — | Logical database name exposed to connecting clients. |
| `SourceDSN` | `string` | — | `database/sql` DSN for the real MySQL source database. |
| `AuthSourceAddr` | `string` | **required** | TCP address of the real MySQL server used for the auth proxy. |
| `AuthProbeTimeout` | `time.Duration` | 5s | Timeout for the per-connection auth probe to the source. |
| `ConnReadTimeout` | `time.Duration` | 1m | Per-connection read timeout passed to GMS. |
| `ConnWriteTimeout` | `time.Duration` | 1m | Per-connection write timeout passed to GMS. |

---

## Known Limitations

- **Composite primary keys** are not supported. Only the first column of a `PRIMARY KEY` constraint is recognised for duplicate-key detection and auto-increment tracking.
- **`ENUM` and `SET` columns** are mapped to `LONGTEXT` for schema purposes. Values are passed through correctly but GMS will not enforce valid enum/set membership.
- **`DECIMAL` precision/scale** is always mapped as `DECIMAL(10,0)`. Exact precision requires fetching `NUMERIC_PRECISION` and `NUMERIC_SCALE` from `INFORMATION_SCHEMA`, which is not yet implemented.
- **`SHOW TABLES`** returns an empty result. The source database's `INFORMATION_SCHEMA` handles table listing when queried directly.
- **Writes are not persisted by the driver.** DML events are forwarded to the framework; the framework is responsible for deciding what to do with them.
- **TLS is not supported** on the client-facing listener. SSL capability flags are stripped from the server greeting so clients do not attempt a TLS upgrade.

---

## Dependencies

| Package | Purpose |
|---|---|
| `github.com/AnqorDX/vdb-core` | Framework interface (`core.DriverAPI`, `core.Server`). |
| `github.com/dolthub/go-mysql-server` | SQL parsing, planning, and MySQL protocol server. |
| `github.com/dolthub/vitess` | Vitess types exposed in the GMS v0.20.x public API. |
| `github.com/go-sql-driver/mysql` | `database/sql` driver for the source MySQL connection. |

---

## License

Elastic License 2.0. See [LICENSE.md](LICENSE.md).

The EL v2 license allows free use, modification, and redistribution for any purpose that does not involve offering the software as a hosted or managed service to third parties. See [CLA.md](CLA.md) for contributor requirements.
