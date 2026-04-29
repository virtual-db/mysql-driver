# mysql-driver

A MySQL protocol proxy for the [VirtualDB](https://github.com/virtual-db/core) framework. It binds a standard MySQL TCP endpoint that any MySQL-compatible client can connect to, then intercepts the full query and row lifecycle — delegating every event to a `core.DriverAPI` implementation supplied by the caller.

The driver persists nothing, transforms nothing, and stores nothing. All business logic lives in the `core.DriverAPI` implementation.

---

## Architecture

```
MySQL Client
     │  MySQL wire protocol
     ▼
┌─────────────────────────────────────┐
│           mysql-driver              │
│                                     │
│  Vitess listener (wire protocol)    │
│         │                           │
│  GMS SQL engine (parse / plan)      │
│         │                           │
│  EventBridge → core.DriverAPI  ◄────┼── your implementation
│         │                           │
│  Source DB pool (read-only)         │
└─────────────────────────────────────┘
     │  database/sql (read queries + schema)
     ▼
Real MySQL Source Database
```

Three external integration points:

1. **MySQL clients** connect to `Config.Addr` using the standard MySQL protocol.
2. **The source MySQL database** is used for schema reads (`INFORMATION_SCHEMA`) and row reads (`SELECT`). The driver never writes to it.
3. **`core.DriverAPI`** — the interface your product implements. The driver calls it at every lifecycle event.

---

## Installation

Requires Go 1.23.3 or later.

```
go get github.com/virtual-db/mysql-driver
```

---

## Quick Start

```go
import (
    "log"

    driver "github.com/virtual-db/mysql-driver"
    "github.com/virtual-db/core"
)

// MyAPI implements core.DriverAPI. Every method the driver calls must be present.
// See docs/driver-api.md for full parameter and return semantics.
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

func main() {
    cfg := driver.Config{
        Addr:           ":3307",
        DBName:         "mydb",
        SourceDSN:      "user:pass@tcp(localhost:3306)/mydb",
        AuthSourceAddr: "localhost:3306",
    }

    d := driver.NewDriver(cfg, &MyAPI{})

    if err := d.Run(); err != nil {
        log.Fatal(err)
    }
}
```

`Run` blocks until `Stop` is called or a fatal error occurs. See [docs/driver-api.md](docs/driver-api.md) for full method documentation.

---

## Documentation

| Document | Contents |
|---|---|
| [docs/configuration.md](docs/configuration.md) | All `Config` fields, types, defaults, and constraints |
| [docs/driver-api.md](docs/driver-api.md) | `core.DriverAPI` — every method, its parameters, return semantics, and veto behaviour |
| [docs/lifecycle.md](docs/lifecycle.md) | Event ordering, authentication, query execution, read/write paths, TLS |
| [docs/schema.md](docs/schema.md) | Schema resolution, `INFORMATION_SCHEMA` queries, and MySQL → GMS type mapping |
| [docs/error-handling.md](docs/error-handling.md) | Error behaviour and known limitations |

---

## License

Elastic License 2.0. See [LICENSE.md](LICENSE.md).

Free use, modification, and redistribution are permitted for any purpose that does not involve offering the software as a hosted or managed service to third parties. See [CLA.md](CLA.md) for contributor requirements.