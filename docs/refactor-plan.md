# vdb-mysql-driver — Structural Refactor Plan

> **Dependency note:** This plan is written against the vdb-core refactor plan.
> Two prerequisite changes from that plan directly affect this module:
>
> 1. `types.Record` is eliminated from `vdb-core`'s public API. `DriverAPI`
>    method signatures use `map[string]any` directly. Since `vdb-mysql-driver`
>    already uses `map[string]any` throughout its `callbacks` struct, this
>    requires zero modification here. It does confirm that `bridge.EventBridge`
>    is the correct long-term shape.
>
> 2. `core.DriverReceiver` is eliminated from `vdb-core`'s public API. The
>    implicit `SetDriverAPI` auto-wiring inside `UseDriver` is replaced by an
>    explicit `app.DriverAPI() core.DriverAPI` getter. The composition root
>    (`vdb-mysql`) calls `app.DriverAPI()` and passes the result directly to
>    `mysql.NewDriver(cfg, api)`. As a consequence, `Driver.SetDriverAPI` is not
>    a public method and `Driver` does not implement `core.DriverReceiver`.
>    The wiring logic previously in `SetDriverAPI` moves into `New()`.

## Status: COMPLETE

---

## 1. Problem Statement

The module ships 5 source files totalling ~1,320 lines. Three of those files have
serious structural problems that make the codebase difficult to navigate, test, or
extend.

**`session.go` — 595 lines, 8 unrelated domains.**
This single file owns: the GMS transaction type, the GMS session type and all its
interface implementations, the session factory, the query interceptor, the database
and table hierarchy, the write adapter, the partition iterator, and two context helper
functions. A developer looking for query interception logic must scan past 160 lines
of session and transaction code to find it. A developer making a change to table
row-fetching must open the same file as one making a change to savepoint handling.

**`callbacks` — a struct of 12 anonymous function fields.**
`callbacks.go` defines a flat bag of function fields that is effectively an interface
expressed in the wrong construct. Anonymous function fields give you none of what Go
interfaces give you: no method-level documentation, no named navigation target, no
compile-time satisfaction check, no stub construction without `reflect`. The fact that
`validateCallbacks` must use `reflect.ValueOf(c.fn).IsNil()` to check for nil fields
at runtime is the clearest possible signal that this should be an interface. Every
field of `callbacks` maps 1:1 to a method on `core.DriverAPI` — the struct exists
solely because the GMS layer avoids importing vdb-core. The correct tool for
describing a contract without sharing a concrete type is an interface.

**`SetDriverAPI` — one method doing six jobs.**
It opens the source database connection, builds a schema provider, wraps the schema
provider, wires 12 anonymous closures (one per DriverAPI method), validates them with
reflection, builds the row provider, and builds the GMS engine. This is not a method;
it is an inlined initialiser for six distinct subsystems. None of those steps can be
tested independently because they are all anonymous and all entangled.

---

## 2. Design Goals

1. **Internal packages for domain isolation.** Each GMS integration concern gets its
   own package under `internal/`. The Go compiler enforces the boundary. `session`
   cannot accidentally call `queryInterceptor` logic and vice versa.

2. **`EventBridge` interface replaces `callbacks` struct.** The contract between the
   GMS layer and the outside world is expressed as a proper Go interface. The compiler
   validates satisfaction. Tests supply stubs by implementing the interface. No
   reflection. No `validateCallbacks`.

3. **`apiAdapter` replaces anonymous closures.** The root package provides a named
   struct that adapts `core.DriverAPI` to `bridge.EventBridge`. Each method is named,
   documented, and navigable.

4. **`SetDriverAPI` becomes a wiring function.** After refactor it calls named
   constructors from each internal package and assembles the result. It does not
   contain any logic — only construction and wiring.

5. **Every internal package is independently testable.** A test in
   `internal/gms/session` needs only a stub `bridge.EventBridge`. It does not need a
   real `core.DriverAPI`, a real MySQL connection, or a real GMS engine.

6. **No file exceeds 200 lines.**

---

## 3. Import DAG


The rule: edges point downward only. No package imports anything above it.

```
┌──────────────────────────────────────────────────────────┐
│  vdb-mysql-driver (root, package driver)                 │
│                                                          │
│  driver.go  (Driver, Config, New, Run, Stop, apiAdapter) │
└──────┬───────────────────────────────────────────────────┘
       │ imports
       ▼
┌─────────────────────────────────────────────────────────┐
│  internal/gms/session   internal/gms/query              │
│  internal/gms/engine    internal/gms/rows               │
│  internal/schema                                        │
└──────┬──────────────────────────────────────────────────┘
       │ all import
       ▼
┌──────────────────────────────────────────────────────────┐
│  internal/bridge                                         │
│  (EventBridge interface — no imports from this module)   │
└──────────────────────────────────────────────────────────┘
```

`vdb-core` is imported **only** by the root package. No internal package knows that
vdb-core exists. The `bridge.EventBridge` interface describes the contract that
`core.DriverAPI` satisfies from the outside; it is defined at the point of
consumption (the GMS integration layer), not at the point of implementation.

`github.com/dolthub/go-mysql-server` and `github.com/dolthub/vitess` are imported
by the GMS-specific internal packages (`session`, `query`, `engine`, `rows`) and by
the root for `Run()`. `internal/bridge` and `internal/schema` do not import GMS
types except where unavoidable (e.g. `toGMSSchema` in `internal/schema` must
produce a `gmssql.Schema`).

---

## 4. Target File Tree

```
vdb-mysql-driver/
│
│   ── Public API (package driver) ────────────────────────────────────────
│
├── driver.go               # Driver, Config, New(), Run(), Stop(), apiAdapter
│
│   ── Internal packages ───────────────────────────────────────────────────
│
├── internal/
│   │
│   ├── bridge/             # The event contract between GMS and the outside world
│   │   ├── events.go       # EventBridge interface
│   │   └── events_test.go  # Compile-time stub satisfaction check
│   │
│   ├── schema/             # Table schema resolution
│   │   ├── provider.go     # Provider interface, SQLProvider (sqlSchemaProvider)
│   │   ├── notify.go       # NotifyingProvider (wrappedSchemaProvider)
│   │   ├── gms.go          # ToGMSSchema (toGMSSchema)
│   │   └── provider_test.go
│   │
│   └── gms/                # go-mysql-server integration layer
│       │
│       ├── session/        # GMS session + transaction lifecycle
│       │   ├── session.go      # Session struct + GMS interface implementations
│       │   ├── transaction.go  # Transaction type
│       │   ├── factory.go      # SessionBuilder (buildSessionBuilder)
│       │   └── session_test.go
│       │
│       ├── query/          # Query interception
│       │   ├── interceptor.go  # Interceptor struct + Query, ParsedQuery, etc.
│       │   └── interceptor_test.go
│       │
│       ├── engine/         # Database, table, and write hierarchy
│       │   ├── provider.go     # DatabaseProvider (vdbDatabaseProvider)
│       │   ├── database.go     # Database (vdbDatabase)
│       │   ├── table.go        # Table (vdbTable), fetchFromSource, partition types
│       │   ├── writer.go       # RowWriter (vdbRowWriter)
│       │   └── engine_test.go
│       │
│       └── rows/           # Row type translation (GMS sql.Row ↔ map[string]any)
│           ├── provider.go     # Provider interface, GMSProvider (gmsRowProvider)
│           ├── converter.go    # RowToMap, MapToRow, SchemaColumns
│           ├── iter.go         # Iter (vdbRowIter)
│           └── converter_test.go
│
│   ── Tests ─────────────────────────────────────────────────────────────
│
├── driver_test.go          # New(), SetDriverAPI() nil-guard, Run() before SetDriverAPI
├── rows_test.go            # (retain and update to use internal/gms/rows directly)
├── schema_test.go          # (retain and update to use internal/schema directly)
└── testutil_test.go        # stubEventBridge (replaces fullCallbacks + stubSchemaProvider)
```

---

## 5. Package Definitions

---

### 5.1 `internal/bridge`

**Purpose:** Defines the event contract between the GMS integration layer and the
outside world. Imports nothing from this module. Contains zero implementation.

**`events.go`**

```go
package bridge

// EventBridge is the contract the GMS session, query interceptor, and table
// implementations use to signal lifecycle events. It does not import vdb-core.
// The root driver package satisfies this interface by adapting core.DriverAPI.
//
// All methods mirror the corresponding core.DriverAPI method in name and
// semantics. map[string]any is used throughout because core.DriverAPI itself
// uses map[string]any directly — types.Record no longer exists in vdb-core's
// public API. There is no type conversion anywhere in the adapter.
type EventBridge interface {
    // Connection lifecycle.
    ConnectionOpened(id uint32, user, addr string) error
    ConnectionClosed(id uint32, user, addr string)

    // Transaction lifecycle.
    TransactionBegun(connID uint32, readOnly bool) error
    TransactionCommitted(connID uint32) error
    TransactionRolledBack(connID uint32, savepoint string)

    // Query lifecycle.
    QueryReceived(connID uint32, query, database string) (string, error)
    QueryCompleted(connID uint32, query string, rowsAffected int64, err error)

    // Row lifecycle.
    RowsFetched(connID uint32, table string, records []map[string]any) ([]map[string]any, error)
    RowsReady(connID uint32, table string, records []map[string]any) ([]map[string]any, error)
    RowInserted(connID uint32, table string, record map[string]any) (map[string]any, error)
    RowUpdated(connID uint32, table string, old, new map[string]any) (map[string]any, error)
    RowDeleted(connID uint32, table string, record map[string]any) error

    // Schema lifecycle.
        SchemaLoaded(table string, cols []string, pkCol string)
        SchemaInvalidated(table string)
}
```

`EventBridge` replaces the `callbacks` struct entirely. The compiler validates that
any implementation covers all methods. Tests implement it with a stub struct. No
reflection. No `validateCallbacks`.

Note that `SchemaInvalidated` is added here. The current `callbacks` struct omits
it — schema invalidation currently bypasses the bridge entirely. This is a latent
bug: `core.DriverAPI.SchemaInvalidated` exists and is called by the framework, but
the driver never forwards it to anything. `EventBridge` exposes it so the GMS layer
can act on it in future (e.g. clearing a schema cache). The `apiAdapter` in the
root package wires it to `api.SchemaInvalidated`.

---

### 5.2 `internal/schema`

**Purpose:** Table schema resolution. Self-contained; no GMS session or query
logic. Contains the interface, the SQL-backed implementation, the notifying
decorator, and the GMS schema conversion helper.

**`provider.go`**

```go
package schema

// Provider queries a database for table column metadata.
// All implementations must be safe for concurrent use.
type Provider interface {
    // GetSchema returns the ordered column names and the primary-key column
    // for the given table. Returns a non-nil error if the table does not exist.
    GetSchema(table string) (columns []string, pkCol string, err error)
}

// SQLProvider implements Provider by querying INFORMATION_SCHEMA on the
// source MySQL database.
type SQLProvider struct { ... }

func NewSQLProvider(db *sql.DB, dbName string) *SQLProvider
func (p *SQLProvider) GetSchema(table string) ([]string, string, error)
```

**`notify.go`**

```go
package schema

// LoadListener is notified whenever a schema is successfully loaded.
// bridge.EventBridge satisfies this interface via its SchemaLoaded method.
type LoadListener interface {
    SchemaLoaded(table string, cols []string, pkCol string)
}

// NotifyingProvider wraps a Provider and calls listener.SchemaLoaded after
// each successful GetSchema. This is how the root driver forwards schema
// loads to core.DriverAPI without the schema layer knowing about vdb-core.
type NotifyingProvider struct { ... }

func NewNotifyingProvider(inner Provider, listener LoadListener) *NotifyingProvider
func (p *NotifyingProvider) GetSchema(table string) ([]string, string, error)
```

**`gms.go`**

```go
package schema

// ToGMSSchema converts a column name list to a GMS sql.Schema.
// All columns are typed as LongText; GMS coerces values at execution time.
func ToGMSSchema(table string, columns []string) gmssql.Schema
```

Test: `provider_test.go` uses `go-sqlmock` to unit-test `SQLProvider.GetSchema`
without a real MySQL instance — same coverage as the current `schema_test.go`.
`NotifyingProvider` is tested by verifying that the listener is called exactly once
per successful `GetSchema`.

---

### 5.3 `internal/gms/rows`

**Purpose:** Translation between GMS `sql.Row` (an `[]interface{}`) and
`map[string]any`, and the `Provider` interface that drives row lifecycle callbacks.

**`provider.go`**

```go
package rows

// Provider translates GMS row representations into map[string]any form
// and invokes the appropriate EventBridge method at each row lifecycle moment.
// All methods operate without knowledge of vdb-core.
type Provider interface {
    FetchRows(ctx *gmssql.Context, table string, rows []gmssql.Row, schema gmssql.Schema) ([]map[string]any, error)
    CommitRows(ctx *gmssql.Context, table string, records []map[string]any) ([]map[string]any, error)
    InsertRow(ctx *gmssql.Context, table string, row gmssql.Row, schema gmssql.Schema) (map[string]any, error)
    UpdateRow(ctx *gmssql.Context, table string, old, new gmssql.Row, schema gmssql.Schema) (map[string]any, error)
    DeleteRow(ctx *gmssql.Context, table string, row gmssql.Row, schema gmssql.Schema) error
}

// GMSProvider is the concrete Provider implementation.
type GMSProvider struct { ... }

func NewGMSProvider(events bridge.EventBridge) *GMSProvider
```

**`converter.go`**

```go
package rows

// RowToMap converts a GMS sql.Row to map[string]any using the ordered column
// name slice. Panics if len(row) != len(cols) — a mismatch is a programming error.
func RowToMap(row gmssql.Row, cols []string) map[string]any

// MapToRow is the inverse of RowToMap.
func MapToRow(record map[string]any, cols []string) gmssql.Row

// SchemaColumns extracts the ordered column name slice from a GMS sql.Schema.
func SchemaColumns(schema gmssql.Schema) []string
```

**`iter.go`**

```go
package rows

// Iter wraps a []gmssql.Row slice to implement gmssql.RowIter.
type Iter struct { ... }
```

Test: `converter_test.go` exercises `RowToMap`, `MapToRow`, and `SchemaColumns`
with table-driven cases including the panic path. These are pure functions with
no GMS engine dependency.

---

### 5.4 `internal/gms/session`

**Purpose:** GMS session and transaction lifecycle. Maps GMS session callbacks to
`EventBridge` calls.

**`transaction.go`**

```go
package session

// Transaction is the opaque token returned by StartTransaction.
// Implements gmssql.Transaction.
type Transaction struct {
    ReadOnly bool
}

func (t *Transaction) String() string
func (t *Transaction) IsReadOnly() bool
```

**`session.go`**

```go
package session

// Session embeds *gmssql.BaseSession and implements:
//   - gmssql.Session                (via embedding)
//   - gmssql.LifecycleAwareSession  (CommandBegin, CommandEnd, SessionEnd)
//   - gmssql.TransactionSession     (StartTransaction, CommitTransaction,
//                                    Rollback, CreateSavepoint,
//                                    RollbackToSavepoint, ReleaseSavepoint)
type Session struct {
    *gmssql.BaseSession
    connID uint32
    events bridge.EventBridge
}

func (s *Session) CommandBegin() error
func (s *Session) CommandEnd()
func (s *Session) SessionEnd()
func (s *Session) StartTransaction(ctx *gmssql.Context, tChar gmssql.TransactionCharacteristic) (gmssql.Transaction, error)
func (s *Session) CommitTransaction(ctx *gmssql.Context, tx gmssql.Transaction) error
func (s *Session) Rollback(ctx *gmssql.Context, tx gmssql.Transaction) error
func (s *Session) CreateSavepoint(ctx *gmssql.Context, tx gmssql.Transaction, name string) error
func (s *Session) RollbackToSavepoint(ctx *gmssql.Context, tx gmssql.Transaction, name string) error
func (s *Session) ReleaseSavepoint(ctx *gmssql.Context, tx gmssql.Transaction, name string) error
```

**`factory.go`**

```go
package session

// Builder returns a server.SessionBuilder that:
//  1. Extracts connID, user, and addr from the Vitess connection.
//  2. Calls events.ConnectionOpened — a non-nil error refuses the connection.
//  3. Constructs and returns a *Session on success.
func Builder(dbName string, events bridge.EventBridge) server.SessionBuilder
```

Test: `session_test.go` creates a `*Session` with a stub `bridge.EventBridge`
and calls `StartTransaction`, `CommitTransaction`, `Rollback`, and `SessionEnd`
directly. Asserts that the correct `EventBridge` method was called with the correct
arguments. No GMS engine. No network.

---

### 5.5 `internal/gms/query`

**Purpose:** Query interception. Wraps the GMS `server.Interceptor` contract to
fire `QueryReceived` and `QueryCompleted` callbacks.

**`interceptor.go`**

```go
package query

// Interceptor implements server.Interceptor. It intercepts text queries,
// prepared queries, and multi-queries to fire QueryReceived before execution
// and QueryCompleted after.
type Interceptor struct {
    events bridge.EventBridge
}

func NewInterceptor(events bridge.EventBridge) *Interceptor

func (i *Interceptor) Priority() int
func (i *Interceptor) Query(ctx context.Context, chain server.Chain, conn *vitessmysql.Conn, query string, callback func(*sqltypes.Result, bool) error) error
func (i *Interceptor) ParsedQuery(chain server.Chain, conn *vitessmysql.Conn, query string, parsed sqlparser.Statement, callback func(*sqltypes.Result, bool) error) error
func (i *Interceptor) MultiQuery(ctx context.Context, chain server.Chain, conn *vitessmysql.Conn, query string, callback func(*sqltypes.Result, bool) error) (string, error)
func (i *Interceptor) Prepare(ctx context.Context, chain server.Chain, conn *vitessmysql.Conn, query string, prepare *vitessmysql.PrepareData) ([]*querypb.Field, error)
func (i *Interceptor) StmtExecute(ctx context.Context, chain server.Chain, conn *vitessmysql.Conn, prepare *vitessmysql.PrepareData, callback func(*sqltypes.Result) error) error
```

The `connIDFromCtx` and `databaseFromContext` helpers move here as unexported
package-level functions, since `Interceptor` is the only consumer.

Test: `interceptor_test.go` constructs an `Interceptor` with a stub
`bridge.EventBridge`. Verifies that `QueryReceived` is called with the original
query, that a rewritten query is passed to the chain, and that `QueryCompleted`
is called with the correct rowsAffected count.

---

### 5.6 `internal/gms/engine`

**Purpose:** The GMS database, table, and write hierarchy. This is the data access
layer that makes GMS understand VirtualDB's single-database, pass-through model.

**`provider.go`**

```go
package engine

// DatabaseProvider implements gmssql.DatabaseProvider. It vends a single
// Database backed by the given rows and schema providers.
type DatabaseProvider struct {
    dbName string
    rows   rows.Provider
    schema schema.Provider
    db     *sql.DB
}

func NewDatabaseProvider(
    dbName string,
    rows rows.Provider,
    schema schema.Provider,
    db *sql.DB,
) *DatabaseProvider
```

**`database.go`**

```go
package engine

// Database implements gmssql.Database. GetTableInsensitive resolves the table
// schema via the schema provider and returns a Table on success.
type Database struct { ... }
```

**`table.go`**

```go
package engine

// Table implements gmssql.Table, gmssql.InsertableTable,
// gmssql.UpdatableTable, and gmssql.DeletableTable. It reads rows from the
// source database and routes writes through the rows.Provider.
type Table struct { ... }
```

The `fetchFromSource`, `singlePartition`, and `singlePartitionIter` types move
into `table.go` as they are implementation details of `Table.PartitionRows`.

**`writer.go`**

```go
package engine

// RowWriter implements gmssql.RowInserter, gmssql.RowUpdater, and
// gmssql.RowDeleter. All write operations are delegated to rows.Provider.
type RowWriter struct { ... }
```

Test: `engine_test.go` constructs a `DatabaseProvider` with a stub
`rows.Provider` and a stub `schema.Provider`. Verifies that
`GetTableInsensitive` returns the correct table shape and that write
operations reach the stub provider.

---

## 6. Root Package After Refactor

### `driver.go`

The `Driver` struct, `New`, `Run`, and `Stop` remain here. `SetDriverAPI` is
removed — `core.DriverAPI` is now passed directly to `New()`. All wiring that
was previously deferred to `SetDriverAPI` happens unconditionally at construction
time. `noopServerEventListener` is moved to an unexported type inside `driver.go`
(it is a one-off GMS adapter with no domain significance of its own).

```go
// driver.go
package driver

// Driver is the MySQL wire-protocol adapter for VirtualDB.
// Satisfies core.Server via Run and Stop.
// Accepts core.DriverAPI at construction — the composition root obtains it
// via app.DriverAPI() before calling New().
type Driver struct {
    cfg    Config
    api    core.DriverAPI
    bridge *apiAdapter        // adapts api to bridge.EventBridge
    schema schema.Provider    // notifying schema provider
    rows   rows.Provider      // GMS row lifecycle provider
    db     *sql.DB
    gms    *sqle.Engine
    srv    *server.Server
    mu     sync.Mutex
}

// New constructs a fully wired Driver. api is the framework's DriverAPI
// implementation, obtained from the composition root via app.DriverAPI().
//
// Typical composition-root usage:
//
//   api    := app.DriverAPI()
//   driver := mysql.NewDriver(cfg, api)
//   app.UseDriver(driver)
//   app.Run()
func NewDriver(cfg Config, api core.DriverAPI) *Driver {
    adapter := &apiAdapter{api: api}

    sourceDB := mustOpenDB(cfg.SourceDSN)

    raw := schema.NewSQLProvider(sourceDB, cfg.DBName)
    notifying := schema.NewNotifyingProvider(raw, adapter)
    rowProvider := rows.NewGMSProvider(adapter)

    return &Driver{
        cfg:    cfg,
        api:    api,
        bridge: adapter,
        schema: notifying,
        rows:   rowProvider,
        db:     sourceDB,
        gms: sqle.NewDefault(engine.NewDatabaseProvider(
            cfg.DBName, rowProvider, notifying, sourceDB,
        )),
    }
}
```

### `apiAdapter` — replaces `callbacks`

```go
// apiAdapter adapts core.DriverAPI to bridge.EventBridge.
// It is the only place in the codebase where vdb-core types and GMS types meet.
// Every method is a named, documented, one-line delegation.
type apiAdapter struct {
    api core.DriverAPI
}

func (a *apiAdapter) ConnectionOpened(id uint32, user, addr string) error {
    return a.api.ConnectionOpened(id, user, addr)
}
func (a *apiAdapter) ConnectionClosed(id uint32, user, addr string) {
    a.api.ConnectionClosed(id, user, addr)
}
func (a *apiAdapter) TransactionBegun(connID uint32, readOnly bool) error {
    return a.api.TransactionBegun(connID, readOnly)
}
func (a *apiAdapter) TransactionCommitted(connID uint32) error {
    return a.api.TransactionCommitted(connID)
}
func (a *apiAdapter) TransactionRolledBack(connID uint32, savepoint string) {
    a.api.TransactionRolledBack(connID, savepoint)
}
func (a *apiAdapter) QueryReceived(connID uint32, query, database string) (string, error) {
    return a.api.QueryReceived(connID, query, database)
}
func (a *apiAdapter) QueryCompleted(connID uint32, query string, rowsAffected int64, err error) {
    a.api.QueryCompleted(connID, query, rowsAffected, err)
}
func (a *apiAdapter) RowsFetched(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
    return a.api.RecordsSource(connID, table, records)
}
func (a *apiAdapter) RowsReady(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
    return a.api.RecordsMerged(connID, table, records)
}
func (a *apiAdapter) RowInserted(connID uint32, table string, record map[string]any) (map[string]any, error) {
    return a.api.RecordInserted(connID, table, record)
}
func (a *apiAdapter) RowUpdated(connID uint32, table string, old, new map[string]any) (map[string]any, error) {
    return a.api.RecordUpdated(connID, table, old, new)
}
func (a *apiAdapter) RowDeleted(connID uint32, table string, record map[string]any) error {
    return a.api.RecordDeleted(connID, table, record)
}
func (a *apiAdapter) SchemaLoaded(table string, cols []string, pkCol string) {
    a.api.SchemaLoaded(table, cols, pkCol)
}
func (a *apiAdapter) SchemaInvalidated(table string) {
    a.api.SchemaInvalidated(table)
}

var _ bridge.EventBridge = (*apiAdapter)(nil)
```

`callbacks.go` is deleted. `validateCallbacks` is deleted. The compiler replaces
the reflection-based nil check at the `var _ bridge.EventBridge = (*apiAdapter)(nil)`
line.

Because `core.DriverAPI` now uses `map[string]any` directly, every method in
`apiAdapter` is a pure delegation — no type conversion, no wrapping. The adapter
is mechanical and verifiable at a glance. If `core.DriverAPI` ever changes a
signature, the compiler immediately surfaces the mismatch in `apiAdapter`.

---

## 7. What Happens to Each Current File

Note: `vdb-mysql-driver` currently imports `github.com/AnqorDX/vdb-core/types`
for the `types.Record` type used in `driver_api.go` result validators and in the
module's own type assertions. Once `types.Record` is eliminated from vdb-core,
this import disappears automatically — `map[string]any` needs no import.

| Current file | Disposition |
|---|---|
| `driver.go` | Retained. `Driver`, `Config`, `NewDriver` (constructor rewritten to accept `core.DriverAPI` directly; all wiring moved here), `Run`, `Stop`, `noopServerEventListener`, `apiAdapter` (new). `SetDriverAPI` is **deleted**. |
| `callbacks.go` | **Deleted.** `callbacks` struct replaced by `bridge.EventBridge`. `validateCallbacks` replaced by compile-time interface check. |
| `session.go` | **Deleted.** Content dispersed: `vdbSession` → `internal/gms/session/session.go`, `vdbTransaction` → `internal/gms/session/transaction.go`, `buildSessionBuilder` → `internal/gms/session/factory.go`, `queryInterceptor` → `internal/gms/query/interceptor.go`, `vdbDatabaseProvider` → `internal/gms/engine/provider.go`, `vdbDatabase` → `internal/gms/engine/database.go`, `vdbTable` + `singlePartition*` + `fetchFromSource` → `internal/gms/engine/table.go`, `vdbRowWriter` → `internal/gms/engine/writer.go`, `connIDFromCtx` + `databaseFromContext` → `internal/gms/query/interceptor.go`. |
| `rows.go` | **Deleted.** `rowProvider` interface + `gmsRowProvider` → `internal/gms/rows/provider.go`, conversion helpers → `internal/gms/rows/converter.go`, `vdbRowIter` → `internal/gms/rows/iter.go`. |
| `schema.go` | **Deleted.** `schemaProvider` interface + `sqlSchemaProvider` → `internal/schema/provider.go`, `wrappedSchemaProvider` → `internal/schema/notify.go`, `toGMSSchema` → `internal/schema/gms.go`. |
| `testutil_test.go` | **Replaced.** `fullCallbacks()` and `stubSchemaProvider` replaced by `stubEventBridge` struct (implements `bridge.EventBridge`) in `testutil_test.go`. `stubEventBridge` includes `SchemaInvalidated` — the method that `callbacks` was silently dropping. |

---

## 8. Testing Strategy

### Tier 1: Pure unit tests (no network, no engine)

All tier-1 tests use `map[string]any` throughout — no `types.Record` import,
no vdb-core import. This is one of the structural benefits of `bridge.EventBridge`
being self-contained.

| Package | Subject |
|---------|---------|
| `internal/bridge` | Compile-time check that `stubEventBridge` satisfies `EventBridge` |
| `internal/schema` | `SQLProvider.GetSchema` via `go-sqlmock`; `NotifyingProvider` calls listener |
| `internal/gms/rows` | `RowToMap`, `MapToRow`, `SchemaColumns` table-driven; panic on mismatch |
| `internal/gms/session` | `Session.StartTransaction` fires `TransactionBegun`; `SessionEnd` fires `ConnectionClosed`; `RollbackToSavepoint` passes savepoint name |
| `internal/gms/query` | `Interceptor.Query` fires `QueryReceived` before chain; fires `QueryCompleted` after; rewritten query reaches chain |

### Tier 2: Integration tests (root package, stub EventBridge, no network)

`driver_test.go` verifies: `New()` produces a valid `Driver`; `Run()` before
`SetDriverAPI` returns a descriptive error; `Stop()` before `Run()` is a no-op.

### Shared test helpers

`testutil_test.go` (root package) provides `stubEventBridge` — a struct that
records every call and returns configurable values. All root-package tests use this
instead of `fullCallbacks()`.

Each internal package's `_test.go` defines its own local stub for
`bridge.EventBridge` using a minimal implementation with only the methods it needs.
Go interfaces make this trivial: a struct that implements only the called methods
satisfies the interface in tests that only exercise those paths.

---

## 9. Migration Steps

Perform one step at a time. Run `go test ./... -race` after each step. Never let
the build go red between commits.

| Step | Action |
|------|--------|
| 0 | **Prerequisite:** Confirm vdb-core has completed Steps 1 and 1a from its refactor plan: `types.Record` eliminated from `DriverAPI`; `DriverReceiver` removed; `app.DriverAPI()` getter added. Verify `vdb-mysql-driver` still builds — it should require zero changes at this point since it already uses `map[string]any` internally and `SetDriverAPI` is still present. |
| 1 | Create `internal/bridge/events.go` with `EventBridge` interface (including `SchemaInvalidated`). Write `events_test.go` with a compile-time satisfaction check. Build green. |
| 2 | Create `internal/schema/provider.go`, `notify.go`, `gms.go`. Move `schemaProvider`, `sqlSchemaProvider`, `wrappedSchemaProvider`, `toGMSSchema`. Update `schema.go` to import and re-use the internal package. Build green. Run `schema_test.go` — it should still pass unchanged. |
| 3 | Create `internal/gms/rows/provider.go`, `converter.go`, `iter.go`. Move `rowProvider`, `gmsRowProvider`, helpers, `vdbRowIter`. Update `rows.go` to delegate. Build green. Run `rows_test.go` — unchanged. |
| 4 | Create `internal/gms/session/transaction.go`, `session.go`, `factory.go`. Move `vdbTransaction`, `vdbSession`, `buildSessionBuilder`. Update `session.go` to delegate. Build green. |
| 5 | Create `internal/gms/query/interceptor.go`. Move `queryInterceptor`, `connIDFromCtx`, `databaseFromContext`. Update `session.go` to delegate. Build green. |
| 6 | Create `internal/gms/engine/provider.go`, `database.go`, `table.go`, `writer.go`. Move all remaining GMS hierarchy types from `session.go`. Update `session.go` to delegate. Build green. |
| 7 | Add `apiAdapter` to `driver.go` with all 14 methods including `SchemaInvalidated`. Add `var _ bridge.EventBridge = (*apiAdapter)(nil)`. Build green. |
| 8 | Rewrite `driver.go`: replace `New()` + `SetDriverAPI()` with `NewDriver(cfg Config, api core.DriverAPI) *Driver` that performs all wiring unconditionally. Delete `callbacks.go` and `validateCallbacks`. Update `testutil_test.go` with `stubEventBridge`. Update `driver_test.go` to call `NewDriver`. Build green. All tests pass. |
| 9 | Delete the now-empty `session.go`, `rows.go`, `schema.go`. Build green. |
| 10 | Write unit tests for each internal package. Verify coverage. |
| 11 | Audit all file line counts. Any file above 200 lines gets another targeted split. |

---

## 10. Invariants After Refactor

| # | Invariant | Verify with |
|---|-----------|-------------|
| 1 | `vdb-core` is imported only by `driver.go` | `grep -r '"github.com/AnqorDX/vdb-core"' internal/ --include="*.go"` returns nothing |
| 2 | `internal/bridge` imports nothing from this module | Inspect `bridge/events.go` — stdlib only |
| 3 | `callbacks.go` does not exist | `ls callbacks.go` returns not found |
| 4 | `validateCallbacks` does not exist | `grep -r 'validateCallbacks' . --include="*.go"` returns nothing |
| 5 | `apiAdapter` satisfies `bridge.EventBridge` at compile time | `var _ bridge.EventBridge = (*apiAdapter)(nil)` in `driver.go` |
| 6 | `apiAdapter` has exactly 14 methods — one per `bridge.EventBridge` method | Count methods in `driver.go` |
| 7 | `SetDriverAPI` does not exist anywhere in the module | `grep -r 'SetDriverAPI' . --include="*.go"` returns nothing |
| 8 | `Driver` does not implement `core.DriverReceiver` | `grep -r 'DriverReceiver' . --include="*.go"` returns nothing |
| 9 | `NewDriver` accepts `core.DriverAPI` as its second parameter | Manual — inspect `driver.go` constructor signature |
| 10 | `driver.go` contains no anonymous function literals in the wiring path | Manual — `NewDriver` is purely constructors and assignments |
| 11 | `vdb-mysql-driver` does not import `github.com/AnqorDX/vdb-core/types` | `grep -r '"github.com/AnqorDX/vdb-core/types"' . --include="*.go"` returns nothing |
| 12 | `go test ./... -race` passes at every step | CI |
| 13 | No file exceeds 200 lines | `awk 'END{if(NR>200)print FILENAME,NR}' **/*.go` |