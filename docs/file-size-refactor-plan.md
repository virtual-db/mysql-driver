# vdb-mysql-driver — File-Size Refactor Plan

## Status: DRAFT — pending implementation

---

## 1. Invariant

Every production source file in this module must contain **≤ 200 lines** of Go
code. The line limit is a proxy for a deeper rule: **each file owns exactly one
logical concern**. A file that is at the limit but owns three concerns is just
as wrong as one at 300 lines. The split boundary is always a concern boundary,
never an arbitrary line-count boundary.

Test files and generated files are excluded from the invariant.

---

## 2. Current Violation Survey

Measured with `find . -name "*.go" ! -name "*_test.go" | xargs wc -l | sort -rn`:

| File | Lines | Status |
|---|---|---|
| `driver.go` | 276 | ❌ over limit, multiple concerns |
| `internal/gms/query/interceptor.go` | 200 | ❌ at limit, multiple concerns |
| `internal/gms/engine/table.go` | 152 | ✅ |
| `internal/gms/session/session.go` | 89 | ✅ |
| `internal/gms/engine/provider.go` | 71 | ✅ |
| `internal/bridge/events.go` | 71 | ✅ |
| `internal/gms/engine/writer.go` | 61 | ✅ |
| `internal/gms/engine/database.go` | 53 | ✅ |
| `internal/gms/rows/provider.go` | 103 | ✅ |
| `internal/schema/provider.go` | 109 | ✅ |
| `internal/gms/rows/converter.go` | 48 | ✅ |
| `internal/gms/session/factory.go` | 41 | ✅ |
| `internal/schema/notify.go` | 39 | ✅ |
| `internal/gms/rows/iter.go` | 29 | ✅ |
| `internal/schema/gms.go` | 22 | ✅ |
| `internal/gms/session/transaction.go` | 18 | ✅ |

**Two files require an immediate split.**

---

## 3. Violation Analysis

### 3.1 `driver.go` — 276 lines, three concerns in one file

`driver.go` currently owns three logically distinct types that happen to be
in the same file:

| Type | Lines (approx) | Concern |
|---|---|---|
| `Config` + `Driver` + `NewDriver` + `Run` + `Stop` + `mustOpenDB` | ~170 | Public driver lifecycle |
| `apiAdapter` (14 methods) | ~95 | `core.DriverAPI` → `bridge.EventBridge` translation |
| `noopServerEventListener` (4 methods) | ~15 | GMS event listener no-op |

The `apiAdapter` type has nothing to do with the driver's lifecycle.
It is a pure mechanical translation layer from `core.DriverAPI` to
`bridge.EventBridge`. It deserves its own file for the same reason the
`EventBridge` interface has its own file: it is a discrete contract
implementation, not a lifecycle concern.

`noopServerEventListener` is a GMS implementation detail used only in `Run`.
It belongs alongside `apiAdapter` since both are adapter/wrapper types that
support the driver rather than being the driver itself.

---

### 3.2 `internal/gms/query/interceptor.go` — 200 lines, three concerns in one file

`interceptor.go` sits at exactly the line limit. Line count aside, it owns
three distinct concerns that have no reason to share a file:

| Concern | Functions |
|---|---|
| Type definition and contract | `Interceptor` struct, `NewInterceptor`, `Priority` |
| Query-lifecycle interception | `Query`, `ParsedQuery`, `MultiQuery` |
| Delegation-only methods | `Prepare`, `StmtExecute` |
| Context helpers | `connIDFromCtx`, `databaseFromContext` |

The query-lifecycle methods (`Query`, `ParsedQuery`, `MultiQuery`) are the
hottest path in the driver — every SQL statement passes through them. They
account for ~130 lines of non-trivial logic involving query rewrites, callback
wrapping, and row-count accumulation. They belong in their own file where they
can be read, reviewed, and modified in isolation.

`Prepare` and `StmtExecute` are one-liners that delegate directly to the
chain with no additional logic. They are a distinct concern from the
interception methods: they are contract fulfillers, not interceptors.

`connIDFromCtx` and `databaseFromContext` are unexported helpers that deal
entirely with GMS context extraction. They are utility functions, not part of
the interceptor's core logic.

---

## 4. Required Splits

### 4.1 `driver.go` → `driver.go` + `adapter.go`

**`driver.go` after split (~175 lines)**

Retains the public surface of the root package:

```
// Config holds the configuration for the MySQL driver.
type Config struct { ... }

// Driver is the MySQL wire-protocol driver for VirtualDB.
type Driver struct { ... }

var _ core.Server = (*Driver)(nil)

func NewDriver(cfg Config, api core.DriverAPI) *Driver { ... }
func (d *Driver) Run() error                           { ... }
func (d *Driver) Stop() error                          { ... }
func mustOpenDB(dsn string) *sql.DB                    { ... }
```

**`adapter.go` (new file, ~105 lines)**

Owns the two adapter/wrapper types extracted from `driver.go`:

```
// apiAdapter translates every bridge.EventBridge call to the corresponding
// core.DriverAPI call.
type apiAdapter struct { api core.DriverAPI }

var _ bridge.EventBridge = (*apiAdapter)(nil)

func (a *apiAdapter) ConnectionOpened(...)  error          { ... }
func (a *apiAdapter) ConnectionClosed(...)                 { ... }
func (a *apiAdapter) TransactionBegun(...) error           { ... }
func (a *apiAdapter) TransactionCommitted(...) error       { ... }
func (a *apiAdapter) TransactionRolledBack(...)            { ... }
func (a *apiAdapter) QueryReceived(...) (string, error)    { ... }
func (a *apiAdapter) QueryCompleted(...)                   { ... }
func (a *apiAdapter) RowsFetched(...) ([]map[string]any, error) { ... }
func (a *apiAdapter) RowsReady(...)   ([]map[string]any, error) { ... }
func (a *apiAdapter) RowInserted(...) (map[string]any, error)   { ... }
func (a *apiAdapter) RowUpdated(...)  (map[string]any, error)   { ... }
func (a *apiAdapter) RowDeleted(...) error                 { ... }
func (a *apiAdapter) SchemaLoaded(...)                     { ... }
func (a *apiAdapter) SchemaInvalidated(...)                { ... }

// noopServerEventListener satisfies server.ServerEventListener with no-ops.
type noopServerEventListener struct{}

func (noopServerEventListener) ClientConnected()                       {}
func (noopServerEventListener) ClientDisconnected()                    {}
func (noopServerEventListener) QueryStarted()                          {}
func (noopServerEventListener) QueryCompleted(_ bool, _ time.Duration) {}
```

---

### 4.2 `interceptor.go` → `interceptor.go` + `intercept_query.go` + `context.go`

**`interceptor.go` after split (~40 lines)**

Owns only the type definition, constructor, and the two delegation-only
methods that fulfil the `server.Interceptor` contract without adding logic:

```
// Interceptor implements server.Interceptor.
type Interceptor struct { events bridge.EventBridge }

var _ server.Interceptor = (*Interceptor)(nil)

func NewInterceptor(events bridge.EventBridge) *Interceptor { ... }
func (qi *Interceptor) Priority() int                       { return 0 }
func (qi *Interceptor) Prepare(...) ([]*querypb.Field, error) { ... }
func (qi *Interceptor) StmtExecute(...) error               { ... }
```

**`intercept_query.go` (new file, ~130 lines)**

Owns the three methods that implement actual query interception logic —
rewriting, callback wrapping, and row-count accumulation:

```
func (qi *Interceptor) Query(...)       error         { ... }
func (qi *Interceptor) ParsedQuery(...) error         { ... }
func (qi *Interceptor) MultiQuery(...) (string, error) { ... }
```

**`context.go` (new file, ~20 lines)**

Owns GMS context extraction helpers. These functions are pure utilities; they
have no dependency on the `Interceptor` type and would be equally at home in
any other `gms/query` file if one were added in the future:

```
func connIDFromCtx(ctx *gmssql.Context) uint32      { ... }
func databaseFromContext(ctx context.Context) string { ... }
```

---

## 5. Target File Tree After Refactor

```
vdb-mysql-driver/
├── adapter.go                          ← NEW: apiAdapter + noopServerEventListener
├── driver.go                           ← TRIMMED: Config, Driver, NewDriver, Run, Stop, mustOpenDB
├── internal/
│   ├── bridge/
│   │   └── events.go                  ← unchanged (71 lines)
│   ├── gms/
│   │   ├── engine/
│   │   │   ├── database.go            ← unchanged (53 lines)
│   │   │   ├── provider.go            ← unchanged (71 lines)
│   │   │   ├── table.go               ← unchanged (152 lines)
│   │   │   └── writer.go              ← unchanged (61 lines)
│   │   ├── query/
│   │   │   ├── context.go             ← NEW: connIDFromCtx, databaseFromContext
│   │   │   ├── intercept_query.go     ← NEW: Query, ParsedQuery, MultiQuery
│   │   │   └── interceptor.go         ← TRIMMED: struct, NewInterceptor, Priority, Prepare, StmtExecute
│   │   ├── rows/
│   │   │   ├── converter.go           ← unchanged (48 lines)
│   │   │   ├── iter.go                ← unchanged (29 lines)
│   │   │   └── provider.go            ← unchanged (103 lines)
│   │   └── session/
│   │       ├── factory.go             ← unchanged (41 lines)
│   │       ├── session.go             ← unchanged (89 lines)
│   │       └── transaction.go         ← unchanged (18 lines)
│   └── schema/
│       ├── gms.go                     ← unchanged (22 lines)
│       ├── notify.go                  ← unchanged (39 lines)
│       └── provider.go                ← unchanged (109 lines)
```

Files changed: **2** trimmed, **3** created. No other files touched.

---

## 6. Line-Count Projections

| File | Before | After |
|---|---|---|
| `driver.go` | 276 | ~175 |
| `adapter.go` | — (new) | ~105 |
| `internal/gms/query/interceptor.go` | 200 | ~40 |
| `internal/gms/query/intercept_query.go` | — (new) | ~130 |
| `internal/gms/query/context.go` | — (new) | ~20 |

All resulting files are well under 200 lines and each owns a single concern.

---

## 7. Concern Purity After Split

| File | Single concern |
|---|---|
| `driver.go` | ✅ Driver lifecycle only |
| `adapter.go` | ✅ Adapter/wrapper types only |
| `internal/gms/query/interceptor.go` | ✅ Type definition, constructor, delegation-only methods |
| `internal/gms/query/intercept_query.go` | ✅ Query-lifecycle interception logic only |
| `internal/gms/query/context.go` | ✅ GMS context extraction helpers only |

---

## 8. What Does Not Change

- No package boundaries change. `adapter.go` is `package driver`; all three
  query files remain `package query`.
- No exported symbols change signature, name, or semantics.
- No internal packages are touched beyond `gms/query`.
- `go build ./...` must pass without modification after every split.
- `go test ./...` must pass without modification after every split.

---

## 9. Migration Steps

### Phase A — Root package (`driver.go`)

1. Create `adapter.go` in the root package with `package driver`.
2. Move `apiAdapter` struct, its compile-time assertion (`var _ bridge.EventBridge`),
   and all 14 methods verbatim from `driver.go` to `adapter.go`.
3. Move `noopServerEventListener` struct and its 4 methods verbatim to
   `adapter.go`.
4. Add required imports to `adapter.go` (`time`, `core`, `bridge`).
5. Remove the moved declarations from `driver.go`.
6. Remove any imports from `driver.go` that are no longer referenced
   (at minimum `bridge`; verify with `go build`).
7. Run `go build ./...` — zero errors.
8. Run `go test ./...` — zero failures.
9. Verify: `wc -l driver.go adapter.go` — both ≤ 200 lines.

### Phase B — `internal/gms/query/interceptor.go`

1. Create `internal/gms/query/context.go` with `package query`.
2. Move `connIDFromCtx` and `databaseFromContext` verbatim to `context.go`.
3. Move the GMS imports those functions require (`gmssql`) to `context.go`;
   remove from `interceptor.go` if no longer needed there.
4. Create `internal/gms/query/intercept_query.go` with `package query`.
5. Move `Query`, `ParsedQuery`, and `MultiQuery` verbatim to
   `intercept_query.go` along with all imports they exclusively require.
6. Remove the moved declarations from `interceptor.go`.
7. Run `go build ./...` — zero errors.
8. Run `go test ./...` — zero failures.
9. Verify: `wc -l internal/gms/query/*.go` — all ≤ 200 lines.

---

## 10. Invariants After Refactor

- No production file in the module exceeds 200 lines.
- No production file owns more than one logical concern.
- `driver.go` owns exactly one concern: the public `Driver` lifecycle.
- `adapter.go` owns exactly one concern: `core.DriverAPI` → `bridge.EventBridge` translation.
- `interceptor.go` owns exactly one concern: the `Interceptor` type, its
  constructor, and the two delegation-only interface methods.
- `intercept_query.go` owns exactly one concern: query interception logic
  (`Query`, `ParsedQuery`, `MultiQuery`).
- `context.go` owns exactly one concern: GMS context extraction utilities.