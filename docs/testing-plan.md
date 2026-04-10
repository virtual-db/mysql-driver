# vdb-mysql-driver — Testing Plan

## Status: DRAFT — pending implementation

---

## 1. Problem Statement

The module has 1,043 lines of test code across four files, all in `package
driver` at the repository root:

| File | Lines | What it actually tests |
|---|---|---|
| `schema_test.go` | 439 | `internal/schema` — SQLProvider, NotifyingProvider, ToGMSSchema |
| `rows_test.go` | 282 | `internal/gms/rows` — RowToMap, MapToRow, SchemaColumns, FetchRows, CommitRows, InsertRow, UpdateRow, DeleteRow |
| `testutil_test.go` | 220 | Shared stubs: stubSchemaProvider, stubEventBridge, fullBridge, stubCoreAPI |
| `driver_test.go` | 102 | Root package — NewDriver construction, Stop before Run |

Six internal packages have **zero test files**:

- `internal/bridge`
- `internal/gms/engine`
- `internal/gms/query`
- `internal/gms/session`

This arrangement has three structural defects:

**Defect 1 — Wrong package locality.**
`schema_test.go` and `rows_test.go` test internal packages from the root.
When a test in `schema_test.go` fails, the error surface says
`FAIL github.com/AnqorDX/vdb-mysql-driver` rather than
`FAIL .../internal/schema`. The failing package is invisible in the test
output, which makes triage harder as the module grows.

**Defect 2 — Shared stub library.**
`testutil_test.go` is a monolithic stub file shared by all four test files in
the root package. It defines `stubEventBridge`, `stubSchemaProvider`, and
`stubCoreAPI`. This creates invisible coupling between test suites: any change
to `stubEventBridge` to support a new test in `rows_test.go` may silently
affect assertions in `schema_test.go`.

The `bridge.EventBridge` interface has 14 methods. The `stubEventBridge`
struct replicates all 14 with optional function fields. This pattern is
correct and battle-tested. The problem is the location and scope of the stub,
not its structure.

**Defect 3 — Untested packages.**
`internal/gms/query`, `internal/gms/session`, `internal/gms/engine`, and
`internal/bridge` have no tests whatsoever. The query interceptor is the
hottest path in the driver (every SQL statement passes through it), and it
has no unit test.

---

## 2. Design Goals

1. **Each internal package owns its own tests.** A test for `internal/schema`
   lives in `internal/schema/`, uses `package schema_test`, and imports only
   what `internal/schema` exports.

2. **Each package owns its own local stubs.** No shared test helper library.
   If `internal/gms/rows` needs a stub `bridge.EventBridge`, it defines a
   minimal one in `internal/gms/rows/bridge_stub_test.go`. If
   `internal/schema` needs one too, it defines its own. The duplication is
   intentional: it keeps each package's test suite self-contained and prevents
   invisible coupling.

3. **Root package tests cover only the root package.** `driver_test.go` stays
   at the root. It tests `NewDriver`, `Stop`, and the `Config` defaults. It
   does not reach into internal packages.

4. **No mocking framework.** All stubs are handwritten structs with optional
   function fields (the existing `stubEventBridge` pattern is correct and
   should be replicated locally in each package).

5. **Tier separation.** Tests are classified into two tiers:
   - **Tier 1** — pure unit tests, no network, no engine, no GMS context
   - **Tier 2** — integration tests in the root package, stub EventBridge, no
     network

6. **`go test ./...` must pass at every step.** Each migration step is
   independently buildable and testable.

---

## 3. Package Inventory and Test Coverage Targets

### 3.1 `internal/bridge`

**What it owns:** The `EventBridge` interface (14 methods) — the sole contract
between GMS internals and the vdb-core framework.

**Current tests:** None.

**Why test an interface?**
The interface itself cannot be tested, but a compile-time assertion that the
`apiAdapter` in the root package satisfies `EventBridge` is not the same as
verifying the *shape* of the contract. The bridge package should host a test
that confirms `bridge.EventBridge` has the expected method set by using a local
no-op implementation. This acts as a canary: if the interface is accidentally
broken (wrong signature, removed method), `internal/bridge` fails first with a
precise error, not the root package.

**Target tests:**
- `TestEventBridge_CompileTimeAssertionViaLocalNoOp` — define a local
  `noopBridge` that satisfies `EventBridge`; the test body can be empty. The
  compile-time `var _ EventBridge = (*noopBridge)(nil)` assertion is the
  entire value.

---

### 3.2 `internal/schema`

**What it owns:** `Provider` interface, `SQLProvider`, `NotifyingProvider`,
`ToGMSSchema`.

**Current tests:** Covered in `schema_test.go` (root) — 439 lines, 16 test
functions. The coverage is good; only the location is wrong.

**Target files after migration:**

```
internal/schema/
├── provider.go
├── notify.go
├── gms.go
└── schema_test.go    ← NEW (migrated from root)
```

**Target tests (migrated from `schema_test.go`):**

*SQLProvider — GetSchema:*
- `TestGetSchema_ColumnsReturnedInOrdinalOrder`
- `TestGetSchema_PrimaryKeyColumnIdentified`
- `TestGetSchema_MissingTable_ReturnsError`
- `TestGetSchema_NoPrimaryKey_ReturnsEmptyPKColAndNoError`
- `TestGetSchema_DatabaseError_ReturnsWrappedError`
- `TestGetSchema_ConcurrentCallsDoNotRace`
- `TestNewSQLSchemaProvider_ReturnsSchemaProvider`

*ToGMSSchema:*
- `TestToGMSSchema_LengthMatchesColumnCount`
- `TestToGMSSchema_ColumnNamesPreserved`
- `TestToGMSSchema_SourceSetToTableName`
- `TestToGMSSchema_AllColumnsNullable`
- `TestToGMSSchema_EmptyColumns_ReturnsEmptySchema`
- `TestToGMSSchema_TypeIsNonNil`

*NotifyingProvider:*
- `TestWrappedSchemaProvider_OnSuccess_CallsOnLoad`
- `TestWrappedSchemaProvider_OnError_SuppressesOnLoad`
- `TestWrappedSchemaProvider_NilOnLoad_DoesNotPanic`
- `TestWrappedSchemaProvider_ReturnValuesMatchInner`
- `TestWrappedSchemaProvider_NoPrimaryKey_PassedThrough`

**Local stubs required:**
- `stubSchemaProvider` — implements `schema.Provider` with configurable
  `cols`, `pkCol`, `err` fields
- `stubLoadListener` — implements `schema.LoadListener` with an optional
  `schemaLoaded` function field (not the full `bridge.EventBridge` — the
  `NotifyingProvider` only needs `LoadListener`)

**Note on the `LoadListener` stub:**
`NotifyingProvider` takes a `LoadListener`, not a full `EventBridge`. The
local stub needs only one method (`SchemaLoaded`), not all 14. This is a
deliberate simplification: the schema package should not need to know about the
full bridge contract.

---

### 3.3 `internal/gms/rows`

**What it owns:** `Provider` interface, `GMSProvider`, `RowToMap`, `MapToRow`,
`SchemaColumns`, `Iter`.

**Current tests:** Covered in `rows_test.go` (root) — 282 lines, 12 test
functions. Coverage is good; location is wrong.

**Target files after migration:**

```
internal/gms/rows/
├── provider.go
├── converter.go
├── iter.go
└── rows_test.go    ← NEW (migrated from root)
```

**Target tests (migrated from `rows_test.go`):**

*Converter — pure functions:*
- `TestRowToMap_RoundTrip`
- `TestRowToMap_PanicsOnMismatch`
- `TestMapToRow_IsInverseOfRowToMap`
- `TestMapToRow_PreservesColumnOrdering`
- `TestSchemaColumns_ExtractsNames`

*GMSProvider — FetchRows:*
- `TestFetchRows_InvokesRowsFetchedCallback`
- `TestFetchRows_NilCallback_ReturnsUnmodified`
- `TestFetchRows_CallbackError_IsReturned`

*GMSProvider — CommitRows:*
- `TestCommitRows_InvokesRowsReadyCallback`
- `TestCommitRows_NilCallback_ReturnsUnmodified`

*GMSProvider — InsertRow / UpdateRow / DeleteRow:*
- `TestInsertRow_NilCallback_ReturnsNilNoError`
- `TestUpdateRow_NilCallback_ReturnsNilNoError`
- `TestDeleteRow_NilCallback_ReturnsNil`
- `TestInsertRow_InvokesCallback`

**Local stubs required:**
- `stubEventBridge` — a minimal stub with optional function fields for the
  five methods used by `GMSProvider`: `RowsFetched`, `RowsReady`,
  `RowInserted`, `RowUpdated`, `RowDeleted`. The remaining 9 methods may be
  no-ops without function fields since `GMSProvider` never calls them.
- `fullBridge()` helper — returns a `*stubEventBridge` with all five relevant
  fields populated with minimal pass-through functions. Used by tests that
  replace a single field to verify it is called.

---

### 3.4 `internal/gms/session`

**What it owns:** `Session` (GMS session lifecycle), `Transaction` (read-only
wrapper), `Builder` (GMS session factory function).

**Current tests:** None.

**Target files:**

```
internal/gms/session/
├── session.go
├── transaction.go
├── factory.go
└── session_test.go    ← NEW
```

**Target tests:**

*Transaction:*
- `TestTransaction_ReadOnlyTrue_ReportsReadOnly`
- `TestTransaction_ReadOnlyFalse_NotReadOnly`
- `TestTransaction_String_ReadOnly_ContainsReadOnly`
- `TestTransaction_String_ReadWrite_ContainsReadWrite`

*Session lifecycle:*
- `TestSession_StartTransaction_CallsTransactionBegun`
- `TestSession_StartTransaction_Error_IsReturned`
- `TestSession_CommitTransaction_CallsTransactionCommitted`
- `TestSession_Rollback_CallsTransactionRolledBack`
- `TestSession_Rollback_WithSavepoint_PassesSavepoint`
- `TestSession_CreateSavepoint_DoesNotError`
- `TestSession_RollbackToSavepoint_DoesNotError`
- `TestSession_ReleaseSavepoint_DoesNotError`
- `TestSession_CommandBegin_DoesNotError`
- `TestSession_CommandEnd_DoesNotError`
- `TestSession_SessionEnd_CallsConnectionClosed`

*Builder:*
- `TestBuilder_ReturnsNonNilSession`
- `TestBuilder_Session_HasCorrectConnID`
- `TestBuilder_Session_DatabaseMatchesDBName`

**Local stubs required:**
- `stubEventBridge` — needs `ConnectionOpened`, `ConnectionClosed`,
  `TransactionBegun`, `TransactionCommitted`, `TransactionRolledBack` with
  optional function fields. Remaining methods are no-op stubs.
- A GMS `sql.Context` can be constructed with `gmssql.NewEmptyContext()`.

---

### 3.5 `internal/gms/query`

**What it owns:** `Interceptor` — the GMS server interceptor that hooks into
the query lifecycle. Every SQL statement passes through this code path.

**Current tests:** None. This is the highest-priority gap.

**Target files:**

```
internal/gms/query/
├── interceptor.go
└── query_test.go    ← NEW
```

**Target tests:**

*Priority:*
- `TestInterceptor_Priority_ReturnsZero`

*Query — basic lifecycle:*
- `TestQuery_CallsQueryReceived`
- `TestQuery_CallsQueryCompleted`
- `TestQuery_QueryReceived_ReturnsRewrite_RewriteUsed`
- `TestQuery_QueryReceived_ReturnsEmpty_OriginalUsed`
- `TestQuery_QueryReceived_Error_ShortCircuits`
- `TestQuery_RowsAffected_AccumulatedFromResults`

*ParsedQuery — basic lifecycle:*
- `TestParsedQuery_CallsQueryReceived`
- `TestParsedQuery_CallsQueryCompleted`
- `TestParsedQuery_QueryReceived_Error_ShortCircuits`

*MultiQuery — basic lifecycle:*
- `TestMultiQuery_CallsQueryReceived`
- `TestMultiQuery_CallsQueryCompleted`
- `TestMultiQuery_QueryReceived_Error_ShortCircuits`

*Prepare and StmtExecute (delegation only):*
- `TestPrepare_DelegatesToChain`
- `TestStmtExecute_DelegatesToChain`

*Context helpers (unexported — test via package internals):*
- `TestDatabaseFromContext_WithValidSession_ReturnsDatabase`
- `TestDatabaseFromContext_WithNilContext_ReturnsEmpty`
- `TestDatabaseFromContext_WithNonSQLContext_ReturnsEmpty`

**Local stubs required:**
- `stubEventBridge` — needs `QueryReceived`, `QueryCompleted` with optional
  function fields. All other methods are no-ops.
- `stubChain` — implements `server.Chain`. The `ComQuery` method must be
  stubable (configurable result and remainder). This is the hardest stub to
  write in this module because `server.Chain` is a GMS interface. Use a
  minimal struct with function fields.

**Testing note on Chain:**
`server.Chain` is the GMS interceptor chain interface. Constructing a real
chain in a unit test is complex. A `stubChain` with a `comQuery` function
field is the correct approach. The stub's `ComQuery` records the query string
it received (to verify rewrites were applied) and returns configurable rows.

---

### 3.6 `internal/gms/engine`

**What it owns:** `DatabaseProvider`, `Database`, `Table`, `RowWriter`.

These types implement deep GMS interfaces (`sql.DatabaseProvider`,
`sql.Database`, `sql.Table`, `sql.RowInserter` / `sql.RowUpdater` /
`sql.RowDeleter`). Full unit testing requires stubbing significant GMS
plumbing.

**Current tests:** None.

**Target files:**

```
internal/gms/engine/
├── provider.go
├── database.go
├── table.go
├── writer.go
└── engine_test.go    ← NEW
```

**Target tests (Tier 1 — pure/lightweight only):**

*DatabaseProvider:*
- `TestNewDatabaseProvider_ReturnsNonNil`
- `TestDatabaseProvider_Database_KnownName_ReturnsDatabase`
- `TestDatabaseProvider_Database_UnknownName_ReturnsError`
- `TestDatabaseProvider_AllDatabases_ContainsRegisteredDB`
- `TestDatabaseProvider_HasSetCollation_ReturnsFalse`

*Database:*
- `TestDatabase_Name_MatchesConstructorArg`
- `TestDatabase_GetTableInsensitive_KnownTable_ReturnsTable`
- `TestDatabase_GetTableInsensitive_UnknownTable_ReturnsNilNoError`

**Tables and writers are deferred.** `Table` and `RowWriter` satisfy large
GMS interfaces and interact heavily with the GMS execution engine. Testing
them in isolation requires non-trivial GMS context construction. They are
deferred to a future Tier 2 integration test pass, or to a dedicated test
that exercises them through the full GMS engine.

**Local stubs required:**
- `stubSchemaProvider` — implements `schema.Provider`
- `stubRowsProvider` — implements `rows.Provider`
- A real `*sql.DB` backed by `sqlmock` for `Database.GetTableInsensitive`
  (it requires a `*sql.DB` to pass to `Table`).

---

### 3.7 Root package (`package driver`)

**What it owns:** `Config`, `Driver`, `NewDriver`, `Run`, `Stop`,
`mustOpenDB`, `apiAdapter`, `noopServerEventListener`.

**Current tests:** `driver_test.go` — 102 lines, 6 test functions. These
are correctly located and well-scoped. They stay.

**Target tests (existing — no migration needed):**
- `TestNew_DefaultsConnTimeouts`
- `TestNew_ExplicitTimeouts_NotOverridden`
- `TestNewDriver_GMSIsNonNil`
- `TestNew_SrvNilUntilRun`
- `TestStop_BeforeRun_ReturnsNil`
- `TestStop_BeforeRun_DoesNotPanic`

**New tests to add (Tier 2 — apiAdapter):**
After `adapter.go` is split out, add tests that verify the adapter's
translation layer:
- `TestAPIAdapter_ConnectionOpened_DelegatesToCoreAPI`
- `TestAPIAdapter_ConnectionClosed_DelegatesToCoreAPI`
- `TestAPIAdapter_TransactionBegun_DelegatesToCoreAPI`
- `TestAPIAdapter_TransactionCommitted_DelegatesToCoreAPI`
- `TestAPIAdapter_TransactionRolledBack_DelegatesToCoreAPI`
- `TestAPIAdapter_QueryReceived_DelegatesToCoreAPI`
- `TestAPIAdapter_QueryCompleted_DelegatesToCoreAPI`
- `TestAPIAdapter_RowsFetched_MapsToRecordsSource`
- `TestAPIAdapter_RowsReady_MapsToRecordsMerged`
- `TestAPIAdapter_RowInserted_MapsToRecordInserted`
- `TestAPIAdapter_RowUpdated_MapsToRecordUpdated`
- `TestAPIAdapter_RowDeleted_MapsToRecordDeleted`
- `TestAPIAdapter_SchemaLoaded_DelegatesToCoreAPI`
- `TestAPIAdapter_SchemaInvalidated_DelegatesToCoreAPI`

These tests verify the name-mapping contracts documented in the `apiAdapter`
method comments (e.g. `RowsFetched` → `RecordsSource`,
`RowsReady` → `RecordsMerged`). If a rename occurs in `core.DriverAPI`, the
adapter tests catch the mismatch immediately.

**Local stubs required:**
- `stubCoreAPI` (already in `testutil_test.go`) — keep at root, trim to only
  what root-package tests need after internal package tests are in place.

---

## 4. Stub Strategy

### 4.1 Principle: local stubs, no shared library

Every internal package that needs a stub defines its own in a `*_stub_test.go`
or inline in `*_test.go`. No file in `testutil_test.go` is imported by
internal packages (Go's `_test` files are not importable). The goal is to keep
each package self-contained.

### 4.2 The `stubEventBridge` pattern

The existing `stubEventBridge` in `testutil_test.go` is the correct pattern.
Each internal package that needs a bridge stub should replicate it with only
the methods it actually uses having function fields. Methods not used by the
package under test are plain no-ops:

```vdb-mysql-driver/internal/gms/rows/rows_test.go
// stubEventBridge is a local stub for bridge.EventBridge used only by
// gms/rows tests. It exposes function fields for the five methods that
// GMSProvider calls; all others are no-ops.
type stubEventBridge struct {
    rowsFetched func(uint32, string, []map[string]any) ([]map[string]any, error)
    rowsReady   func(uint32, string, []map[string]any) ([]map[string]any, error)
    rowInserted func(uint32, string, map[string]any) (map[string]any, error)
    rowUpdated  func(uint32, string, map[string]any, map[string]any) (map[string]any, error)
    rowDeleted  func(uint32, string, map[string]any) error
}
```

The 9 unused methods are compiled as empty one-liners. This is intentional —
it documents which bridge methods the package actually uses and which it
ignores.

### 4.3 Root `testutil_test.go` after migration

Once internal package tests are in place, `testutil_test.go` at the root
should be trimmed to contain only what root-package tests need:
- `stubCoreAPI` (used by `driver_test.go` for `NewDriver` calls)

`stubSchemaProvider`, `stubEventBridge`, and `fullBridge()` can be deleted
from the root once `schema_test.go` and `rows_test.go` have moved to their
packages with local stubs.

---

## 5. Migration Steps

Each step is independently buildable and testable. Run `go test ./...` after
each step.

### Step 1 — Migrate `internal/schema` tests

1. Create `internal/schema/schema_test.go` with `package schema_test`.
2. Copy all 16 test functions from root `schema_test.go` verbatim.
3. Adjust imports: remove root-package imports; add `schema`, `sqlmock`.
4. Add a local `stubSchemaProvider` and `stubLoadListener` to the new file.
5. Run `go test ./internal/schema/...` — all 16 tests must pass.
6. Delete `schema_test.go` from the root.
7. Remove `stubSchemaProvider` and the schema-specific parts of
   `stubEventBridge` from `testutil_test.go` if they are no longer needed by
   any root test.
8. Run `go test ./...` — must pass.

### Step 2 — Migrate `internal/gms/rows` tests

1. Create `internal/gms/rows/rows_test.go` with `package rows_test`.
2. Copy all 14 test functions from root `rows_test.go` verbatim.
3. Adjust imports: remove root-package imports; add `rows`, `gmssql`.
4. Add a local `stubEventBridge` and `fullBridge()` with only the five
   methods `GMSProvider` calls.
5. Run `go test ./internal/gms/rows/...` — all 14 tests must pass.
6. Delete `rows_test.go` from the root.
7. Remove the rows-specific parts of `stubEventBridge` and `fullBridge()`
   from `testutil_test.go` if they are no longer needed by any root test.
8. Run `go test ./...` — must pass.

### Step 3 — Trim `testutil_test.go`

1. After Steps 1 and 2, audit `testutil_test.go` for unused symbols.
2. If `stubEventBridge` and `fullBridge()` are unused in root tests, delete
   them.
3. If `stubSchemaProvider` is unused in root tests, delete it.
4. `stubCoreAPI` is still used by `driver_test.go` — keep it.
5. If `testutil_test.go` is now empty or trivially small, inline its remaining
   content into `driver_test.go` and delete the file.
6. Run `go test ./...` — must pass.

### Step 4 — Add `internal/bridge` tests

1. Create `internal/bridge/bridge_test.go` with `package bridge_test`.
2. Add a local `noopBridge` struct satisfying `EventBridge` via a compile-time
   assertion.
3. Add `TestEventBridge_CompileTimeAssertionViaLocalNoOp` (body may be empty).
4. Run `go test ./internal/bridge/...` — must pass.

### Step 5 — Add `internal/gms/session` tests

1. Create `internal/gms/session/session_test.go` with `package session_test`.
2. Add a local `stubEventBridge` with function fields for the five session
   lifecycle methods.
3. Write tests for `Transaction`, `Session`, and `Builder` as listed in
   §3.4.
4. Run `go test ./internal/gms/session/...` — all tests must pass.

### Step 6 — Add `internal/gms/query` tests

1. Create `internal/gms/query/query_test.go` with `package query_test`.
2. Add a local `stubEventBridge` with function fields for `QueryReceived` and
   `QueryCompleted`.
3. Add a `stubChain` implementing `server.Chain` with a `comQuery` function
   field that records the query it received.
4. Write tests for `Interceptor` as listed in §3.5.
5. Run `go test ./internal/gms/query/...` — all tests must pass.

### Step 7 — Add `internal/gms/engine` tests

1. Create `internal/gms/engine/engine_test.go` with `package engine_test`.
2. Add local `stubSchemaProvider` and `stubRowsProvider`.
3. Write the `DatabaseProvider` and `Database` tests listed in §3.6.
4. Run `go test ./internal/gms/engine/...` — all tests must pass.

### Step 8 — Add `apiAdapter` tests to root

1. Add an `adapter_test.go` file in the root with `package driver`.
2. Add a local `stubCoreAPI` that records which methods were called and with
   what arguments.
3. Write the 14 `TestAPIAdapter_*` tests listed in §3.7.
4. Run `go test ./...` — all tests must pass.

---

## 6. Package-to-Test-File Mapping (After Migration)

```
vdb-mysql-driver/
├── adapter_test.go               ← NEW: 14 apiAdapter delegation tests
├── driver_test.go                ← EXISTING: kept, no change
├── testutil_test.go              ← TRIMMED or DELETED
├── internal/
│   ├── bridge/
│   │   └── bridge_test.go        ← NEW (Step 4)
│   ├── gms/
│   │   ├── engine/
│   │   │   └── engine_test.go    ← NEW (Step 7)
│   │   ├── query/
│   │   │   └── query_test.go     ← NEW (Step 6)
│   │   ├── rows/
│   │   │   └── rows_test.go      ← MIGRATED from root (Step 2)
│   │   └── session/
│   │       └── session_test.go   ← NEW (Step 5)
│   └── schema/
│       └── schema_test.go        ← MIGRATED from root (Step 1)
```

Files deleted from root: `schema_test.go`, `rows_test.go`.

---

## 7. Test Tier Classification

### Tier 1 — Pure unit tests (no network, no engine)

| Package | File | Dependencies |
|---|---|---|
| `internal/bridge` | `bridge_test.go` | none |
| `internal/schema` | `schema_test.go` | `sqlmock`, `stubSchemaProvider`, `stubLoadListener` |
| `internal/gms/rows` | `rows_test.go` | `gmssql.NewEmptyContext()`, `stubEventBridge` |
| `internal/gms/session` | `session_test.go` | `gmssql.NewEmptyContext()`, `stubEventBridge` |
| `internal/gms/query` | `query_test.go` | `stubChain`, `stubEventBridge` |
| `internal/gms/engine` | `engine_test.go` (partial) | `sqlmock`, `stubSchemaProvider`, `stubRowsProvider` |

### Tier 2 — Root package integration tests (stub EventBridge, no network)

| File | Dependencies |
|---|---|
| `driver_test.go` | `stubCoreAPI`, real `sql.Open` (no TCP needed for construction) |
| `adapter_test.go` | `stubCoreAPI` with call recording |

---

## 8. What Is Not Tested

The following are explicitly out of scope for this plan:

- **End-to-end tests** (real MySQL source, real GMS listener, real TCP
  connection). These require infrastructure and belong in a separate `e2e/`
  directory or CI integration environment.
- **`Table` and `RowWriter` GMS internals** (deferred per §3.6). Testing
  these requires a full GMS execution context. They will be addressed in a
  dedicated future pass.
- **`Run` lifecycle** — binding a real TCP port in a unit test is fragile.
  `TestStop_BeforeRun_ReturnsNil` already covers the safe-before-run
  invariant. The full Run/Stop cycle belongs in an integration test.

---

## 9. Invariants After Migration

- Every production package that can be unit tested has at least one test file
  in the same directory.
- `go test ./...` passes with zero failures.
- `go test ./internal/schema/...` fails only for schema bugs (not rows, not
  driver bugs).
- `go test ./internal/gms/rows/...` fails only for rows bugs.
- `go test ./internal/gms/query/...` fails only for interceptor bugs.
- No test file imports another package's test stubs.
- No shared test helper library exists at the root or any shared path.
- `testutil_test.go` at the root is either deleted or contains only
  `stubCoreAPI`.