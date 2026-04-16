# vdb-mysql-driver — Testing Plan

## Status: Phase 1 DRAFT — pending implementation | Phase 2 DRAFT — pending implementation

---

## Overview

This plan has two phases.

**Phase 1** — Migrate existing tests to their correct packages and add tests for
packages that have none. All test files land in source directories during this
phase (the standard location before the restructure).

**Phase 2** — Move all test files into `tests/` subdirectories, enforce the
external-package convention, and apply concern-based file splits.

Phase 2 depends on Phase 1 being complete and green.

---

## Core Conventions

### Test perspective

Tests are written from the *client's* perspective. A test only observes what
a caller of the package can observe: exported types, exported functions, and
the side effects they produce on their dependencies. Internal mechanisms are
invisible and irrelevant. A test that relies on knowing which internal function
was called, or which string key was registered on a stub, is not testing
behaviour — it is testing implementation.

### External test package with dot import

Every file in a `tests/` directory declares `package X_test`. The package
under test is imported with a dot import so exported names are unqualified.
All other imports keep their package qualifiers.

```go
package schema_test

import (
    . "github.com/AnqorDX/vdb-mysql-driver/internal/schema"
    "github.com/AnqorDX/vdb-mysql-driver/internal/bridge"
)

// NewSQLProvider(...) not schema.NewSQLProvider(...)
// bridge.EventBridge is qualified because bridge is a dependency, not the SUT
```

### No stubRegistrar

The vdb-core handler tests previously used a `stubRegistrar` to verify that
`Register` attached functions to specific string keys. That tests internal
wiring. The correct test constructs a real pipeline, calls `Register`, drives
the pipeline with a payload, and asserts the observable outcome on a concrete
dependency. This module's handler tests follow the same principle.

### No export_test.go

If a test requires exposing an unexported symbol, the test is in the wrong
place. Every test in this plan exercises only the public API.

### The 200-line marker is a smell indicator

A file that exceeds 200 lines is a prompt to ask whether it owns more than one
concern. If the answer is no, the file may stay. If the answer is yes, the
split boundary is the concern boundary, not the line boundary. Files are never
split mechanically to hit a number.

---

## Go Toolchain Note: `tests` directories

Tests live in `tests/` subdirectories. Because `tests/` contains only
`*_test.go` files, `go build ./...` is unaffected. `go test ./...` visits them
automatically — no Makefile required:

```
go test -race -count=1 ./...
```

---

## Current State

Root test files (all `package driver`):

| File | Lines | What it actually tests |
|---|---|---|
| `schema_test.go` | 439 | `internal/schema` — SQLProvider, NotifyingProvider, ToGMSSchema |
| `rows_test.go` | 282 | `internal/gms/rows` — converters and GMSProvider callbacks |
| `testutil_test.go` | 220 | Shared stubs used by the above two files |
| `driver_test.go` | 102 | Root package — NewDriver construction, Stop before Run |

Internal packages with no tests:
- `internal/bridge`
- `internal/schema`
- `internal/gms/rows`
- `internal/gms/session`
- `internal/gms/query`
- `internal/gms/engine`

---

## Phase 1: Migrate and Add Tests

Phase 1 places test files in source directories using `package X_test`
(external package). This is the intermediate state before Phase 2 moves them
into `tests/` subdirectories.

### Phase 1 file targets

```
internal/bridge/bridge_test.go          ← NEW
internal/schema/schema_test.go          ← MIGRATED from root schema_test.go
internal/gms/rows/rows_test.go          ← MIGRATED from root rows_test.go
internal/gms/session/session_test.go    ← NEW
internal/gms/query/query_test.go        ← NEW
internal/gms/engine/engine_test.go      ← NEW
driver_test.go                          ← KEPT (already correct location)
adapter_test.go                         ← NEW
testutil_test.go                        ← TRIMMED or DELETED
```

### Phase 1 steps

| Step | Action | Verify |
|------|--------|--------|
| 1 | Create `internal/schema/schema_test.go`. Migrate 16 tests from root. Replace root stubs with local ones scoped to schema's actual dependencies. | `go test ./internal/schema/` green |
| 2 | Delete root `schema_test.go`. Remove schema-related stubs from `testutil_test.go`. | `go test ./` green |
| 3 | Create `internal/gms/rows/rows_test.go`. Migrate 14 tests from root. Local stubs only. | `go test ./internal/gms/rows/` green |
| 4 | Delete root `rows_test.go`. Remove rows-related stubs from `testutil_test.go`. | `go test ./` green |
| 5 | Trim or delete `testutil_test.go`. If `stubCoreAPI` is still needed by `driver_test.go`, keep it there inline. | `go test ./` green |
| 6 | Create `internal/bridge/bridge_test.go`. | `go test ./internal/bridge/` green |
| 7 | Create `internal/gms/session/session_test.go`. | `go test ./internal/gms/session/` green |
| 8 | Create `internal/gms/query/query_test.go`. | `go test ./internal/gms/query/` green |
| 9 | Create `internal/gms/engine/engine_test.go`. | `go test ./internal/gms/engine/` green |
| 10 | Create root `adapter_test.go`. | `go test ./` green |

---

## Phase 1 Package Coverage

### `internal/bridge`

**What the client cares about:** `EventBridge` is a 14-method interface. A
client implementing it must satisfy all 14 signatures. The test confirms the
interface shape has not been accidentally broken.

**`bridge_test.go`**

One test: define a local `noopBridge` struct that satisfies `EventBridge` via
a compile-time assertion. The test body is empty. If the interface changes in a
breaking way, this package fails with a precise error rather than a distant
downstream failure.

---

### `internal/schema`

**What the client cares about:** given a database connection, `SQLProvider`
returns the correct column names and primary key for a table. `NotifyingProvider`
wraps a provider and calls a listener when a schema is successfully loaded.
`ToGMSSchema` converts a schema entry into the GMS wire format.

These are three distinct types with distinct responsibilities, but they are all
small enough to live comfortably in one file without mixing concerns. If the
file grows significantly, split at the type boundary: one file per type.

**`schema_test.go`**

*SQLProvider:*
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

Local stubs: a `stubSchemaProvider` with configurable columns/pkCol/error, and
a `stubLoadListener` that satisfies `schema.LoadListener` (one method:
`SchemaLoaded`). `NotifyingProvider` takes a `LoadListener`, not a full
`EventBridge` — the stub needs only that one method.

---

### `internal/gms/rows`

**What the client cares about:** `RowToMap` and `MapToRow` are inverses of each
other and preserve column ordering. `SchemaColumns` extracts column names from
a schema. `GMSProvider` calls the bridge callbacks (`RowsFetched`, `RowsReady`,
`RowInserted`, `RowUpdated`, `RowDeleted`) with the correct arguments and
returns whatever the callback returns.

The converter functions and the provider callbacks are distinct concerns.
`RowToMap`/`MapToRow`/`SchemaColumns` are pure functions — they have no
dependencies. The provider callback tests require a stub `EventBridge`. If the
file exceeds 200 lines during migration, split at this boundary.

**`rows_test.go`**

*Converters (pure functions, no stubs needed):*
- `TestRowToMap_RoundTrip`
- `TestRowToMap_PanicsOnMismatch`
- `TestMapToRow_IsInverseOfRowToMap`
- `TestMapToRow_PreservesColumnOrdering`
- `TestSchemaColumns_ExtractsNames`

*GMSProvider callbacks:*
- `TestFetchRows_InvokesRowsFetchedCallback`
- `TestFetchRows_NilCallback_ReturnsUnmodified`
- `TestFetchRows_CallbackError_IsReturned`
- `TestCommitRows_InvokesRowsReadyCallback`
- `TestCommitRows_NilCallback_ReturnsUnmodified`
- `TestInsertRow_InvokesCallback`
- `TestInsertRow_NilCallback_ReturnsNilNoError`
- `TestUpdateRow_NilCallback_ReturnsNilNoError`
- `TestDeleteRow_NilCallback_ReturnsNil`

Local stubs: a minimal `stubEventBridge` with optional function fields only for
the five methods `GMSProvider` actually calls. The remaining nine `EventBridge`
methods are no-op stubs. This documents which methods the provider uses.

---

### `internal/gms/session`

**What the client cares about:** `Transaction` correctly reports its read-only
status. `Session` correctly calls the bridge when connection and transaction
lifecycle events occur. `Builder` produces a non-nil `Session` bound to the
correct connection ID.

All three types serve the same concern — managing the GMS session lifecycle —
and belong in one file.

**`session_test.go`**

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
- `TestSession_CreateSavepoint_DoesNotError`
- `TestSession_SessionEnd_CallsConnectionClosed`
- `TestSession_CommandBegin_DoesNotError`
- `TestSession_CommandEnd_DoesNotError`

*Builder:*
- `TestBuilder_ReturnsNonNilSession`
- `TestBuilder_Session_HasCorrectConnID`

Local stubs: a `stubEventBridge` with function fields for the methods `Session`
calls: `ConnectionClosed`, `TransactionBegun`, `TransactionCommitted`,
`TransactionRolledBack`. Remaining methods are no-ops.

---

### `internal/gms/query`

**What the client cares about:** `Interceptor` sits on the hot path — every
SQL statement passes through it. The observable behaviour is: `QueryReceived`
is called before the chain runs; `QueryCompleted` is called after; a rewritten
query from `QueryReceived` is what the chain actually receives; an error from
`QueryReceived` short-circuits the chain. `Prepare` and `StmtExecute` delegate
to the chain without modification.

The interception logic (`Query`, `ParsedQuery`, `MultiQuery`) and the
delegation methods (`Prepare`, `StmtExecute`) are different concerns, but both
depend on the same `stubChain`. Keep them in one file unless it grows beyond
200 lines with genuinely separable concerns.

**`query_test.go`**

- `TestInterceptor_Priority_ReturnsZero`
- `TestQuery_CallsQueryReceived`
- `TestQuery_CallsQueryCompleted`
- `TestQuery_QueryReceived_ReturnsRewrite_RewriteUsed`
- `TestQuery_QueryReceived_Error_ShortCircuits`
- `TestParsedQuery_CallsQueryReceived`
- `TestParsedQuery_CallsQueryCompleted`
- `TestParsedQuery_QueryReceived_Error_ShortCircuits`
- `TestMultiQuery_CallsQueryReceived`
- `TestPrepare_DelegatesToChain`
- `TestStmtExecute_DelegatesToChain`
- `TestDatabaseFromContext_WithNonSQLContext_ReturnsEmpty`

Local stubs: a `stubEventBridge` with function fields for `QueryReceived` and
`QueryCompleted`; a `stubChain` implementing `server.Chain` with a configurable
`comQuery` function field that records the query it received.

---

### `internal/gms/engine`

**What the client cares about:** `DatabaseProvider` returns a `Database` for
known names and an error for unknown names. `Database` returns the correct name
and can locate tables case-insensitively.

`Table` and `RowWriter` implement deep GMS interfaces and interact heavily with
the GMS execution engine. Testing them in isolation requires non-trivial GMS
context construction. They are deferred to a future integration test pass.

**`engine_test.go`**

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

Local stubs: minimal `stubSchemaProvider` and `stubRowsProvider` satisfying
their respective interfaces.

---

### Root `adapter_test.go`

**What the client cares about:** `apiAdapter` translates every `EventBridge`
call into the corresponding `core.DriverAPI` call with the correct arguments.
Each of the 14 methods has a name-mapping contract documented in the adapter
comments (e.g. `RowsFetched` → `RecordsSource`). If a rename occurs in
`core.DriverAPI`, these tests catch the mismatch immediately.

**`adapter_test.go`** — 14 tests, one per method:
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

Local stub: a `stubCoreAPI` satisfying `core.DriverAPI` that records which
method was called and with what arguments.

---

## Phase 2: `tests/` Reorganisation

Phase 2 begins after all Phase 1 tests are green under `-race`.

### Phase 2 target structure

```
vdb-mysql-driver/
├── tests/
│   ├── adapter_test.go
│   └── driver_test.go
└── internal/
    ├── bridge/
    │   └── tests/
    │       └── bridge_test.go
    ├── schema/
    │   └── tests/
    │       ├── notify_test.go      ← NotifyingProvider
    │       ├── provider_test.go    ← SQLProvider
    │       └── schema_test.go      ← ToGMSSchema (pure function, no stubs)
    └── gms/
        ├── rows/
        │   └── tests/
        │       ├── converter_test.go   ← pure converter functions
        │       └── provider_test.go    ← GMSProvider callbacks
        ├── session/
        │   └── tests/
        │       └── session_test.go
        ├── query/
        │   └── tests/
        │       └── query_test.go
        └── engine/
            └── tests/
                └── engine_test.go
```

### Phase 2 concern-based splits

**`internal/schema`** — migrate as 3 files, one per type:

- `provider_test.go` — `SQLProvider` behaviour. The 7 SQLProvider tests depend
  on a database connection and a `stubSchemaProvider`. Distinct dependency set.
- `schema_test.go` — `ToGMSSchema` behaviour. A pure function with no stubs.
  Its inputs and outputs are entirely self-contained. Separating it from the
  provider tests reflects that it has no runtime dependencies.
- `notify_test.go` — `NotifyingProvider` behaviour. Depends on `LoadListener`
  but not on a database connection. Different dependency set from SQLProvider.

The split is justified because the three types have different dependencies and
different failure modes, not because the combined file (439 lines) is too long.

**`internal/gms/rows`** — migrate as 2 files:

- `converter_test.go` — `RowToMap`, `MapToRow`, `SchemaColumns`. Pure
  functions. No stubs, no dependencies beyond the package itself.
- `provider_test.go` — `GMSProvider` callbacks. Requires a `stubEventBridge`.
  Different concern (callback invocation and delegation) from data conversion.

The split is justified by the dependency difference: pure functions belong
separately from dependency-driven behaviour tests.

**All other packages** — migrate as-is (one file, no split). Their Phase 1
test files own a single cohesive concern and are expected to remain under 200
lines.

### Phase 2 steps

| Step | Action | Verify |
|------|--------|--------|
| 1 | Create all `tests/` directories | `find . -type d -name "tests"` shows 7 dirs |
| 2 | Split and move `internal/schema` tests | `go test ./internal/schema/tests/` green |
| 3 | Split and move `internal/gms/rows` tests | `go test ./internal/gms/rows/tests/` green |
| 4 | Move `internal/bridge` test | `go test ./internal/bridge/tests/` green |
| 5 | Move `internal/gms/session` test | `go test ./internal/gms/session/tests/` green |
| 6 | Move `internal/gms/query` test | `go test ./internal/gms/query/tests/` green |
| 7 | Move `internal/gms/engine` test | `go test ./internal/gms/engine/tests/` green |
| 8 | Move root `driver_test.go` and `adapter_test.go` to `tests/` | `go test ./tests/` green |
| 9 | Delete all old `*_test.go` files from source directories | `find . -name "*_test.go" ! -path "*/tests/*"` empty |
| 10 | Run full suite | `go test -race -count=1 ./...` all green, zero data races |

---

## What Is Not Tested

- **End-to-end tests** — a real MySQL source, real GMS listener, and real TCP
  connection require infrastructure and belong in a separate `e2e/` directory.
- **`Table` and `RowWriter`** — deferred. These implement deep GMS interfaces
  and require a full GMS execution context to exercise meaningfully.
- **`Run` lifecycle** — binding a real TCP port in a unit test is fragile.
  `TestStop_BeforeRun_ReturnsNil` covers the pre-run safety invariant.

---

## Invariants After Phase 2

| # | Invariant | Verify with |
|---|-----------|-------------|
| 1 | No test file in any source directory | `find . -name "*_test.go" ! -path "*/tests/*"` empty |
| 2 | All test files declare `package X_test` | `grep -rh "^package" --include="*.go" */tests/` all end in `_test` |
| 3 | No `export_test.go` exists | `find . -name "export_test.go"` empty |
| 4 | All tests pass with race detector | `go test -race -count=1 ./...` exits 0 |
| 5 | Every file over 200 lines is reviewed for multiple concerns | Manual — a written concern justification accompanies any such file |