# DDL Implementation Plan

## Current State

None of the GMS DDL dispatch interfaces are implemented. Any DDL issued by a
client will fail. The schema delta does not exist. `fetchFromSource` in
`catalog/table.go` has no translation layer and always queries columns by their
current virtual names, which is correct only while no DDL has been issued.

---

## Step 1 — Schema Delta (`catalog/delta.go`)

This is the foundation everything else builds on. Create it first.

```go
// SchemaDelta holds all structural mutations applied to one table within a
// virtual session. Zero value is valid (no mutations applied).
type SchemaDelta struct {
    // AddedCols are columns appended via ADD COLUMN, in the order they were added.
    // Source queries must not include these; their values are synthesised on read.
    AddedCols []gmssql.Column

    // DroppedCols is the set of column names removed via DROP COLUMN.
    // These must be excluded from source SELECT lists and stripped from results.
    DroppedCols map[string]struct{}

    // ModifiedCols maps virtual column name → updated GMS Column descriptor.
    ModifiedCols map[string]gmssql.Column

    // RenamedCols maps virtual name → original source name.
    // Source queries use the source name; results are aliased to the virtual name.
    RenamedCols map[string]string

    // AddedIndexes is the set of index names added via ADD INDEX / CREATE INDEX.
    AddedIndexes map[string]gmssql.IndexDef

    // DroppedIndexes is the set of index names removed via DROP INDEX.
    DroppedIndexes map[string]struct{}

    // SourceName is set when the table was renamed. Source queries use this name.
    // Empty means no rename — use the Table's current name.
    SourceName string

    // Created is true when this table was created entirely through VDB (CREATE TABLE).
    // PartitionRows must return rows from the row delta only; no source query is made.
    Created bool

    // Dropped is true when the table was dropped (DROP TABLE).
    // Any access through GetTableInsensitive must return (nil, false, nil).
    Dropped bool
}
```

`SchemaDelta` lives in `DatabaseProvider`, keyed by current virtual table name:

```go
// in DatabaseProvider
schemaMu    sync.RWMutex
schemaDeltas map[string]*SchemaDelta   // virtual name → delta; nil map = no mutations
```

Helper on `DatabaseProvider`:

```go
func (p *DatabaseProvider) deltaFor(table string) *SchemaDelta {
    p.schemaMu.Lock()
    defer p.schemaMu.Unlock()
    if p.schemaDeltas == nil {
        p.schemaDeltas = make(map[string]*SchemaDelta)
    }
    d, ok := p.schemaDeltas[table]
    if !ok {
        d = &SchemaDelta{
            DroppedCols:    make(map[string]struct{}),
            ModifiedCols:   make(map[string]gmssql.Column),
            RenamedCols:    make(map[string]string),
            AddedIndexes:   make(map[string]gmssql.IndexDef),
            DroppedIndexes: make(map[string]struct{}),
        }
        p.schemaDeltas[table] = d
    }
    return d
}
```

---

## Step 2 — `Table.Schema()` Must Return the Merged View

`Table` currently stores a single `gmssql.Schema` field set at construction
time in `GetTableInsensitive`. That field must reflect the schema delta on every
call, because GMS re-reads `Schema()` between DDL and the next DML.

Add a `delta *SchemaDelta` field to `Table` (populated from `DatabaseProvider`
during `GetTableInsensitive`). Change `Schema()`:

```go
func (t *Table) Schema() gmssql.Schema {
    if t.delta == nil {
        return t.schema // no mutations; fast path
    }
    return applyDeltaToSchema(t.schema, t.delta)
}
```

`applyDeltaToSchema` walks `t.schema`, skips any column in `delta.DroppedCols`,
replaces any column in `delta.ModifiedCols`, renames any column in
`delta.RenamedCols` (virtual name, not source name), then appends
`delta.AddedCols`.

---

## Step 3 — `fetchFromSource` Translation

`fetchFromSource` in `catalog/table.go` builds `SELECT col1, col2 … FROM db.table`.
When a schema delta is present it must adjust both the column list and the table
name before sending the query to the source DB.

```go
func (t *Table) fetchFromSource(ctx *gmssql.Context) ([]gmssql.Row, error) {
    if t.delta != nil && t.delta.Created {
        return nil, nil // no source backing; all rows come from row delta
    }
    if t.delta != nil && t.delta.Dropped {
        return nil, fmt.Errorf("table %q does not exist", t.name)
    }

    // Resolve the name the source DB knows.
    sourceName := t.name
    if t.delta != nil && t.delta.SourceName != "" {
        sourceName = t.delta.SourceName
    }

    // Build column list, skipping added and dropped columns.
    // For renamed columns, emit `originalName AS virtualName`.
    cols := t.sourceColumns() // see below
    if len(cols) == 0 {
        return nil, nil
    }

    query := "SELECT " + strings.Join(cols, ", ") +
        " FROM `" + t.dbName + "`.`" + sourceName + "`"
    // ... rest of fetch unchanged ...
}

// sourceColumns returns the SELECT column fragments to use in fetchFromSource.
// Added columns are excluded. Renamed columns use `src AS virtual` syntax.
// Dropped columns are excluded.
func (t *Table) sourceColumns() []string {
    var frags []string
    for _, col := range t.schema { // t.schema is the base (source) schema
        name := col.Name
        if t.delta == nil {
            frags = append(frags, "`"+name+"`")
            continue
        }
        if _, dropped := t.delta.DroppedCols[name]; dropped {
            continue
        }
        // Check if this is a renamed column (delta key is virtual name; value is source name).
        // We need the reverse: source name → virtual name.
        virtualName := name
        for vn, sn := range t.delta.RenamedCols {
            if sn == name {
                virtualName = vn
                break
            }
        }
        if virtualName != name {
            frags = append(frags, "`"+name+"` AS `"+virtualName+"`")
        } else {
            frags = append(frags, "`"+name+"`")
        }
    }
    return frags
}
```

After `fetchFromSource` returns, `PartitionRows` must synthesise values for
added columns. Insert a `nil` (or the column default) at the correct ordinal
position for each added column into every row returned from the source before
passing them to the row provider:

```go
// in PartitionRows, after fetchFromSource:
rawRows = synthesiseAddedCols(rawRows, t.delta, t.Schema())
```

```go
func synthesiseAddedCols(rows []gmssql.Row, d *SchemaDelta, virtualSchema gmssql.Schema) []gmssql.Row {
    if d == nil || len(d.AddedCols) == 0 {
        return rows
    }
    // Build a map: virtual col name → position in virtualSchema
    // Source rows currently have positions matching the source schema.
    // We need to expand each row to match virtualSchema length,
    // filling added-column positions with nil/default.
    for i, row := range rows {
        expanded := make(gmssql.Row, len(virtualSchema))
        copy(expanded, row) // source columns land at their original indices
        for j, col := range virtualSchema {
            if j >= len(row) {
                // This slot belongs to an added column.
                if col.Default != nil {
                    v, _ := col.Default.Eval(nil, nil)
                    expanded[j] = v
                } else {
                    expanded[j] = nil
                }
            }
        }
        rows[i] = expanded
    }
    return rows
}
```

> **Note on column ordering:** `applyDeltaToSchema` must append `AddedCols` at
> the end of the schema to match the `copy(expanded, row)` assumption above. If
> `ADD COLUMN … AFTER x` support is needed, ordering must be tracked explicitly
> and this synthesis must walk by name rather than index.

---

## Step 4 — Implement the DDL Interfaces on `Table`

Add compile-time assertions alongside the existing ones in `catalog/table.go`:

```go
var _ gmssql.AddColumnTable    = (*Table)(nil)
var _ gmssql.DropColumnTable   = (*Table)(nil)
var _ gmssql.ModifyColumnTable = (*Table)(nil)
var _ gmssql.RenameColumnTable = (*Table)(nil)
var _ gmssql.IndexAlterableTable = (*Table)(nil)
var _ gmssql.RenamableTable    = (*Table)(nil)
var _ gmssql.TruncateableTable = (*Table)(nil)
```

Each method mutates the table's `SchemaDelta` (obtained from `DatabaseProvider`
via `t.delta`, which is already set at `GetTableInsensitive` time).

### `AddColumn`

```go
// gmssql.AddColumnTable
func (t *Table) AddColumn(_ *gmssql.Context, col *gmssql.Column, order *gmssql.ColumnOrder) error {
    col.Source = t.name
    t.delta.AddedCols = append(t.delta.AddedCols, *col)
    return nil
}
```

`*gmssql.ColumnOrder` carries `First bool` and `AfterColumn string`. Record this
in `SchemaDelta.AddedCols` if column ordering is required by downstream callers.
For v1, appending to the end is sufficient — GMS reports the schema as-declared
and clients issuing `ADD COLUMN` rarely depend on positional order in result
sets.

### `DropColumn`

```go
// gmssql.DropColumnTable
func (t *Table) DropColumn(_ *gmssql.Context, colName string) error {
    t.delta.DroppedCols[colName] = struct{}{}
    return nil
}
```

### `ModifyColumn`

```go
// gmssql.ModifyColumnTable
func (t *Table) ModifyColumn(_ *gmssql.Context, colName string, col *gmssql.Column, order *gmssql.ColumnOrder) error {
    col.Source = t.name
    t.delta.ModifiedCols[colName] = *col
    return nil
}
```

### `RenameColumn`

```go
// gmssql.RenameColumnTable
func (t *Table) RenameColumn(_ *gmssql.Context, oldName, newName string) error {
    // If this column was previously added virtually, just rename it in AddedCols.
    for i, c := range t.delta.AddedCols {
        if c.Name == oldName {
            t.delta.AddedCols[i].Name = newName
            return nil
        }
    }
    // It is a source column. Track it so source queries use the old name.
    // Follow the chain: if oldName was already a renamed column, the source
    // name is the value already stored.
    srcName := oldName
    if existing, ok := t.delta.RenamedCols[oldName]; ok {
        srcName = existing
        delete(t.delta.RenamedCols, oldName)
    }
    t.delta.RenamedCols[newName] = srcName
    return nil
}
```

### `CreateIndex` / `DropIndex`

```go
// gmssql.IndexAlterableTable
func (t *Table) CreateIndex(_ *gmssql.Context, idx gmssql.IndexDef) error {
    delete(t.delta.DroppedIndexes, idx.Name())
    t.delta.AddedIndexes[idx.Name()] = idx
    return nil
}

func (t *Table) DropIndex(_ *gmssql.Context, idxName string) error {
    delete(t.delta.AddedIndexes, idxName)
    t.delta.DroppedIndexes[idxName] = struct{}{}
    return nil
}
```

GMS also requires `IndexedTable.GetIndexes()` to be consistent with the schema
for index-based query planning. Add a minimal implementation that returns the
`AddedIndexes` slice; the source DB's real indexes are not surfaced through VDB
in the current design.

### `Rename`

```go
// gmssql.RenamableTable
func (t *Table) Rename(_ *gmssql.Context, newName string, _ gmssql.Database) error {
    // Record the source name if not already renamed.
    if t.delta.SourceName == "" {
        t.delta.SourceName = t.name
    }
    // Move the delta to the new key in DatabaseProvider.
    t.provider.renameDelta(t.name, newName)
    t.name = newName
    return nil
}
```

`DatabaseProvider.renameDelta`:

```go
func (p *DatabaseProvider) renameDelta(oldName, newName string) {
    p.schemaMu.Lock()
    defer p.schemaMu.Unlock()
    if d, ok := p.schemaDeltas[oldName]; ok {
        delete(p.schemaDeltas, oldName)
        p.schemaDeltas[newName] = d
    }
    // Also move the AI state.
    p.aiMu.Lock()
    defer p.aiMu.Unlock()
    if ai, ok := p.aiTables[oldName]; ok {
        delete(p.aiTables, oldName)
        p.aiTables[newName] = ai
    }
}
```

### `Truncate`

```go
// gmssql.TruncateableTable
func (t *Table) Truncate(ctx *gmssql.Context) (int, error) {
    if t.rows == nil {
        return 0, nil
    }
    n, err := t.rows.TruncateRows(ctx, t.name)
    return n, err
}
```

`rows.Provider` must gain a `TruncateRows(ctx, table) (int, error)` method.
The implementation tombstones all source rows and clears all delta inserts for
the table (the exact mechanics depend on the `rows.Provider` implementation, but
the contract is: after `TruncateRows`, `FetchRows` returns an empty slice).

---

## Step 5 — Implement `CreateTable` / `DropTable` on `Database`

### `CreateTable`

```go
// gmssql.TableCreator (embedded in gmssql.Database for GMS ≥ v0.18)
func (d *Database) CreateTable(ctx *gmssql.Context, name string, schema gmssql.PrimaryKeySchema, collation gmssql.CollationID, comment string) error {
    delta := d.provider.deltaFor(name)
    delta.Created = true
    // Seed AddedCols from the declared schema so Schema() returns the correct shape.
    for _, col := range schema.Schema {
        c := *col
        c.Source = name
        delta.AddedCols = append(delta.AddedCols, c)
    }
    return nil
}
```

`GetTableInsensitive` must be updated to check the delta for `Created` tables
and return a `Table` for them even when `schema.Provider.GetSchema` returns an
error (the source DB has no knowledge of this table):

```go
func (d *Database) GetTableInsensitive(ctx *gmssql.Context, tblName string) (gmssql.Table, bool, error) {
    // Check delta first.
    if d.provider != nil {
        if delta := d.provider.peekDelta(tblName); delta != nil {
            if delta.Dropped {
                return nil, false, nil
            }
            if delta.Created {
                ai := d.provider.getOrCreateAI(tblName)
                return &Table{
                    name:     tblName,
                    dbName:   d.name,
                    schema:   gmssql.Schema{}, // Schema() will call applyDeltaToSchema
                    rows:     d.rows,
                    db:       d.db,
                    ai:       ai,
                    delta:    delta,
                    provider: d.provider,
                }, true, nil
            }
        }
    }
    // Original path.
    cols, _, err := d.schema.GetSchema(tblName)
    if err != nil {
        return nil, false, nil
    }
    // ... rest unchanged, but pass delta to Table ...
}
```

### `DropTable`

```go
// gmssql.TableDropper
func (d *Database) DropTable(ctx *gmssql.Context, name string) error {
    delta := d.provider.deltaFor(name)
    delta.Dropped = true
    return nil
}
```

`GetTableInsensitive` already checks `delta.Dropped` above and returns
`(nil, false, nil)`, which is what GMS expects for a non-existent table.

---

## Step 6 — GMS Interface Signature Verification

GMS's exact method signatures for DDL interfaces as of the version in `go.mod`:

| Interface | Method Signature |
|---|---|
| `sql.AddColumnTable` | `AddColumn(*Context, *Column, *ColumnOrder) error` |
| `sql.DropColumnTable` | `DropColumn(*Context, string) error` |
| `sql.ModifyColumnTable` | `ModifyColumn(*Context, string, *Column, *ColumnOrder) error` |
| `sql.RenameColumnTable` | `RenameColumn(*Context, string, string) error` |
| `sql.IndexAlterableTable` | `CreateIndex(*Context, IndexDef) error`, `DropIndex(*Context, string) error` |
| `sql.RenamableTable` | `Rename(*Context, string, Database) error` |
| `sql.TruncateableTable` | `Truncate(*Context) (int, error)` |
| `sql.TableCreator` | `CreateTable(*Context, string, PrimaryKeySchema, CollationID, string) error` |
| `sql.TableDropper` | `DropTable(*Context, string) error` |

Confirm these against the GMS version pinned in `go.mod` before implementing —
GMS has reshuffled some signatures across minor versions. The compile-time
`var _ Interface = (*Type)(nil)` assertions will catch any mismatch immediately.

---

## Step 7 — `rows.Provider` Extension for Created Tables

Tables created via `CREATE TABLE` have no source backing. `PartitionRows` for
these tables must skip `fetchFromSource` entirely and return rows solely from
the row delta. The `Created` flag on `SchemaDelta` already gates this in
`fetchFromSource` (Step 3). No additional changes are needed if the row delta
already handles inserts into previously-unknown tables by table name. Verify
that `rows.Provider.InsertRow` does not require the table to have been
pre-registered.

---

## Step 8 — `applyDeltaToSchema` Implementation Detail

```go
func applyDeltaToSchema(base gmssql.Schema, d *SchemaDelta) gmssql.Schema {
    result := make(gmssql.Schema, 0, len(base)+len(d.AddedCols))
    for _, col := range base {
        if _, dropped := d.DroppedCols[col.Name]; dropped {
            continue
        }
        c := *col // copy to avoid mutating the base
        if modified, ok := d.ModifiedCols[col.Name]; ok {
            c = modified
        }
        // Apply rename: if any renamed entry maps to this source name, use the virtual name.
        for vn, sn := range d.RenamedCols {
            if sn == col.Name {
                c.Name = vn
                break
            }
        }
        result = append(result, &c)
    }
    // AddedCols are appended after source columns. Positional ordering (AFTER x)
    // is not implemented in v1.
    for i := range d.AddedCols {
        c := d.AddedCols[i]
        result = append(result, &c)
    }
    return result
}
```

---

## Affected Files

| File | Change |
|---|---|
| `catalog/delta.go` | **New.** `SchemaDelta` struct + `applyDeltaToSchema`. |
| `catalog/table.go` | Add `delta *SchemaDelta` and `provider *DatabaseProvider` fields. Override `Schema()`. Add `AddColumn`, `DropColumn`, `ModifyColumn`, `RenameColumn`, `CreateIndex`, `DropIndex`, `Rename`, `Truncate`. Update `fetchFromSource` and `PartitionRows` for translation + synthesis. |
| `catalog/database.go` | Add `CreateTable`, `DropTable`. Update `GetTableInsensitive` to check delta first. |
| `catalog/provider.go` | Add `schemaMu`, `schemaDeltas` fields. Add `deltaFor`, `peekDelta`, `renameDelta` helpers. |
| `internal/rows/provider.go` | Add `TruncateRows(ctx, table) (int, error)` to the `Provider` interface. |

---

## Known Gaps Not Addressed Here

- **Column ordering for `ADD COLUMN … AFTER x`** — tracked in `SchemaDelta` but
  not yet reflected in `applyDeltaToSchema` or `synthesiseAddedCols`.
- **`TRUNCATE` row-count return** — `TruncateRows` must return the count of
  tombstoned + cleared rows. The row provider must track this.
- **Index exposure to GMS planner** — `IndexedTable` / `IndexAlterableTable.IndexedTable`
  returning the added indexes so GMS can use them for query planning is a
  follow-on; the DDL itself is correct without it.
- **`RENAME TABLE` across databases** — the `Database` argument to `Rename` is
  ignored in this plan. Cross-database renames are not supported.

