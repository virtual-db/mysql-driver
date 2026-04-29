# Error Handling & Known Limitations

## Error Handling

| Scenario | Behaviour |
|---|---|
| `cfg.AuthSourceAddr == ""` | **Panics** in `NewDriver` |
| `sql.Open(SourceDSN)` fails | **Panics** in `NewDriver` |
| `net.Listen(cfg.Addr)` fails | `Run()` returns a wrapped error |
| Vitess listener creation fails | `Run()` returns a wrapped error |
| Auth probe — bad credentials | Client receives MySQL error `1045: Access denied for user` |
| Auth probe — timeout or network error | Client receives a connection error |
| `ConnectionOpened` returns error | Session is not established; subsequent queries on the connection return an error |
| `QueryReceived` returns error | Query is rejected; client receives the error |
| `QueryReceived` returns rewritten query | Rewritten SQL is executed; original SQL is passed to `QueryCompleted` |
| `RecordsSource` returns error | Read aborted; client receives the error |
| `RecordsMerged` returns error | Read aborted; client receives the error |
| `RecordInserted` / `RecordUpdated` / `RecordDeleted` returns error | DML aborted; client receives the error |
| GMS duplicate-key error | Sent to the client as MySQL error `1062: Duplicate entry` |
| Unrecognised GMS error | Wrapped via `CastSQLError` and sent with the correct MySQL error code |
| Table not found in `INFORMATION_SCHEMA` | GMS treats the table as not found; client receives a `Table '...' doesn't exist` error |

---

## Known Limitations

**Composite primary keys** — only the first column of a `PRIMARY KEY` is recognised. Duplicate-key detection and auto-increment tracking are unreliable for tables with composite PKs.

**`ENUM` / `SET` columns** — mapped to `LONGTEXT`. Values are passed through as strings but GMS does not enforce valid set membership.

**`DECIMAL` precision and scale** — always mapped as `DECIMAL(10,0)`. The exact precision and scale from `INFORMATION_SCHEMA.NUMERIC_PRECISION` / `NUMERIC_SCALE` are not fetched.

**`SHOW TABLES`** — returns an empty result set. Table listing works if you query `INFORMATION_SCHEMA.TABLES` directly against the source database.

**No predicate pushdown** — all `SELECT` queries fetch every row of the target table from the source database. `WHERE` clauses are evaluated in memory by GMS after rows are returned from `RecordsMerged`.

**Schema is not cached** — `INFORMATION_SCHEMA` is queried on every query planning pass.

**Writes are not persisted by the driver** — `INSERT`, `UPDATE`, and `DELETE` are routed to `DriverAPI`. The driver never writes to the source database.

**`SchemaInvalidated` is never called by the driver** — it is provided for external framework use only.

**`ComPrepare` returns no parameter metadata** — prepared statement clients receive an empty field list for parameters.