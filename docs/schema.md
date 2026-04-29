# Schema Resolution

Table schemas are resolved on demand by querying `INFORMATION_SCHEMA.COLUMNS` and `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` on the source database. The result is translated into GMS types for SQL planning and used to reconstruct typed rows from raw source data.

Schema is **not cached**. It is re-fetched from `INFORMATION_SCHEMA` on every query planning pass.

`SchemaLoaded` fires on your `DriverAPI` implementation after each successful fetch. `SchemaInvalidated` is available for external callers to signal that cached schema should be discarded, but the driver itself never calls it.

---

## Type Mapping

The following table shows how `INFORMATION_SCHEMA.DATA_TYPE` values are mapped to GMS types.

| MySQL type | GMS type | Notes |
|---|---|---|
| `tinyint`, `smallint`, `mediumint`, `int`, `integer` | `INT` | |
| `bigint` | `BIGINT` | |
| `float` | `FLOAT` | |
| `double`, `real` | `DOUBLE` | |
| `decimal`, `numeric` | `DECIMAL(10,0)` | Precision and scale are not fetched — see limitations |
| `char`, `varchar`, `tinytext`, `text`, `mediumtext`, `longtext` | `LONGTEXT` | |
| `tinyblob`, `mediumblob`, `blob` | `BLOB` | |
| `binary`, `varbinary`, `longblob` | `LONGBLOB` | |
| `date` | `DATE` | |
| `datetime` | `DATETIME` | |
| `timestamp` | `TIMESTAMP` | |
| `time` | `TIME` | |
| `year` | `YEAR` | |
| `json` | `JSON` | |
| `enum`, `set` | `LONGTEXT` | Valid membership is not enforced by GMS |
| `bit` | `BOOLEAN` | Multi-bit columns are collapsed to a single boolean |
| (unknown) | `LONGTEXT` | Fallback for any unrecognised type string |

---

## Limitations

**`DECIMAL` precision and scale** — always mapped as `DECIMAL(10,0)`. `INFORMATION_SCHEMA.NUMERIC_PRECISION` and `NUMERIC_SCALE` are not currently fetched, so any column declared as `DECIMAL(m,d)` will be planned as `DECIMAL(10,0)`. Values are still passed through correctly as strings; only GMS-level type enforcement is affected.

**`ENUM` / `SET` columns** — mapped to `LONGTEXT`. Values are passed through as strings but GMS does not enforce valid set membership or reject out-of-range values.

**`bit` columns** — collapsed to `BOOLEAN`. A `BIT(n)` column where `n > 1` is not representable; only zero/non-zero distinction is preserved.

**Composite primary keys** — only the first column of a `PRIMARY KEY` (by ordinal position) is recognised as `pkCol` in `SchemaLoaded`. Duplicate-key detection and auto-increment tracking are unreliable for tables with composite PKs.

**`SHOW TABLES`** — returns an empty result set because `GetTableNames` returns nil. Table listing works correctly if queried directly against `INFORMATION_SCHEMA.TABLES` on the source database.