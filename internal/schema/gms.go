package schema

import (
	gmssql "github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
)

// ToGMSSchema converts a column name list to a GMS sql.Schema. All columns
// are typed as LongText; GMS coerces values at execution time. Precise type
// mapping via INFORMATION_SCHEMA.COLUMNS.DATA_TYPE is a later optimisation.
func ToGMSSchema(table string, columns []string) gmssql.Schema {
	schema := make(gmssql.Schema, len(columns))
	for i, col := range columns {
		schema[i] = &gmssql.Column{
			Name:     col,
			Type:     gmstypes.LongText,
			Source:   table,
			Nullable: true,
		}
	}
	return schema
}
