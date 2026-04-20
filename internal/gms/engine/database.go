package engine

import (
	"database/sql"

	"github.com/virtual-db/mysql-driver/internal/gms/rows"
	intschema "github.com/virtual-db/mysql-driver/internal/schema"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// Database implements gmssql.Database, serving a single logical schema backed
// by a schemaProvider and a rowProvider.
type Database struct {
	name     string
	rows     rows.Provider
	schema   intschema.Provider
	db       *sql.DB
	provider *DatabaseProvider // for shared auto-increment registry
}

var _ gmssql.Database = (*Database)(nil)

// Name returns the database name.
func (d *Database) Name() string { return d.name }

// GetTableInsensitive looks up the table schema via the schema provider. If
// found it constructs and returns a Table backed by the full column descriptors
// returned by GetSchema. Any schema error is treated as "table does not exist"
// from GMS's perspective (returns nil, false, nil).
func (d *Database) GetTableInsensitive(
	ctx *gmssql.Context,
	tblName string,
) (gmssql.Table, bool, error) {
	if d.schema == nil {
		return nil, false, nil
	}
	cols, _, err := d.schema.GetSchema(tblName)
	if err != nil {
		return nil, false, nil
	}
	gmsSchema := intschema.ToGMSSchema(tblName, cols)
	var ai *autoIncrState
	if d.provider != nil {
		ai = d.provider.getOrCreateAI(tblName)
	}
	return &Table{
		name:   tblName,
		dbName: d.name,
		schema: gmsSchema,
		rows:   d.rows,
		db:     d.db,
		ai:     ai,
	}, true, nil
}

// GetTableNames returns nil. GMS uses this for SHOW TABLES; the source
// database's information_schema handles those queries directly.
func (d *Database) GetTableNames(_ *gmssql.Context) ([]string, error) {
	return nil, nil
}
