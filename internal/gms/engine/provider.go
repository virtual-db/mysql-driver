package engine

import (
	"database/sql"
	"fmt"
	"strings"

	rowspkg "github.com/AnqorDX/vdb-mysql-driver/internal/gms/rows"
	schemapkg "github.com/AnqorDX/vdb-mysql-driver/internal/schema"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// DatabaseProvider implements gmssql.DatabaseProvider and serves as the root
// of the GMS engine's object graph. It holds the single logical database that
// the driver exposes to connecting clients.
type DatabaseProvider struct {
	dbName string
	rows   rowspkg.Provider
	schema schemapkg.Provider
	db     *sql.DB
}

var _ gmssql.DatabaseProvider = (*DatabaseProvider)(nil)

// NewDatabaseProvider constructs a DatabaseProvider for the given logical
// database name, backed by rowProv and schemaProv, with db as the source
// connection pool.
func NewDatabaseProvider(
	dbName string,
	rowProv rowspkg.Provider,
	schemaProv schemapkg.Provider,
	db *sql.DB,
) *DatabaseProvider {
	return &DatabaseProvider{
		dbName: dbName,
		rows:   rowProv,
		schema: schemaProv,
		db:     db,
	}
}

// Database satisfies gmssql.DatabaseProvider. Returns the single logical
// database when name matches (case-insensitive), otherwise an error.
func (p *DatabaseProvider) Database(_ *gmssql.Context, name string) (gmssql.Database, error) {
	if !strings.EqualFold(name, p.dbName) {
		return nil, fmt.Errorf("database not found: %s", name)
	}
	return &Database{
		name:   p.dbName,
		rows:   p.rows,
		schema: p.schema,
		db:     p.db,
	}, nil
}

// HasDatabase satisfies gmssql.DatabaseProvider.
func (p *DatabaseProvider) HasDatabase(_ *gmssql.Context, name string) bool {
	return strings.EqualFold(name, p.dbName)
}

// AllDatabases satisfies gmssql.DatabaseProvider.
func (p *DatabaseProvider) AllDatabases(_ *gmssql.Context) []gmssql.Database {
	return []gmssql.Database{
		&Database{
			name:   p.dbName,
			rows:   p.rows,
			schema: p.schema,
			db:     p.db,
		},
	}
}
