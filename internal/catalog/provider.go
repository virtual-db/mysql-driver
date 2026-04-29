package catalog

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	gmssql "github.com/dolthub/go-mysql-server/sql"
	rowspkg "github.com/virtual-db/mysql-driver/internal/rows"
	schemapkg "github.com/virtual-db/mysql-driver/internal/schema"
)

// DatabaseProvider implements gmssql.DatabaseProvider and serves as the root
// of the GMS engine's object graph. It holds the single logical database that
// the driver exposes to connecting clients.
type DatabaseProvider struct {
	dbName string
	rows   rowspkg.Provider
	schema schemapkg.Provider
	db     *sql.DB

	// aiMu guards aiTables. The auto-increment registry lives here so that the
	// counter for each table is shared across all Table instances (which are
	// created fresh on every GetTableInsensitive call). Without this, a second
	// INSERT into the same table would re-seed the counter from the source and
	// re-assign an id already consumed by an earlier delta insert.
	aiMu     sync.Mutex
	aiTables map[string]*autoIncrState
}

// getOrCreateAI returns the shared autoIncrState for the given table, creating
// it if this is the first time the table has been seen.
func (p *DatabaseProvider) getOrCreateAI(table string) *autoIncrState {
	p.aiMu.Lock()
	defer p.aiMu.Unlock()
	if p.aiTables == nil {
		p.aiTables = make(map[string]*autoIncrState)
	}
	ai, ok := p.aiTables[table]
	if !ok {
		ai = &autoIncrState{}
		p.aiTables[table] = ai
	}
	return ai
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
		name:     p.dbName,
		rows:     p.rows,
		schema:   p.schema,
		db:       p.db,
		provider: p,
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
			name:     p.dbName,
			rows:     p.rows,
			schema:   p.schema,
			db:       p.db,
			provider: p,
		},
	}
}
