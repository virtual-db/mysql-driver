// Package driver is the MySQL wire-protocol driver for VirtualDB.
//
// It is the sole importable package from github.com/AnqorDX/vdb-mysql-driver.
// Consuming modules must not import any internal sub-package or any package
// from github.com/dolthub/go-mysql-server directly.
//
// The Driver type satisfies core.Server (via Run and Stop).
//
// Construction via NewDriver performs no network I/O and starts no goroutines.
// All lifecycle management occurs inside Run, which the framework invokes via
// app.Run().
package driver

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // registers the mysql driver with database/sql

	core "github.com/AnqorDX/vdb-core"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	gmssql "github.com/dolthub/go-mysql-server/sql"

	"github.com/AnqorDX/vdb-mysql-driver/internal/bridge"
	"github.com/AnqorDX/vdb-mysql-driver/internal/gms/engine"
	"github.com/AnqorDX/vdb-mysql-driver/internal/gms/query"
	"github.com/AnqorDX/vdb-mysql-driver/internal/gms/rows"
	"github.com/AnqorDX/vdb-mysql-driver/internal/gms/session"
	"github.com/AnqorDX/vdb-mysql-driver/internal/schema"
)

// Config holds the configuration for the MySQL driver.
// All fields are read during NewDriver; none are read after Run starts.
type Config struct {
	// Addr is the TCP address on which the MySQL wire-protocol server will listen.
	// Typical value: ":3306".
	Addr string

	// DBName is the logical database name presented to connecting clients.
	// GMS uses this as the default schema name.
	DBName string

	// SourceDSN is the data source name for the upstream MySQL database that
	// VirtualDB proxies. Format: "user:pass@tcp(host:port)/dbname".
	SourceDSN string

	// ConnReadTimeout is the per-connection read timeout for the MySQL listener.
	// Defaults to time.Minute if zero.
	ConnReadTimeout time.Duration

	// ConnWriteTimeout is the per-connection write timeout for the MySQL listener.
	// Defaults to time.Minute if zero.
	ConnWriteTimeout time.Duration
}

// Driver is the MySQL wire-protocol driver for VirtualDB.
// Satisfies core.Server via Run and Stop.
//
// Construct with NewDriver. Pass to app.UseDriver. Do not call Run directly;
// the framework calls it via app.Run.
type Driver struct {
	cfg    Config
	bridge *apiAdapter // adapts core.DriverAPI to bridge.EventBridge
	schema schema.Provider
	rows   rows.Provider
	db     *sql.DB        // opened in NewDriver; closed in Stop
	gms    *sqle.Engine   // non-nil after NewDriver
	srv    *server.Server // nil until Run
	mu     sync.Mutex     // guards srv
}

// Compile-time interface assertions.
var _ core.Server = (*Driver)(nil)

// NewDriver constructs a fully-wired Driver from cfg and api. It applies
// default timeouts, opens the source DB connection, builds all internal
// providers, and wires the GMS engine. No goroutines are started and no
// network connections are established — those happen inside Run.
//
// Panics if the source DSN is malformed — a misconfigured DSN means the
// process cannot function at all, and failing at construction time is the
// correct behaviour.
func NewDriver(cfg Config, api core.DriverAPI) *Driver {
	if cfg.ConnReadTimeout == 0 {
		cfg.ConnReadTimeout = time.Minute
	}
	if cfg.ConnWriteTimeout == 0 {
		cfg.ConnWriteTimeout = time.Minute
	}

	db := mustOpenDB(cfg.SourceDSN)

	adapt := &apiAdapter{api: api}

	// Wrap the raw schema provider so each successful GetSchema also notifies
	// the event bridge (which forwards to api.SchemaLoaded). The internal GMS
	// layer has no knowledge of core.DriverAPI.
	rawSchema := schema.NewSQLProvider(db, cfg.DBName)
	wrappedSchema := schema.NewNotifyingProvider(rawSchema, adapt)

	rowProv := rows.NewGMSProvider(adapt)

	gmsEngine := sqle.NewDefault(engine.NewDatabaseProvider(
		cfg.DBName,
		rowProv,
		wrappedSchema,
		db,
	))

	return &Driver{
		cfg:    cfg,
		bridge: adapt,
		schema: wrappedSchema,
		rows:   rowProv,
		db:     db,
		gms:    gmsEngine,
	}
}

// Run satisfies core.Server. It binds the MySQL wire-protocol listener and
// blocks until Stop is called or a fatal error occurs.
//
// The framework guarantees that NewDriver is called before Run.
func (d *Driver) Run() error {
	chain := &server.InterceptorChain{}
	chain.WithInterceptor(query.NewInterceptor(d.bridge))

	srv, err := server.NewServer(
		server.Config{
			Protocol:         "tcp",
			Address:          d.cfg.Addr,
			ConnReadTimeout:  d.cfg.ConnReadTimeout,
			ConnWriteTimeout: d.cfg.ConnWriteTimeout,
			Options:          []server.Option{chain.Option()},
		},
		d.gms,
		gmssql.NewContext,
		session.Builder(d.cfg.DBName, d.bridge),
		noopServerEventListener{},
	)
	if err != nil {
		return fmt.Errorf("driver: create server: %w", err)
	}

	d.mu.Lock()
	d.srv = srv
	d.mu.Unlock()

	return srv.Start()
}

// Stop satisfies core.Server. It gracefully shuts down the GMS listener and
// drains the source connection pool. Safe to call before Run; returns nil in
// that case.
func (d *Driver) Stop() error {
	d.mu.Lock()
	srv := d.srv
	d.mu.Unlock()

	if srv == nil {
		return nil
	}
	err := srv.Close()
	if d.db != nil {
		d.db.Close()
	}
	return err
}

// ---------------------------------------------------------------------------
// mustOpenDB
// ---------------------------------------------------------------------------

// mustOpenDB opens the source database connection. sql.Open validates the DSN
// format but does not establish a TCP connection. Panics if the DSN is
// malformed, since a misconfigured DSN means the process cannot function.
func mustOpenDB(dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(fmt.Sprintf("driver: failed to open source DB: %v", err))
	}
	return db
}

// ---------------------------------------------------------------------------
// apiAdapter — adapts core.DriverAPI to bridge.EventBridge
// ---------------------------------------------------------------------------

// apiAdapter translates every bridge.EventBridge call to the corresponding
// core.DriverAPI call. It is the single boundary between the driver's internal
// packages and the vdb-core framework.
type apiAdapter struct {
	api core.DriverAPI
}

// Compile-time assertion: apiAdapter satisfies bridge.EventBridge.
var _ bridge.EventBridge = (*apiAdapter)(nil)

func (a *apiAdapter) ConnectionOpened(id uint32, user, addr string) error {
	return a.api.ConnectionOpened(id, user, addr)
}

func (a *apiAdapter) ConnectionClosed(id uint32, user, addr string) {
	a.api.ConnectionClosed(id, user, addr)
}

func (a *apiAdapter) TransactionBegun(connID uint32, readOnly bool) error {
	return a.api.TransactionBegun(connID, readOnly)
}

func (a *apiAdapter) TransactionCommitted(connID uint32) error {
	return a.api.TransactionCommitted(connID)
}

func (a *apiAdapter) TransactionRolledBack(connID uint32, savepoint string) {
	a.api.TransactionRolledBack(connID, savepoint)
}

func (a *apiAdapter) QueryReceived(connID uint32, query, database string) (string, error) {
	return a.api.QueryReceived(connID, query, database)
}

func (a *apiAdapter) QueryCompleted(connID uint32, query string, rowsAffected int64, err error) {
	a.api.QueryCompleted(connID, query, rowsAffected, err)
}

// RowsFetched maps to core.DriverAPI.RecordsSource (rows just read from the
// source DB before the delta overlay is applied).
func (a *apiAdapter) RowsFetched(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
	return a.api.RecordsSource(connID, table, records)
}

// RowsReady maps to core.DriverAPI.RecordsMerged (final rows after the delta
// overlay has been applied, ready to return to the client).
func (a *apiAdapter) RowsReady(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
	return a.api.RecordsMerged(connID, table, records)
}

func (a *apiAdapter) RowInserted(connID uint32, table string, record map[string]any) (map[string]any, error) {
	return a.api.RecordInserted(connID, table, record)
}

func (a *apiAdapter) RowUpdated(connID uint32, table string, old, new map[string]any) (map[string]any, error) {
	return a.api.RecordUpdated(connID, table, old, new)
}

func (a *apiAdapter) RowDeleted(connID uint32, table string, record map[string]any) error {
	return a.api.RecordDeleted(connID, table, record)
}

// SchemaLoaded satisfies both bridge.EventBridge and schema.LoadListener.
// The apiAdapter is passed directly as the NotifyingProvider's listener.
func (a *apiAdapter) SchemaLoaded(table string, cols []string, pkCol string) {
	a.api.SchemaLoaded(table, cols, pkCol)
}

func (a *apiAdapter) SchemaInvalidated(table string) {
	a.api.SchemaInvalidated(table)
}

// ---------------------------------------------------------------------------
// noopServerEventListener
// ---------------------------------------------------------------------------

// noopServerEventListener satisfies server.ServerEventListener with no-ops.
// Passed to server.NewServer to suppress observability side-effects while
// remaining explicit about the contract rather than relying on nil guards.
type noopServerEventListener struct{}

func (noopServerEventListener) ClientConnected()                       {}
func (noopServerEventListener) ClientDisconnected()                    {}
func (noopServerEventListener) QueryStarted()                          {}
func (noopServerEventListener) QueryCompleted(_ bool, _ time.Duration) {}
