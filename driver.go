// Package driver is the MySQL wire-protocol driver for VirtualDB.
//
// It is the sole importable package from github.com/AnqorDX/vdb-mysql-driver.
// Consuming modules must not import any internal sub-package or any package
// from github.com/dolthub/go-mysql-server directly.
//
// The Driver type satisfies:
//   - core.DriverReceiver (via SetDriverAPI)
//   - core.Server         (via Run, Stop)
//
// Construction via New performs no I/O and starts no goroutines. All lifecycle
// management occurs inside Run, which the framework invokes via app.Run().
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
)

// Config holds the configuration for the MySQL driver.
// All fields are read during New and SetDriverAPI; none are read after Run starts.
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
// Satisfies core.DriverReceiver via SetDriverAPI and core.Server via Run and Stop.
//
// Construct with New. Pass to app.UseDriver. Do not call Run directly;
// the framework calls it via app.Run.
type Driver struct {
	cfg    Config
	api    core.DriverAPI // set by SetDriverAPI; nil until then
	cbs    callbacks      // wired in SetDriverAPI
	rows   rowProvider    // wired in SetDriverAPI
	schema schemaProvider // wired in SetDriverAPI
	db     *sql.DB        // opened in SetDriverAPI; closed in Stop
	gms    *sqle.Engine   // nil until SetDriverAPI
	srv    *server.Server // nil until Run
	mu     sync.Mutex     // guards srv
}

// Compile-time interface assertions.
var _ core.DriverReceiver = (*Driver)(nil)
var _ core.Server = (*Driver)(nil)

// New constructs a Driver from cfg, defaulting zero-value timeouts to one minute.
// No I/O occurs and no goroutines are started.
func New(cfg Config) *Driver {
	if cfg.ConnReadTimeout == 0 {
		cfg.ConnReadTimeout = time.Minute
	}
	if cfg.ConnWriteTimeout == 0 {
		cfg.ConnWriteTimeout = time.Minute
	}
	return &Driver{cfg: cfg}
}

// SetDriverAPI satisfies core.DriverReceiver. It opens the source database
// connection, builds the internal GMS engine, and wires all twelve callbacks
// to the corresponding core.DriverAPI methods.
//
// After SetDriverAPI returns, d.gms is non-nil and ready to serve queries.
// Run must still be called to bind the network port.
//
// Panics if the source DSN is malformed — a misconfigured DSN means the process
// cannot function at all, and failing at wiring time is the correct behaviour.
func (d *Driver) SetDriverAPI(api core.DriverAPI) {
	d.api = api

	// sql.Open validates DSN format but does not open a TCP connection.
	sourceDB, err := sql.Open("mysql", d.cfg.SourceDSN)
	if err != nil {
		panic(fmt.Sprintf("driver: failed to open source DB: %v", err))
	}
	d.db = sourceDB

	// Wrap the raw schema provider so each successful GetSchema also calls
	// api.SchemaLoaded. The internal GMS layer has no knowledge of core.DriverAPI.
	rawSchema := newSQLSchemaProvider(sourceDB, d.cfg.DBName)
	wrapped := &wrappedSchemaProvider{
		inner: rawSchema,
		onLoad: func(table string, cols []string, pkCol string) {
			api.SchemaLoaded(table, cols, pkCol)
		},
	}
	d.schema = wrapped

	// Wire the twelve callbacks to core.DriverAPI methods.
	//
	// Vocabulary (internal field → DriverAPI method):
	//   rowsFetched → RecordsSource   (rows just read from source DB)
	//   rowsReady   → RecordsMerged   (rows after delta overlay, ready for client)
	//   rowInserted → RecordInserted
	//   rowUpdated  → RecordUpdated
	//   rowDeleted  → RecordDeleted
	cbs := callbacks{
		connectionOpened: func(id uint32, user, addr string) error {
			return api.ConnectionOpened(id, user, addr)
		},
		connectionClosed: func(id uint32, user, addr string) {
			api.ConnectionClosed(id, user, addr)
		},
		transactionBegun: func(connID uint32, readOnly bool) error {
			return api.TransactionBegun(connID, readOnly)
		},
		transactionCommitted: func(connID uint32) error {
			return api.TransactionCommitted(connID)
		},
		transactionRolledBack: func(connID uint32, savepoint string) {
			api.TransactionRolledBack(connID, savepoint)
		},
		queryReceived: func(connID uint32, query, database string) (string, error) {
			return api.QueryReceived(connID, query, database)
		},
		queryCompleted: func(connID uint32, query string, rowsAffected int64, err error) {
			api.QueryCompleted(connID, query, rowsAffected, err)
		},
		rowsFetched: func(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
			return api.RecordsSource(connID, table, records)
		},
		rowsReady: func(connID uint32, table string, records []map[string]any) ([]map[string]any, error) {
			return api.RecordsMerged(connID, table, records)
		},
		rowInserted: func(connID uint32, table string, record map[string]any) (map[string]any, error) {
			return api.RecordInserted(connID, table, record)
		},
		rowUpdated: func(connID uint32, table string, old, new map[string]any) (map[string]any, error) {
			return api.RecordUpdated(connID, table, old, new)
		},
		rowDeleted: func(connID uint32, table string, record map[string]any) error {
			return api.RecordDeleted(connID, table, record)
		},
	}
	validateCallbacks(cbs)
	d.cbs = cbs
	d.rows = newGMSRowProvider(wrapped, cbs)

	d.gms = sqle.NewDefault(&vdbDatabaseProvider{
		dbName: d.cfg.DBName,
		rows:   d.rows,
		schema: d.schema,
		db:     d.db,
	})
}

// Run satisfies core.Server. It binds the MySQL wire-protocol listener and
// blocks until Stop is called or a fatal error occurs.
//
// Run must not be called before SetDriverAPI. The framework guarantees this
// ordering by calling SetDriverAPI synchronously inside UseDriver before
// app.Run() fires the server start handler.
func (d *Driver) Run() error {
	if d.gms == nil {
		return fmt.Errorf("driver: SetDriverAPI must be called before Run")
	}

	chain := &server.InterceptorChain{}
	chain.WithInterceptor(&queryInterceptor{cbs: &d.cbs})

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
		buildSessionBuilder(d.cfg.DBName, &d.cbs),
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
