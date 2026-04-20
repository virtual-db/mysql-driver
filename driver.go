package driver

import (
	"database/sql"
	"fmt"
	"net"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	core "github.com/virtual-db/vdb-core"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	gmssql "github.com/dolthub/go-mysql-server/sql"
	vitessmysql "github.com/dolthub/vitess/go/mysql"

	"github.com/virtual-db/vdb-mysql-driver/internal/gms/engine"
	"github.com/virtual-db/vdb-mysql-driver/internal/gms/query"
	"github.com/virtual-db/vdb-mysql-driver/internal/gms/rows"
	"github.com/virtual-db/vdb-mysql-driver/internal/gms/session"
	"github.com/virtual-db/vdb-mysql-driver/internal/listener"
	"github.com/virtual-db/vdb-mysql-driver/internal/probe"
	"github.com/virtual-db/vdb-mysql-driver/internal/schema"
)

type Config struct {
	Addr             string
	DBName           string
	SourceDSN        string
	AuthSourceAddr   string
	AuthProbeTimeout time.Duration
	ConnReadTimeout  time.Duration
	ConnWriteTimeout time.Duration
}

type Driver struct {
	cfg    Config
	bridge *apiAdapter
	schema schema.Provider
	rows   rows.Provider
	db     *sql.DB
	gms    *sqle.Engine
	srv    *server.Server
	mu     sync.Mutex
}

var _ core.Server = (*Driver)(nil)

func NewDriver(cfg Config, api core.DriverAPI) *Driver {
	if cfg.AuthSourceAddr == "" {
		panic("vdb-mysql-driver: Config.AuthSourceAddr must not be empty")
	}
	if cfg.AuthProbeTimeout == 0 {
		cfg.AuthProbeTimeout = 5 * time.Second
	}
	if cfg.ConnReadTimeout == 0 {
		cfg.ConnReadTimeout = time.Minute
	}
	if cfg.ConnWriteTimeout == 0 {
		cfg.ConnWriteTimeout = time.Minute
	}

	db := mustOpenDB(cfg.SourceDSN)
	adapt := &apiAdapter{api: api}
	rawSchema := schema.NewSQLProvider(db, cfg.DBName)
	wrappedSchema := schema.NewNotifyingProvider(rawSchema, adapt)
	rowProv := rows.NewGMSProvider(adapt)
	gmsEngine := sqle.NewDefault(engine.NewDatabaseProvider(
		cfg.DBName, rowProv, wrappedSchema, db,
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

func (d *Driver) Run() error {
	ln, err := net.Listen("tcp", d.cfg.Addr)
	if err != nil {
		return fmt.Errorf("driver: listen on %s: %w", d.cfg.Addr, err)
	}

	authSourceAddr := d.cfg.AuthSourceAddr
	authProbeTimeout := d.cfg.AuthProbeTimeout
	probeFunc := listener.ProbeFunc(func(client net.Conn) (string, string, uint32, error) {
		return probe.RunAuthProxy(client, authSourceAddr, authProbeTimeout)
	})

	aln := listener.New(ln, probeFunc)

	chain := &server.InterceptorChain{}
	chain.WithInterceptor(query.NewInterceptor(d.bridge))

	srv, err := server.NewServer(
		server.Config{
			Listener:                aln,
			ConnReadTimeout:         d.cfg.ConnReadTimeout,
			ConnWriteTimeout:        d.cfg.ConnWriteTimeout,
			Options:                 []server.Option{chain.Option()},
			ProtocolListenerFactory: passthroughListenerFactory,
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

var passthroughListenerFactory server.ProtocolListenerFunc = func(
	cfg server.Config,
	listenerCfg vitessmysql.ListenerConfig,
	sel server.ServerEventListener,
) (server.ProtocolListener, error) {
	listenerCfg.AuthServer = listener.NewPassthroughAuthServer()
	listenerCfg.AllowClearTextWithoutTLS = true
	return server.MySQLProtocolListenerFactory(cfg, listenerCfg, sel)
}

func mustOpenDB(dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(fmt.Sprintf("driver: failed to open source DB: %v", err))
	}
	return db
}
