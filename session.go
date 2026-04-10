package driver

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
	sqltypes "github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	sqlparser "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/server"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// GMS version note: this file targets github.com/dolthub/go-mysql-server v0.20.x.
// The architecture spec (DRV-002) was written against v0.18.x; several interface
// signatures changed between versions:
//
//   - sql.LifecycleAwareSession introduced SessionEnd() — replaces Close()
//   - sql.TransactionSession.CommitTransaction dropped the db string parameter
//   - sql.TransactionSession.RollbackTransaction renamed to Rollback
//   - server.Middleware/Handler replaced by server.Interceptor / InterceptorChain
//   - server.NewServer now requires a sql.ContextFactory parameter
//
// dolthub/vitess is imported directly because GMS v0.20.x exposes Vitess types
// (vitessmysql.Conn, sqltypes.Result, sqlparser.Statement, querypb.Field) in the
// server.Interceptor interface that queryInterceptor must implement. See go.mod
// for the documented rationale.

// ---------------------------------------------------------------------------
// vdbTransaction — opaque token returned by StartTransaction
// ---------------------------------------------------------------------------

type vdbTransaction struct {
	readOnly bool
}

func (t *vdbTransaction) String() string {
	if t.readOnly {
		return "vdbTransaction(readOnly)"
	}
	return "vdbTransaction(readWrite)"
}

func (t *vdbTransaction) IsReadOnly() bool { return t.readOnly }

// ---------------------------------------------------------------------------
// vdbSession
//
// Embeds *gmssql.BaseSession and implements:
//   - sql.Session                (via embedding)
//   - sql.LifecycleAwareSession  (CommandBegin, CommandEnd, SessionEnd)
//   - sql.TransactionSession     (StartTransaction, CommitTransaction, Rollback,
//     CreateSavepoint, RollbackToSavepoint, ReleaseSavepoint)
// ---------------------------------------------------------------------------

type vdbSession struct {
	*gmssql.BaseSession
	connID uint32
	cbs    *callbacks
}

// CommandBegin satisfies sql.LifecycleAwareSession. Called before every SQL command.
func (s *vdbSession) CommandBegin() error { return nil }

// CommandEnd satisfies sql.LifecycleAwareSession. Called after every SQL command.
func (s *vdbSession) CommandEnd() {}

// SessionEnd satisfies sql.LifecycleAwareSession. Called when the connection
// tears down. Invokes callbacks.connectionClosed.
func (s *vdbSession) SessionEnd() {
	client := s.Client()
	s.cbs.connectionClosed(s.connID, client.User, client.Address)
}

// StartTransaction satisfies sql.TransactionSession. Invokes
// callbacks.transactionBegun. A non-nil error refuses the transaction.
func (s *vdbSession) StartTransaction(
	ctx *gmssql.Context,
	tChar gmssql.TransactionCharacteristic,
) (gmssql.Transaction, error) {
	readOnly := tChar == gmssql.ReadOnly
	if err := s.cbs.transactionBegun(s.connID, readOnly); err != nil {
		return nil, err
	}
	return &vdbTransaction{readOnly: readOnly}, nil
}

// CommitTransaction satisfies sql.TransactionSession. Invokes
// callbacks.transactionCommitted.
func (s *vdbSession) CommitTransaction(
	ctx *gmssql.Context,
	tx gmssql.Transaction,
) error {
	return s.cbs.transactionCommitted(s.connID)
}

// Rollback satisfies sql.TransactionSession. Invokes
// callbacks.transactionRolledBack with an empty savepoint (full rollback).
func (s *vdbSession) Rollback(ctx *gmssql.Context, tx gmssql.Transaction) error {
	s.cbs.transactionRolledBack(s.connID, "")
	return nil
}

// CreateSavepoint satisfies sql.TransactionSession. No-op at the engine level.
func (s *vdbSession) CreateSavepoint(
	ctx *gmssql.Context,
	tx gmssql.Transaction,
	name string,
) error {
	return nil
}

// RollbackToSavepoint satisfies sql.TransactionSession. Invokes
// callbacks.transactionRolledBack with the savepoint name.
func (s *vdbSession) RollbackToSavepoint(
	ctx *gmssql.Context,
	tx gmssql.Transaction,
	name string,
) error {
	s.cbs.transactionRolledBack(s.connID, name)
	return nil
}

// ReleaseSavepoint satisfies sql.TransactionSession. No-op at the engine level.
func (s *vdbSession) ReleaseSavepoint(
	ctx *gmssql.Context,
	tx gmssql.Transaction,
	name string,
) error {
	return nil
}

// ---------------------------------------------------------------------------
// buildSessionBuilder — session factory
// ---------------------------------------------------------------------------

// buildSessionBuilder returns a server.SessionBuilder that:
//  1. Extracts connID, user, and addr from the Vitess connection.
//  2. Calls callbacks.connectionOpened — a non-nil error refuses the connection.
//  3. Sets the current database on the base session.
//  4. Constructs and returns a vdbSession on success.
func buildSessionBuilder(dbName string, cbs *callbacks) server.SessionBuilder {
	return func(ctx context.Context, conn *vitessmysql.Conn, addr string) (gmssql.Session, error) {
		connID := conn.ConnectionID
		user := conn.User

		if err := cbs.connectionOpened(connID, user, addr); err != nil {
			return nil, fmt.Errorf("connection refused: %w", err)
		}

		base := gmssql.NewBaseSessionWithClientServer(
			addr,
			gmssql.Client{User: user, Address: addr},
			connID,
		)
		base.SetCurrentDatabase(dbName)

		return &vdbSession{
			BaseSession: base,
			connID:      connID,
			cbs:         cbs,
		}, nil
	}
}

// ---------------------------------------------------------------------------
// queryInterceptor — implements server.Interceptor
// ---------------------------------------------------------------------------

type queryInterceptor struct {
	cbs *callbacks
}

var _ server.Interceptor = (*queryInterceptor)(nil)

// Priority returns 0. There is only one interceptor in this chain.
func (qi *queryInterceptor) Priority() int { return 0 }

// Query is called for each incoming text query, before it reaches the engine.
//  1. Calls queryReceived — the handler may rewrite the query or return an error.
//  2. Wraps the result callback to accumulate affected-row counts.
//  3. Delegates to the inner chain with the (possibly rewritten) query.
//  4. Calls queryCompleted after the chain returns.
func (qi *queryInterceptor) Query(
	ctx context.Context,
	chain server.Chain,
	conn *vitessmysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) error {
	connID := conn.ConnectionID
	database := databaseFromContext(ctx)

	rewritten, err := qi.cbs.queryReceived(connID, query, database)
	if err != nil {
		return err
	}
	execQuery := query
	if rewritten != "" {
		execQuery = rewritten
	}

	var rowsAffected int64
	wrappedCallback := func(res *sqltypes.Result, more bool) error {
		if res != nil {
			if res.RowsAffected > 0 {
				rowsAffected = int64(res.RowsAffected)
			} else {
				rowsAffected += int64(len(res.Rows))
			}
		}
		return callback(res, more)
	}

	execErr := chain.ComQuery(ctx, conn, execQuery, wrappedCallback)
	qi.cbs.queryCompleted(connID, query, rowsAffected, execErr)
	return execErr
}

// ParsedQuery intercepts a pre-parsed query before it reaches the engine.
// Calls queryReceived (which may rewrite the query) and queryCompleted,
// mirroring the behaviour of Query. Database is not available from the
// ParsedQuery signature; an empty string is passed and the intercept handler
// handles that gracefully (it only updates connState when Database is non-empty).
func (qi *queryInterceptor) ParsedQuery(
	chain server.Chain,
	conn *vitessmysql.Conn,
	query string,
	parsed sqlparser.Statement,
	callback func(*sqltypes.Result, bool) error,
) error {
	connID := conn.ConnectionID

	rewritten, err := qi.cbs.queryReceived(connID, query, "")
	if err != nil {
		return err
	}
	execQuery := query
	if rewritten != "" {
		execQuery = rewritten
	}

	var rowsAffected int64
	wrappedCallback := func(res *sqltypes.Result, more bool) error {
		if res != nil {
			if res.RowsAffected > 0 {
				rowsAffected = int64(res.RowsAffected)
			} else {
				rowsAffected += int64(len(res.Rows))
			}
		}
		return callback(res, more)
	}

	execErr := chain.ComQuery(context.Background(), conn, execQuery, wrappedCallback)
	qi.cbs.queryCompleted(connID, query, rowsAffected, execErr)
	return execErr
}

// MultiQuery intercepts a multi-statement query before it reaches the engine.
// Calls queryReceived for the first statement and queryCompleted after it runs.
func (qi *queryInterceptor) MultiQuery(
	ctx context.Context,
	chain server.Chain,
	conn *vitessmysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	connID := conn.ConnectionID

	rewritten, err := qi.cbs.queryReceived(connID, query, "")
	if err != nil {
		return "", err
	}
	execQuery := query
	if rewritten != "" {
		execQuery = rewritten
	}

	var rowsAffected int64
	wrappedCallback := func(res *sqltypes.Result, more bool) error {
		if res != nil {
			if res.RowsAffected > 0 {
				rowsAffected = int64(res.RowsAffected)
			} else {
				rowsAffected += int64(len(res.Rows))
			}
		}
		return callback(res, more)
	}

	remainder, execErr := chain.ComMultiQuery(ctx, conn, execQuery, wrappedCallback)
	qi.cbs.queryCompleted(connID, query, rowsAffected, execErr)
	return remainder, execErr
}

// Prepare delegates to the chain without invoking any callback.
func (qi *queryInterceptor) Prepare(
	ctx context.Context,
	chain server.Chain,
	conn *vitessmysql.Conn,
	query string,
	prepare *vitessmysql.PrepareData,
) ([]*querypb.Field, error) {
	return chain.ComPrepare(ctx, conn, query, prepare)
}

// StmtExecute delegates to the chain without invoking any callback.
func (qi *queryInterceptor) StmtExecute(
	ctx context.Context,
	chain server.Chain,
	conn *vitessmysql.Conn,
	prepare *vitessmysql.PrepareData,
	callback func(*sqltypes.Result) error,
) error {
	return chain.ComStmtExecute(ctx, conn, prepare, callback)
}

// ---------------------------------------------------------------------------
// vdbDatabaseProvider — implements sql.DatabaseProvider
// ---------------------------------------------------------------------------

type vdbDatabaseProvider struct {
	dbName string
	rows   rowProvider
	schema schemaProvider
	db     *sql.DB
}

var _ gmssql.DatabaseProvider = (*vdbDatabaseProvider)(nil)

func (p *vdbDatabaseProvider) Database(_ *gmssql.Context, name string) (gmssql.Database, error) {
	if !strings.EqualFold(name, p.dbName) {
		return nil, fmt.Errorf("database not found: %s", name)
	}
	return &vdbDatabase{name: p.dbName, rows: p.rows, schema: p.schema, db: p.db}, nil
}

func (p *vdbDatabaseProvider) HasDatabase(_ *gmssql.Context, name string) bool {
	return strings.EqualFold(name, p.dbName)
}

func (p *vdbDatabaseProvider) AllDatabases(_ *gmssql.Context) []gmssql.Database {
	return []gmssql.Database{
		&vdbDatabase{name: p.dbName, rows: p.rows, schema: p.schema, db: p.db},
	}
}

// ---------------------------------------------------------------------------
// vdbDatabase — implements sql.Database
// ---------------------------------------------------------------------------

type vdbDatabase struct {
	name   string
	rows   rowProvider
	schema schemaProvider
	db     *sql.DB
}

var _ gmssql.Database = (*vdbDatabase)(nil)

func (d *vdbDatabase) Name() string { return d.name }

// GetTableInsensitive looks up the table schema via schemaProvider. If found,
// it constructs and returns a vdbTable. Any schema error is treated as "table
// does not exist" from GMS's perspective (returns nil, false, nil).
func (d *vdbDatabase) GetTableInsensitive(
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
	gmsSchema := toGMSSchema(tblName, cols)
	return &vdbTable{
		name:   tblName,
		dbName: d.name,
		schema: gmsSchema,
		rows:   d.rows,
		db:     d.db,
	}, true, nil
}

// GetTableNames returns nil. GMS uses this for SHOW TABLES; the source
// database's information_schema handles those queries directly.
func (d *vdbDatabase) GetTableNames(_ *gmssql.Context) ([]string, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// vdbTable — implements gmssql.Table and write interfaces
// ---------------------------------------------------------------------------

type vdbTable struct {
	name   string
	dbName string // needed for fetchFromSource query
	schema gmssql.Schema
	rows   rowProvider
	db     *sql.DB // source database connection
}

var _ gmssql.Table = (*vdbTable)(nil)
var _ gmssql.InsertableTable = (*vdbTable)(nil)
var _ gmssql.UpdatableTable = (*vdbTable)(nil)
var _ gmssql.DeletableTable = (*vdbTable)(nil)

func (t *vdbTable) Name() string                  { return t.name }
func (t *vdbTable) String() string                { return t.name }
func (t *vdbTable) Schema() gmssql.Schema         { return t.schema }
func (t *vdbTable) Collation() gmssql.CollationID { return gmssql.Collation_Default }

func (t *vdbTable) Partitions(_ *gmssql.Context) (gmssql.PartitionIter, error) {
	return &singlePartitionIter{}, nil
}

// PartitionRows reads rows from the source database, passes them through
// FetchRows and CommitRows on the rowProvider, and returns a RowIter over
// the final record set.
func (t *vdbTable) PartitionRows(ctx *gmssql.Context, _ gmssql.Partition) (gmssql.RowIter, error) {
	rawRows, err := t.fetchFromSource(ctx)
	if err != nil {
		return nil, err
	}

	if t.rows == nil {
		return &vdbRowIter{rows: rawRows}, nil
	}

	merged, err := t.rows.FetchRows(ctx, t.name, rawRows, t.schema)
	if err != nil {
		return nil, err
	}

	final, err := t.rows.CommitRows(ctx, t.name, merged)
	if err != nil {
		return nil, err
	}

	cols := schemaColumns(t.schema)
	sqlRows := make([]gmssql.Row, len(final))
	for i, rec := range final {
		sqlRows[i] = mapToRow(rec, cols)
	}
	return &vdbRowIter{rows: sqlRows}, nil
}

// fetchFromSource queries the source MySQL database for all rows of the table.
// If db is nil (e.g. in unit tests), returns an empty row slice.
func (t *vdbTable) fetchFromSource(ctx *gmssql.Context) ([]gmssql.Row, error) {
	if t.db == nil {
		return nil, nil
	}

	cols := schemaColumns(t.schema)
	if len(cols) == 0 {
		return nil, nil
	}

	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = "`" + c + "`"
	}
	query := "SELECT " + strings.Join(quotedCols, ", ") +
		" FROM `" + t.dbName + "`.`" + t.name + "`"

	dbRows, err := t.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("driver: fetch from %q.%q: %w", t.dbName, t.name, err)
	}
	defer dbRows.Close()

	var result []gmssql.Row
	for dbRows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := dbRows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("driver: scan row from %q.%q: %w", t.dbName, t.name, err)
		}
		row := make(gmssql.Row, len(cols))
		for i, v := range vals {
			row[i] = v
		}
		result = append(result, row)
	}
	if err := dbRows.Err(); err != nil {
		return nil, fmt.Errorf("driver: rows error from %q.%q: %w", t.dbName, t.name, err)
	}
	return result, nil
}

func (t *vdbTable) Inserter(_ *gmssql.Context) gmssql.RowInserter { return &vdbRowWriter{table: t} }
func (t *vdbTable) Updater(_ *gmssql.Context) gmssql.RowUpdater   { return &vdbRowWriter{table: t} }
func (t *vdbTable) Deleter(_ *gmssql.Context) gmssql.RowDeleter   { return &vdbRowWriter{table: t} }

// ---------------------------------------------------------------------------
// vdbRowWriter — write operations delegated to rowProvider
// ---------------------------------------------------------------------------

type vdbRowWriter struct {
	table *vdbTable
}

var _ gmssql.RowInserter = (*vdbRowWriter)(nil)
var _ gmssql.RowUpdater = (*vdbRowWriter)(nil)
var _ gmssql.RowDeleter = (*vdbRowWriter)(nil)

func (w *vdbRowWriter) StatementBegin(_ *gmssql.Context)                {}
func (w *vdbRowWriter) DiscardChanges(_ *gmssql.Context, _ error) error { return nil }
func (w *vdbRowWriter) StatementComplete(_ *gmssql.Context) error       { return nil }
func (w *vdbRowWriter) Close(_ *gmssql.Context) error                   { return nil }

func (w *vdbRowWriter) Insert(ctx *gmssql.Context, row gmssql.Row) error {
	if w.table.rows == nil {
		return nil
	}
	_, err := w.table.rows.InsertRow(ctx, w.table.name, row, w.table.schema)
	return err
}

func (w *vdbRowWriter) Update(ctx *gmssql.Context, old, new gmssql.Row) error {
	if w.table.rows == nil {
		return nil
	}
	_, err := w.table.rows.UpdateRow(ctx, w.table.name, old, new, w.table.schema)
	return err
}

func (w *vdbRowWriter) Delete(ctx *gmssql.Context, row gmssql.Row) error {
	if w.table.rows == nil {
		return nil
	}
	return w.table.rows.DeleteRow(ctx, w.table.name, row, w.table.schema)
}

// ---------------------------------------------------------------------------
// singlePartitionIter — trivial single-partition implementation
// ---------------------------------------------------------------------------

type singlePartition struct{}

func (singlePartition) Key() []byte { return []byte("single") }

type singlePartitionIter struct {
	done bool
}

func (i *singlePartitionIter) Next(_ *gmssql.Context) (gmssql.Partition, error) {
	if i.done {
		return nil, io.EOF
	}
	i.done = true
	return singlePartition{}, nil
}

func (i *singlePartitionIter) Close(_ *gmssql.Context) error { return nil }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// connIDFromCtx extracts the connection ID from a GMS sql.Context.
func connIDFromCtx(ctx *gmssql.Context) uint32 {
	if ctx == nil || ctx.Session == nil {
		return 0
	}
	return ctx.Session.ID()
}

// databaseFromContext extracts the current database name from the sql.Session
// stored in the context. Returns empty string if unavailable.
func databaseFromContext(ctx context.Context) string {
	sqlCtx, ok := ctx.(*gmssql.Context)
	if !ok || sqlCtx == nil {
		return ""
	}
	if sqlCtx.Session == nil {
		return ""
	}
	return sqlCtx.Session.GetCurrentDatabase()
}
