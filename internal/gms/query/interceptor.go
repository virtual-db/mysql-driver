// Package query provides the GMS server.Interceptor implementation that hooks
// into the query lifecycle and forwards events to a bridge.EventBridge.
package query

import (
	"context"

	"github.com/AnqorDX/vdb-mysql-driver/internal/bridge"
	vitessmysql "github.com/dolthub/vitess/go/mysql"
	sqltypes "github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	sqlparser "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/server"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// Interceptor implements server.Interceptor to hook into the query lifecycle.
// It calls EventBridge.QueryReceived before each query (allowing rewrites) and
// EventBridge.QueryCompleted after the chain returns.
type Interceptor struct {
	events bridge.EventBridge
}

// Compile-time interface assertion.
var _ server.Interceptor = (*Interceptor)(nil)

// NewInterceptor constructs an Interceptor backed by the given EventBridge.
// events must not be nil.
func NewInterceptor(events bridge.EventBridge) *Interceptor {
	return &Interceptor{events: events}
}

// Priority returns 0. There is only one interceptor in this chain.
func (qi *Interceptor) Priority() int { return 0 }

// Query is called for each incoming text query, before it reaches the engine.
//  1. Calls QueryReceived — the handler may rewrite the query or return an error.
//  2. Wraps the result callback to accumulate affected-row counts.
//  3. Delegates to the inner chain with the (possibly rewritten) query.
//  4. Calls QueryCompleted after the chain returns.
func (qi *Interceptor) Query(
	ctx context.Context,
	chain server.Chain,
	conn *vitessmysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) error {
	connID := conn.ConnectionID
	database := databaseFromContext(ctx)

	rewritten, err := qi.events.QueryReceived(connID, query, database)
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
	qi.events.QueryCompleted(connID, query, rowsAffected, execErr)
	return execErr
}

// ParsedQuery intercepts a pre-parsed query before it reaches the engine.
// Calls QueryReceived (which may rewrite the query) and QueryCompleted,
// mirroring the behaviour of Query. Database is not available from the
// ParsedQuery signature; an empty string is passed to QueryReceived.
func (qi *Interceptor) ParsedQuery(
	chain server.Chain,
	conn *vitessmysql.Conn,
	query string,
	parsed sqlparser.Statement,
	callback func(*sqltypes.Result, bool) error,
) error {
	connID := conn.ConnectionID

	rewritten, err := qi.events.QueryReceived(connID, query, "")
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
	qi.events.QueryCompleted(connID, query, rowsAffected, execErr)
	return execErr
}

// MultiQuery intercepts a multi-statement query before it reaches the engine.
// Calls QueryReceived for the combined query string and QueryCompleted after it runs.
func (qi *Interceptor) MultiQuery(
	ctx context.Context,
	chain server.Chain,
	conn *vitessmysql.Conn,
	query string,
	callback func(*sqltypes.Result, bool) error,
) (string, error) {
	connID := conn.ConnectionID

	rewritten, err := qi.events.QueryReceived(connID, query, "")
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
	qi.events.QueryCompleted(connID, query, rowsAffected, execErr)
	return remainder, execErr
}

// Prepare delegates to the chain without invoking any callback.
func (qi *Interceptor) Prepare(
	ctx context.Context,
	chain server.Chain,
	conn *vitessmysql.Conn,
	query string,
	prepare *vitessmysql.PrepareData,
) ([]*querypb.Field, error) {
	return chain.ComPrepare(ctx, conn, query, prepare)
}

// StmtExecute delegates to the chain without invoking any callback.
func (qi *Interceptor) StmtExecute(
	ctx context.Context,
	chain server.Chain,
	conn *vitessmysql.Conn,
	prepare *vitessmysql.PrepareData,
	callback func(*sqltypes.Result) error,
) error {
	return chain.ComStmtExecute(ctx, conn, prepare, callback)
}

// ---------------------------------------------------------------------------
// Unexported helpers
// ---------------------------------------------------------------------------

// connIDFromCtx extracts the connection ID from a GMS sql.Context.
// Returns 0 if the context or session is nil.
func connIDFromCtx(ctx *gmssql.Context) uint32 {
	if ctx == nil || ctx.Session == nil {
		return 0
	}
	return ctx.Session.ID()
}

// databaseFromContext extracts the current database name from the sql.Session
// stored in the context. Returns an empty string if unavailable.
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
