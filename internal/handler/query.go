package handler

import (
	"context"
	"fmt"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"

	gmssql "github.com/dolthub/go-mysql-server/sql"

	"github.com/virtual-db/mysql-driver/internal/session"
)

// ComQuery satisfies vitessmysql.Handler. Builds a *sql.Context from the
// session, calls engine.Query directly, and spools rows to the callback.
func (h *Handler) ComQuery(
	ctx context.Context,
	c *vitessmysql.Conn,
	query string,
	callback vitessmysql.ResultSpoolFn,
) error {
	sess, ok := c.ClientData.(*session.Session)
	if !ok || sess == nil {
		return fmt.Errorf("no session for connection %d", c.ConnectionID)
	}

	sqlCtx := gmssql.NewContext(ctx, gmssql.WithSession(sess))

	schema, rowIter, _, err := h.engine.Query(sqlCtx, query)
	if err != nil {
		return castError(err)
	}
	defer rowIter.Close(sqlCtx)

	return castError(spoolResult(sqlCtx, schema, rowIter, callback))
}

// ComMultiQuery satisfies vitessmysql.Handler. Executes the first statement
// and returns the remainder. Multi-statement support mirrors ComQuery.
func (h *Handler) ComMultiQuery(
	ctx context.Context,
	c *vitessmysql.Conn,
	query string,
	callback vitessmysql.ResultSpoolFn,
) (string, error) {
	// Execute only the first statement; return the remainder unparsed.
	// The Vitess listener will call us again for each subsequent statement.
	return "", h.ComQuery(ctx, c, query, callback)
}

// ComPrepare satisfies vitessmysql.Handler. Returns no fields — prepared
// statement parameter metadata is not yet implemented.
func (h *Handler) ComPrepare(_ context.Context, _ *vitessmysql.Conn, _ string, _ *vitessmysql.PrepareData) ([]*querypb.Field, error) {
	return nil, nil
}

// ComStmtExecute satisfies vitessmysql.Handler. Converts the Vitess bind
// variables into sqlparser expressions and calls engine.QueryWithBindings so
// that positional parameters are substituted before execution.
func (h *Handler) ComStmtExecute(
	ctx context.Context,
	c *vitessmysql.Conn,
	prepare *vitessmysql.PrepareData,
	callback func(*sqltypes.Result) error,
) error {
	sess, ok := c.ClientData.(*session.Session)
	if !ok || sess == nil {
		return fmt.Errorf("no session for connection %d", c.ConnectionID)
	}

	sqlCtx := gmssql.NewContext(ctx, gmssql.WithSession(sess))

	exprs, err := bindingsToExprs(prepare.BindVars)
	if err != nil {
		return err
	}

	schema, rowIter, _, err := h.engine.QueryWithBindings(sqlCtx, prepare.PrepareStmt, nil, exprs, nil)
	if err != nil {
		return castError(err)
	}
	defer rowIter.Close(sqlCtx)

	return castError(spoolResult(sqlCtx, schema, rowIter, func(res *sqltypes.Result, more bool) error {
		return callback(res)
	}))
}
