package handler

import (
	"context"
	"fmt"
	"io"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	gmssql "github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/virtual-db/mysql-driver/internal/auth"
	"github.com/virtual-db/mysql-driver/internal/bridge"
	"github.com/virtual-db/mysql-driver/internal/gms/session"
)

// bindingsToExprs converts Vitess bind variables (from a prepared statement
// execution) into the sqlparser.Expr map that engine.QueryWithBindings expects.
func bindingsToExprs(bindings map[string]*querypb.BindVariable) (map[string]sqlparser.Expr, error) {
	if len(bindings) == 0 {
		return nil, nil
	}
	res := make(map[string]sqlparser.Expr, len(bindings))
	for name, bv := range bindings {
		val, err := sqltypes.BindVariableToValue(bv)
		if err != nil {
			return nil, fmt.Errorf("handler: bind variable %q: %w", name, err)
		}
		expr, err := sqlparser.ExprFromValue(val)
		if err != nil {
			return nil, fmt.Errorf("handler: bind variable %q to expr: %w", name, err)
		}
		res[name] = expr
	}
	return res, nil
}

// Handler implements vitessmysql.Handler. It is the boundary between the
// Vitess MySQL wire protocol layer (Layer 1) and the GMS sqle.Engine (Layer 2).
//
// NewConnection creates a VDBSession from the auth grants populated by
// VDBAuthServer during the handshake and stores it in conn.ClientData.
//
// ComQuery builds a *sql.Context from the session, calls engine.Query directly,
// converts the resulting rows to Vitess sqltypes.Result values, and spools them
// to the callback. No server.Server is involved at any point.
type Handler struct {
	engine *sqle.Engine
	dbName string
	events bridge.EventBridge
}

// New constructs a Handler.
func New(engine *sqle.Engine, dbName string, events bridge.EventBridge) *Handler {
	return &Handler{engine: engine, dbName: dbName, events: events}
}

// NewConnection satisfies vitessmysql.Handler. Called after the Vitess
// handshake and auth cycle complete. Extracts the grants populated by
// VDBAuthServer from conn.UserData, fires ConnectionOpened, and stores
// the resulting VDBSession in conn.ClientData for use by ComQuery.
func (h *Handler) NewConnection(c *vitessmysql.Conn) {
	grants := auth.GetGrantsFromConn(c)
	connID := c.ConnectionID
	user := c.User
	addr := c.RemoteAddr().String()

	if err := h.events.ConnectionOpened(connID, user, addr); err != nil {
		// NewConnection has no error return. Flag the connection as refused
		// by leaving ClientData nil; ComQuery will detect this and return an
		// error for every subsequent query.
		return
	}

	base := gmssql.NewBaseSessionWithClientServer(
		addr,
		gmssql.Client{User: user, Address: addr},
		connID,
	)
	base.SetCurrentDatabase(h.dbName)

	c.ClientData = session.New(base, connID, h.events, grants)
}

// ConnectionClosed satisfies vitessmysql.Handler. Fires SessionEnd on the
// stored VDBSession, which in turn fires the ConnectionClosed pipeline event.
func (h *Handler) ConnectionClosed(c *vitessmysql.Conn) {
	sess, ok := c.ClientData.(*session.Session)
	if !ok || sess == nil {
		return
	}
	sess.SessionEnd()
}

// ConnectionAborted satisfies vitessmysql.Handler. Called when a new
// connection cannot be fully established (e.g. auth failure). No session was
// created, so there is nothing to clean up.
func (h *Handler) ConnectionAborted(_ *vitessmysql.Conn, _ string) error {
	return nil
}

// ComInitDB satisfies vitessmysql.Handler. Updates the current database on the
// session stored in conn.ClientData.
func (h *Handler) ComInitDB(c *vitessmysql.Conn, schemaName string) error {
	sess, ok := c.ClientData.(*session.Session)
	if !ok || sess == nil {
		return fmt.Errorf("no session for connection %d", c.ConnectionID)
	}
	sess.SetCurrentDatabase(schemaName)
	return nil
}

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
		return err
	}
	defer rowIter.Close(sqlCtx)

	return spoolResult(sqlCtx, schema, rowIter, callback)
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
		return err
	}
	defer rowIter.Close(sqlCtx)

	return spoolResult(sqlCtx, schema, rowIter, func(res *sqltypes.Result, more bool) error {
		return callback(res)
	})
}

// WarningCount satisfies vitessmysql.Handler.
func (h *Handler) WarningCount(_ *vitessmysql.Conn) uint16 { return 0 }

// ComResetConnection satisfies vitessmysql.Handler. Resets the current
// database on the session to the driver's configured database name.
func (h *Handler) ComResetConnection(c *vitessmysql.Conn) error {
	sess, ok := c.ClientData.(*session.Session)
	if !ok || sess == nil {
		return nil
	}
	sess.SetCurrentDatabase(h.dbName)
	return nil
}

// ParserOptionsForConnection satisfies vitessmysql.Handler.
func (h *Handler) ParserOptionsForConnection(_ *vitessmysql.Conn) (sqlparser.ParserOptions, error) {
	return sqlparser.ParserOptions{}, nil
}

// spoolResult dispatches to spoolOkResult or spoolRows depending on whether
// the schema represents a DML OkResult or a normal row-returning query.
func spoolResult(
	ctx *gmssql.Context,
	schema gmssql.Schema,
	iter gmssql.RowIter,
	callback vitessmysql.ResultSpoolFn,
) error {
	if gmstypes.IsOkResultSchema(schema) {
		return spoolOkResult(ctx, iter, callback)
	}
	return spoolRows(ctx, schema, iter, callback)
}

// spoolOkResult handles DML queries (INSERT, UPDATE, DELETE, DDL) that return
// an OkResult rather than a row set. It reads the single OkResult from the
// iterator and returns an sqltypes.Result carrying RowsAffected and InsertID.
func spoolOkResult(
	ctx *gmssql.Context,
	iter gmssql.RowIter,
	callback vitessmysql.ResultSpoolFn,
) error {
	row, err := iter.Next(ctx)
	if err != nil && err != io.EOF {
		return err
	}

	res := &sqltypes.Result{}
	if len(row) > 0 {
		ok := gmstypes.GetOkResult(row)
		res.RowsAffected = ok.RowsAffected
		res.InsertID = ok.InsertID
	}

	return callback(res, false)
}

// spoolRows converts a GMS (schema, rowIter) pair into Vitess sqltypes.Result
// values and delivers them to callback. Each row is sent as a separate Result
// to keep memory bounded on large result sets.
func spoolRows(
	ctx *gmssql.Context,
	schema gmssql.Schema,
	iter gmssql.RowIter,
	callback vitessmysql.ResultSpoolFn,
) error {
	fields := schemaToFields(ctx, schema)
	buf := gmssql.NewByteBuffer(4096)

	// Send the fields-only header result first (more=true because rows follow).
	if err := callback(&sqltypes.Result{Fields: fields}, true); err != nil {
		return err
	}

	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		sqlVals, err := server.RowToSQL(ctx, schema, row, nil, buf)
		if err != nil {
			return err
		}

		if err := callback(&sqltypes.Result{
			Fields: fields,
			Rows:   [][]sqltypes.Value{sqlVals},
		}, true); err != nil {
			return err
		}
	}

	// Send the final empty result to signal completion.
	return callback(&sqltypes.Result{Fields: fields}, false)
}

// schemaToFields converts a GMS sql.Schema into the Vitess []*querypb.Field
// representation used in Result packets. Covers the common type set; charset
// and flag metadata matches what GMS's own handler produces.
func schemaToFields(ctx *gmssql.Context, s gmssql.Schema) []*querypb.Field {
	charSetResults := ctx.GetCharacterSetResults()
	fields := make([]*querypb.Field, len(s))
	for i, col := range s {
		charset := uint32(gmssql.Collation_Default.CharacterSet())
		if ct, ok := col.Type.(gmssql.TypeWithCollation); ok {
			charset = uint32(ct.Collation().CharacterSet())
		}
		if gmstypes.IsBinaryType(col.Type) {
			charset = uint32(gmssql.Collation_binary)
		} else if charSetResults != gmssql.CharacterSet_Unspecified {
			charset = uint32(charSetResults)
		}

		var flags querypb.MySqlFlag
		if !col.Nullable {
			flags |= querypb.MySqlFlag_NOT_NULL_FLAG
		}
		if col.AutoIncrement {
			flags |= querypb.MySqlFlag_AUTO_INCREMENT_FLAG
		}
		if col.PrimaryKey {
			flags |= querypb.MySqlFlag_PRI_KEY_FLAG
		}
		if gmstypes.IsUnsigned(col.Type) {
			flags |= querypb.MySqlFlag_UNSIGNED_FLAG
		}

		fields[i] = &querypb.Field{
			Name:     col.Name,
			OrgName:  col.Name,
			Table:    col.Source,
			OrgTable: col.Source,
			Database: col.DatabaseSource,
			Type:     col.Type.Type(),
			Charset:  charset,
			Flags:    uint32(flags),
		}
	}
	return fields
}
