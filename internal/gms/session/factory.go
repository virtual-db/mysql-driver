package session

import (
	"context"
	"fmt"

	vitessmysql "github.com/dolthub/vitess/go/mysql"

	"github.com/virtual-db/mysql-driver/internal/bridge"
	"github.com/dolthub/go-mysql-server/server"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// Builder returns a server.SessionBuilder that:
//  1. Extracts connID, user, and addr from the Vitess connection.
//  2. Calls events.ConnectionOpened — a non-nil error refuses the connection.
//  3. Sets the current database on the base session.
//  4. Constructs and returns a Session on success.
func Builder(dbName string, events bridge.EventBridge) server.SessionBuilder {
	return func(ctx context.Context, conn *vitessmysql.Conn, addr string) (gmssql.Session, error) {
		connID := conn.ConnectionID
		user := conn.User

		if err := events.ConnectionOpened(connID, user, addr); err != nil {
			return nil, fmt.Errorf("connection refused: %w", err)
		}

		base := gmssql.NewBaseSessionWithClientServer(
			addr,
			gmssql.Client{User: user, Address: addr},
			connID,
		)
		base.SetCurrentDatabase(dbName)

		return &Session{
			BaseSession: base,
			connID:      connID,
			events:      events,
		}, nil
	}
}
