package auth

import (
	vitessmysql "github.com/dolthub/vitess/go/mysql"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

// Grants holds the authorization state derived from the probe connection.
// It is populated once at connection time and carried for the lifetime of
// the session. The Delta reads it via Session.GetGrants() on every storage
// backend call to enforce authorization scope.
type Grants struct {
	// User is the authenticated MySQL username.
	User string
	// Scopes contains the raw rows returned by SHOW GRANTS FOR CURRENT_USER().
	// Each entry is one GRANT statement as MySQL formats it.
	Scopes []string
}

// vdbGetter wraps Grants and satisfies vitessmysql.Getter.
// Vitess stores this value in conn.UserData after a successful handshake.
type vdbGetter struct {
	grants *Grants
}

// Get satisfies vitessmysql.Getter.
func (g *vdbGetter) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: g.grants.User}
}

// GetGrantsFromConn extracts *Grants from a Vitess connection's UserData.
// Returns nil if UserData was not populated by VDBAuthServer — for example,
// in unit tests that bypass auth entirely.
func GetGrantsFromConn(conn *vitessmysql.Conn) *Grants {
	if conn.UserData == nil {
		return nil
	}
	g, ok := conn.UserData.(*vdbGetter)
	if !ok {
		return nil
	}
	return g.grants
}
