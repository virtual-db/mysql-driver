package auth

import (
	"context"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net"
	"time"

	_ "github.com/go-sql-driver/mysql"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
)

// noCacheStorage implements vitessmysql.CachingStorage and always returns
// AuthNeedMoreData. This permanently disables the SHA2 fast-auth (cache-hit)
// path, forcing every caching_sha2_password handshake through full-auth so
// that Vitess requests and receives the plaintext password. The plaintext
// password is required to open the source DB probe connection.
type noCacheStorage struct{}

func (n *noCacheStorage) UserEntryWithCacheHash(
	_ []*x509.Certificate,
	_ []byte,
	_ string,
	_ []byte,
	_ net.Addr,
) (vitessmysql.Getter, vitessmysql.CacheState, error) {
	return nil, vitessmysql.AuthNeedMoreData, nil
}

// acceptAllValidator implements vitessmysql.UserValidator.
// VDB holds no local user list. Every username is eligible for the auth flow;
// the source DB is the sole authority on whether credentials are valid.
type acceptAllValidator struct{}

func (v *acceptAllValidator) HandleUser(_ string, _ net.Addr) bool { return true }

// probeStorage implements vitessmysql.PlainTextStorage. It receives the
// plaintext password from Vitess after the full-auth handshake, opens a
// short-lived connection to the source DB to verify the credentials, fetches
// the user's grants, and closes the connection before returning.
type probeStorage struct {
	sourceAddr string
	timeout    time.Duration
}

// UserEntryWithPassword satisfies vitessmysql.PlainTextStorage.
// Called by Vitess once the client has sent its plaintext password, either
// via caching_sha2_password full-auth (TLS) or mysql_clear_password (no TLS).
func (p *probeStorage) UserEntryWithPassword(
	_ []*x509.Certificate,
	user string,
	password string,
	_ net.Addr,
) (vitessmysql.Getter, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", user, password, p.sourceAddr)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("auth: open probe connection for %q: %w", user, err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, vitessmysql.NewSQLError(
			vitessmysql.ERAccessDeniedError,
			vitessmysql.SSAccessDeniedError,
			"Access denied for user '%s'", user,
		)
	}

	scopes, err := fetchGrants(ctx, db)
	if err != nil {
		// Grant fetch failure is not a credential failure. The probe was
		// accepted by the source DB. Proceed with an empty scope set.
		// TODO: log this error when a logging interface is available.
		scopes = nil
	}

	return &vdbGetter{grants: &Grants{User: user, Scopes: scopes}}, nil
}

// fetchGrants queries SHOW GRANTS FOR CURRENT_USER() on the authenticated
// probe connection. The connection is already authenticated as the client, so
// the source DB is the authority on what this identity is permitted to access.
func fetchGrants(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return nil, fmt.Errorf("auth: fetch grants: %w", err)
	}
	defer rows.Close()

	var scopes []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return nil, fmt.Errorf("auth: scan grant row: %w", err)
		}
		scopes = append(scopes, g)
	}
	return scopes, rows.Err()
}
