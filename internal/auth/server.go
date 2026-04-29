package auth

import (
	"crypto/tls"
	"time"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
)

// Config holds the parameters needed to construct a VDBAuthServer.
type Config struct {
	// SourceAddr is the host:port of the source MySQL database used for
	// credential verification probes. Required.
	SourceAddr string

	// ProbeTimeout is the maximum time allowed for the probe connection,
	// including the SHOW GRANTS query. Defaults to 5 seconds if zero.
	ProbeTimeout time.Duration

	// TLSConfig is the server-side TLS configuration.
	// Non-nil: caching_sha2_password is advertised; TLS is required for
	// full-auth. Nil: mysql_clear_password is advertised; the caller must
	// set AllowClearTextWithoutTLS on the listener (local dev mode).
	TLSConfig *tls.Config
}

// VDBAuthServer implements vitessmysql.AuthServer. Credentials are validated
// by probing the source MySQL database. Grants are fetched on success and
// returned via conn.UserData for session.Builder to consume.
type VDBAuthServer struct {
	methods []vitessmysql.AuthMethod
	hasTLS  bool
}

// New constructs a VDBAuthServer from cfg.
func New(cfg Config) *VDBAuthServer {
	if cfg.ProbeTimeout == 0 {
		cfg.ProbeTimeout = 5 * time.Second
	}

	store := &probeStorage{
		sourceAddr: cfg.SourceAddr,
		timeout:    cfg.ProbeTimeout,
	}
	cache := &noCacheStorage{}
	validator := &acceptAllValidator{}

	var methods []vitessmysql.AuthMethod
	if cfg.TLSConfig != nil {
		// TLS present: caching_sha2_password. noCacheStorage forces the
		// full-auth path so Vitess delivers the plaintext password to
		// probeStorage.UserEntryWithPassword.
		methods = []vitessmysql.AuthMethod{
			vitessmysql.NewSha2CachingAuthMethod(cache, store, validator),
		}
	} else {
		// No TLS: mysql_clear_password for local dev. Plaintext password is
		// delivered to the same probeStorage.UserEntryWithPassword method.
		methods = []vitessmysql.AuthMethod{
			vitessmysql.NewMysqlClearAuthMethod(store, validator),
		}
	}

	return &VDBAuthServer{methods: methods, hasTLS: cfg.TLSConfig != nil}
}

// AuthMethods satisfies vitessmysql.AuthServer.
func (s *VDBAuthServer) AuthMethods() []vitessmysql.AuthMethod {
	return s.methods
}

// DefaultAuthMethodDescription satisfies vitessmysql.AuthServer.
// Returns the plugin name advertised in the server greeting packet.
func (s *VDBAuthServer) DefaultAuthMethodDescription() vitessmysql.AuthMethodDescription {
	if s.hasTLS {
		return vitessmysql.CachingSha2Password
	}
	return vitessmysql.MysqlClearPassword
}
