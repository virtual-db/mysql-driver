# Configuration

`Config` is passed to `NewDriver` and controls every aspect of how the driver binds, connects, and authenticates.

```go
type Config struct {
    Addr             string
    DBName           string
    SourceDSN        string
    AuthSourceAddr   string
    AuthProbeTimeout time.Duration
    ConnReadTimeout  time.Duration
    ConnWriteTimeout time.Duration
    TLSConfig        *tls.Config
}
```

---

## Fields

### `Addr` · `string` · required

TCP address the driver listens on. Passed verbatim to `net.Listen("tcp", ...)`.

```
Addr: ":3307"
Addr: "0.0.0.0:3307"
Addr: "127.0.0.1:3307"
```

---

### `DBName` · `string` · required

Logical database name exposed to connecting clients. Used for GMS catalog lookup, `INFORMATION_SCHEMA` queries, and session initialization. Must match the database name in `SourceDSN`.

---

### `SourceDSN` · `string` · required

`database/sql`-format DSN for the source MySQL database. The driver opens a persistent connection pool against this DSN for schema reads (`INFORMATION_SCHEMA`) and row reads (`SELECT`). The driver never issues writes against this connection.

```
SourceDSN: "user:pass@tcp(localhost:3306)/mydb"
SourceDSN: "user:pass@tcp(10.0.0.1:3306)/mydb?parseTime=true"
```

Uses `github.com/go-sql-driver/mysql` — see its documentation for the full DSN syntax.

---

### `AuthSourceAddr` · `string` · required

`host:port` of the real MySQL server used to validate client credentials. For every new connection, the driver opens a short-lived probe connection to this address and issues `PING` + `SHOW GRANTS FOR CURRENT_USER()`. If the real server rejects the credentials, the connecting client receives MySQL error `1045`.

**`NewDriver` panics if this field is empty.**

This may be the same host as the one in `SourceDSN`, but the two serve different purposes: `SourceDSN` is for the long-lived data pool; `AuthSourceAddr` is for per-connection credential probing.

```
AuthSourceAddr: "localhost:3306"
AuthSourceAddr: "10.0.0.1:3306"
```

---

### `AuthProbeTimeout` · `time.Duration` · default `5s`

Maximum time the auth probe is allowed to take, covering TCP connect, `PING`, and `SHOW GRANTS FOR CURRENT_USER()`. Connections that exceed this timeout are refused.

Zero is replaced with `5 * time.Second`.

```
AuthProbeTimeout: 3 * time.Second
```

---

### `ConnReadTimeout` · `time.Duration` · default `1m`

Per-connection read timeout on the MySQL listener. Zero is replaced with `time.Minute`.

```
ConnReadTimeout: 30 * time.Second
```

---

### `ConnWriteTimeout` · `time.Duration` · default `1m`

Per-connection write timeout on the MySQL listener. Zero is replaced with `time.Minute`.

```
ConnWriteTimeout: 30 * time.Second
```

---

### `TLSConfig` · `*tls.Config` · default `nil`

Server-side TLS configuration for the client-facing listener.

| Value | Auth method advertised | Behaviour |
|---|---|---|
| `nil` | `mysql_clear_password` | Plaintext passwords accepted without TLS. `AllowClearTextWithoutTLS` is set on the listener. Suitable for local development or trusted private networks. |
| non-nil | `caching_sha2_password` | The listener requires TLS. The SHA-2 cache fast-path is permanently disabled to ensure the plaintext password is always delivered to the auth probe logic. |

In both modes the plaintext password is forwarded to `AuthSourceAddr` for validation and is never stored by the driver.

```go
cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
if err != nil {
    log.Fatal(err)
}

cfg := driver.Config{
    // ...
    TLSConfig: &tls.Config{
        Certificates: []tls.Certificate{cert},
    },
}
```
