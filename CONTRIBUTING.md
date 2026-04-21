# Contributing to mysql-driver

> [!IMPORTANT]
> ### Public Contributions Are Not Yet Open
>
> VirtualDB is not currently accepting public pull requests. We intend to open the project to community contributions in the future — when we do, this document will be updated accordingly.
>
> **In the meantime, we encourage you to participate by [opening a GitHub Issue](https://github.com/virtual-db/mysql-driver/issues).** Issues are the best way to report bugs, request features, ask questions, and start discussions with the team.
>
> Thank you for your interest in VirtualDB.

---

## What We Will Accept

When contributions open, the following will be in scope for this module:

- Bug fixes in the driver lifecycle, auth proxy, schema resolution, row conversion, or GMS wiring.
- Improvements to test coverage for existing behaviour.
- Performance improvements that do not change the public API or event semantics.
- Documentation corrections.

The following will require prior discussion via a GitHub Issue:

- New public API surface (fields on `Config`, new exported types or functions).
- Changes to the `bridge.EventBridge` contract, since it has downstream effects on `core` and every framework implementation.
- Schema features that expand what is fetched from `INFORMATION_SCHEMA` (e.g. composite PK support, ENUM value lists, DECIMAL precision).
- Dependency additions or upgrades.

---

## Development Setup

**Requirements**

- Go 1.23.3 or later.
- A running MySQL 8.x instance for integration tests (the unit tests use `go-sqlmock` and do not require a real database).

**Clone and build**

```sh
git clone https://github.com/virtual-db/mysql-driver
cd mysql-driver
go build ./...
```

**Run unit tests**

```sh
go test ./...
```

---

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`).
- Keep package boundaries as they are. Internal packages (`internal/`) must not import `core` or any other framework package. The `bridge.EventBridge` interface is the only crossing point.
- New exported symbols require a doc comment.
- Unexported helpers that are not obvious need a comment explaining the non-obvious part.
- Avoid adding new direct dependencies without prior discussion.

---

## Reporting Bugs

Open an issue. Include:

- Go version and OS.
- The MySQL source version you are connecting to.
- A minimal reproducer (SQL queries, `Config` values with credentials redacted, observed vs. expected behaviour).

---

## Security Issues

Do not open a public issue for security vulnerabilities. Email the maintainers directly. You will receive a response within 5 business days.

---

## License

Contributions will be licensed under the Elastic License 2.0. See [LICENSE.md](LICENSE.md).