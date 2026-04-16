# Contributing to vdb-mysql-driver

Thank you for your interest in contributing. This document covers how to submit changes, what is expected of contributors, and how the project is structured internally.

---

## Contributor License Agreement

Before any contribution can be merged, you must sign the Anqor Contributor License Agreement (CLA). The CLA grants Anqor the rights necessary to distribute your contribution under the Elastic License 2.0 and future license versions without requiring further permission from you.

**You must read and agree to [CLA.md](CLA.md) before submitting a pull request.**

If you are contributing on behalf of an employer or other legal entity, an authorized representative of that entity must also sign the CLA.

---

## What We Accept

Contributions that are in scope for this module:

- Bug fixes in the driver lifecycle, auth proxy, schema resolution, row conversion, or GMS wiring.
- Improvements to test coverage for existing behaviour.
- Performance improvements that do not change the public API or event semantics.
- Documentation corrections.

Contributions that require prior discussion:

- New public API surface (fields on `Config`, new exported types or functions).
- Changes to the `bridge.EventBridge` contract, since it has downstream effects on `vdb-core` and every framework implementation.
- Schema features that expand what is fetched from `INFORMATION_SCHEMA` (e.g. composite PK support, ENUM value lists, DECIMAL precision).
- Dependency additions or upgrades.

Open an issue and describe what you want to do before writing code for any of the above. This avoids wasted effort if the direction does not fit the project.

---

## Development Setup

**Requirements**

- Go 1.23.3 or later.
- A running MySQL 8.x instance for integration tests (the unit tests use `go-sqlmock` and do not require a real database).

**Clone and build**

```
git clone https://github.com/AnqorDX/vdb-mysql-driver
cd vdb-mysql-driver
go build ./...
```

**Run unit tests**

```
go test ./...
```

Unit tests use mocks and stubs. They do not open a real MySQL connection and do not require any external services.

---

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`).
- Keep package boundaries as they are. Internal packages (`internal/`) must not import `vdb-core` or any other framework package. The `bridge.EventBridge` interface is the only crossing point.
- New exported symbols require a doc comment.
- Unexported helpers that are not obvious need a comment explaining the non-obvious part.
- Avoid adding new direct dependencies without prior discussion.

---

## Submitting a Pull Request

1. Fork the repository and create a branch from `main`.
2. Make your changes. If you are fixing a bug, add a test that fails before the fix and passes after.
3. Run `go test ./...` and confirm all tests pass.
4. Run `go vet ./...`.
5. Open a pull request against `main`. Describe what the change does and why.
6. Sign the CLA if prompted. The CLA bot will comment on the PR with instructions if you have not signed yet.

Pull requests that do not pass tests, do not have a description, or are missing CLA sign-off will not be reviewed until those issues are resolved.

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

By contributing to this repository, you agree that your contributions will be licensed under the Elastic License 2.0. See [LICENSE.md](LICENSE.md) and [CLA.md](CLA.md).
