package auth

// This file exposes package-internal types and functions for use by the
// internal/auth/tests package. Because the auth package is under internal/,
// these exports are invisible to any code outside this module, so they carry
// no public-API cost.
//
// Go's export_test.go mechanism only works for tests that live in the same
// directory; tests in a sub-directory (internal/auth/tests/) require real
// (non-_test.go) exported symbols. Keeping them here – in an internal package
// – is the standard work-around.

import (
	"context"
	"database/sql"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
)

// NewGetterForTest wraps grants in a vdbGetter and returns it as the
// vitessmysql.Getter interface. Assign the result to conn.UserData in tests
// that need a valid, correctly-typed UserData value.
func NewGetterForTest(grants *Grants) vitessmysql.Getter {
	return &vdbGetter{grants: grants}
}

// NewNoCacheStorageForTest returns an initialised *noCacheStorage so that
// probe_test.go can exercise UserEntryWithCacheHash without importing the
// unexported type directly.
func NewNoCacheStorageForTest() *noCacheStorage {
	return &noCacheStorage{}
}

// NewAcceptAllValidatorForTest returns an initialised *acceptAllValidator so
// that probe_test.go can call HandleUser without importing the unexported type.
func NewAcceptAllValidatorForTest() *acceptAllValidator {
	return &acceptAllValidator{}
}

// FetchGrantsForTest calls the unexported fetchGrants function so that
// probe_test.go can test it against a sqlmock database.
func FetchGrantsForTest(ctx context.Context, db *sql.DB) ([]string, error) {
	return fetchGrants(ctx, db)
}
