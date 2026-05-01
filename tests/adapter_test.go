package driver_test

import (
	"testing"

	core "github.com/virtual-db/vdb-core"
	driver "github.com/virtual-db/vdb-mysql-driver"
)

// TestDriver_SatisfiesCoreServer is the compile-time contract test.
// apiAdapter is an unexported implementation detail — its 14 delegation
// methods are verified at compile time by the var _ assertion in adapter.go.
// What is observable from outside the package is that *Driver satisfies
// core.Server, which is the contract the framework depends on.
func TestDriver_SatisfiesCoreServer(t *testing.T) {
	var _ core.Server = (*driver.Driver)(nil)
}
