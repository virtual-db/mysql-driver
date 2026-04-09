# vdb-mysql-driver

MySQL-specific driver for VirtualDB. Implements `vdb-core` interfaces using the go-mysql-server (GMS) engine.

Imports `github.com/AnqorDX/vdb-core` and `github.com/dolthub/go-mysql-server`. All GMS surface area is encapsulated here — consumers of this module do not interact with GMS directly.

## Module ecosystem

```
vdb-mysql
  └── github.com/AnqorDX/vdb-mysql-driver  ← this module
        └── github.com/AnqorDX/vdb-core
```
