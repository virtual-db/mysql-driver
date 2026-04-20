package session

import (
	gmssql "github.com/dolthub/go-mysql-server/sql"

	"github.com/virtual-db/mysql-driver/internal/bridge"
)

// Session embeds *gmssql.BaseSession and implements:
//   - sql.Session                (via embedding)
//   - sql.LifecycleAwareSession  (CommandBegin, CommandEnd, SessionEnd)
//   - sql.TransactionSession     (StartTransaction, CommitTransaction, Rollback,
//     CreateSavepoint, RollbackToSavepoint, ReleaseSavepoint)
type Session struct {
	*gmssql.BaseSession
	connID uint32
	events bridge.EventBridge
}

// CommandBegin satisfies sql.LifecycleAwareSession. Called before every SQL command.
func (s *Session) CommandBegin() error { return nil }

// CommandEnd satisfies sql.LifecycleAwareSession. Called after every SQL command.
func (s *Session) CommandEnd() {}

// SessionEnd satisfies sql.LifecycleAwareSession. Called when the connection
// tears down. Invokes events.ConnectionClosed.
func (s *Session) SessionEnd() {
	client := s.Client()
	s.events.ConnectionClosed(s.connID, client.User, client.Address)
}

// StartTransaction satisfies sql.TransactionSession. Invokes
// events.TransactionBegun. A non-nil error refuses the transaction.
func (s *Session) StartTransaction(
	ctx *gmssql.Context,
	tChar gmssql.TransactionCharacteristic,
) (gmssql.Transaction, error) {
	readOnly := tChar == gmssql.ReadOnly
	if err := s.events.TransactionBegun(s.connID, readOnly); err != nil {
		return nil, err
	}
	return &Transaction{ReadOnly: readOnly}, nil
}

// CommitTransaction satisfies sql.TransactionSession. Invokes
// events.TransactionCommitted.
func (s *Session) CommitTransaction(
	ctx *gmssql.Context,
	tx gmssql.Transaction,
) error {
	return s.events.TransactionCommitted(s.connID)
}

// Rollback satisfies sql.TransactionSession. Invokes
// events.TransactionRolledBack with an empty savepoint (full rollback).
func (s *Session) Rollback(ctx *gmssql.Context, tx gmssql.Transaction) error {
	s.events.TransactionRolledBack(s.connID, "")
	return nil
}

// CreateSavepoint satisfies sql.TransactionSession. No-op at the engine level.
func (s *Session) CreateSavepoint(
	ctx *gmssql.Context,
	tx gmssql.Transaction,
	name string,
) error {
	return nil
}

// RollbackToSavepoint satisfies sql.TransactionSession. Invokes
// events.TransactionRolledBack with the savepoint name.
func (s *Session) RollbackToSavepoint(
	ctx *gmssql.Context,
	tx gmssql.Transaction,
	name string,
) error {
	s.events.TransactionRolledBack(s.connID, name)
	return nil
}

// ReleaseSavepoint satisfies sql.TransactionSession. No-op at the engine level.
func (s *Session) ReleaseSavepoint(
	ctx *gmssql.Context,
	tx gmssql.Transaction,
	name string,
) error {
	return nil
}
