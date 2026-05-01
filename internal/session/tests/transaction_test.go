package session_test

import (
	"strings"
	"testing"

	. "github.com/virtual-db/vdb-mysql-driver/internal/session"
)

func TestTransaction_ReadOnlyTrue_ReportsReadOnly(t *testing.T) {
	tx := &Transaction{ReadOnly: true}
	if !tx.IsReadOnly() {
		t.Error("IsReadOnly: got false, want true")
	}
}

func TestTransaction_ReadOnlyFalse_NotReadOnly(t *testing.T) {
	tx := &Transaction{ReadOnly: false}
	if tx.IsReadOnly() {
		t.Error("IsReadOnly: got true, want false")
	}
}

func TestTransaction_String_ReadOnly_ContainsReadOnly(t *testing.T) {
	tx := &Transaction{ReadOnly: true}
	if !strings.Contains(tx.String(), "readOnly") {
		t.Errorf("String(): got %q, expected it to contain %q", tx.String(), "readOnly")
	}
}

func TestTransaction_String_ReadWrite_ContainsReadWrite(t *testing.T) {
	tx := &Transaction{ReadOnly: false}
	if !strings.Contains(tx.String(), "readWrite") {
		t.Errorf("String(): got %q, expected it to contain %q", tx.String(), "readWrite")
	}
}
