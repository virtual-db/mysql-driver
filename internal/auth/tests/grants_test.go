package auth_test

import (
	"testing"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/virtual-db/mysql-driver/internal/auth"
)

// wrongGetter satisfies vitessmysql.Getter but is NOT a *vdbGetter.
type wrongGetter struct{}

func (w *wrongGetter) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: "wrong"}
}

func TestGetGrantsFromConn_NilUserData_ReturnsNil(t *testing.T) {
	conn := &vitessmysql.Conn{}
	conn.UserData = nil

	got := auth.GetGrantsFromConn(conn)
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestGetGrantsFromConn_WrongType_ReturnsNil(t *testing.T) {
	conn := &vitessmysql.Conn{}
	conn.UserData = &wrongGetter{}

	got := auth.GetGrantsFromConn(conn)
	if got != nil {
		t.Fatalf("expected nil for wrong UserData type, got %+v", got)
	}
}

func TestGetGrantsFromConn_ValidGetter_ReturnsGrants(t *testing.T) {
	want := &auth.Grants{
		User:   "alice",
		Scopes: []string{"GRANT ALL PRIVILEGES ON *.* TO 'alice'@'%'"},
	}

	conn := &vitessmysql.Conn{}
	conn.UserData = auth.NewGetterForTest(want)

	got := auth.GetGrantsFromConn(conn)
	if got == nil {
		t.Fatal("expected non-nil Grants, got nil")
	}
	if got.User != want.User {
		t.Errorf("User: got %q, want %q", got.User, want.User)
	}
	if len(got.Scopes) != len(want.Scopes) {
		t.Fatalf("Scopes length: got %d, want %d", len(got.Scopes), len(want.Scopes))
	}
	for i, s := range want.Scopes {
		if got.Scopes[i] != s {
			t.Errorf("Scopes[%d]: got %q, want %q", i, got.Scopes[i], s)
		}
	}
}
