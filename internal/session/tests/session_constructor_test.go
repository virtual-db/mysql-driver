package session_test

import (
	"testing"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
)

func TestBuilder_ReturnsNonNilSession(t *testing.T) {
	conn := &vitessmysql.Conn{ConnectionID: 1, User: "alice"}
	s := buildSession(t, "testdb", &stubEventBridge{}, conn, "127.0.0.1:1234")
	if s == nil {
		t.Fatal("Builder returned nil session")
	}
}

func TestBuilder_Session_HasCorrectConnID(t *testing.T) {
	conn := &vitessmysql.Conn{ConnectionID: 42, User: "alice"}
	s := buildSession(t, "testdb", &stubEventBridge{}, conn, "127.0.0.1:1234")
	if s.ID() != 42 {
		t.Errorf("session ID: got %d, want 42", s.ID())
	}
}

func TestSession_CommandBegin_DoesNotError(t *testing.T) {
	conn := &vitessmysql.Conn{ConnectionID: 1, User: "alice"}
	s := buildSession(t, "testdb", &stubEventBridge{}, conn, "127.0.0.1:1234")

	if err := s.CommandBegin(); err != nil {
		t.Fatalf("CommandBegin returned unexpected error: %v", err)
	}
}

func TestSession_CommandEnd_DoesNotPanic(t *testing.T) {
	conn := &vitessmysql.Conn{ConnectionID: 1, User: "alice"}
	s := buildSession(t, "testdb", &stubEventBridge{}, conn, "127.0.0.1:1234")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("CommandEnd panicked: %v", r)
		}
	}()
	s.CommandEnd()
}
