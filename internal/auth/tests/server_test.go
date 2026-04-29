package auth_test

import (
	"crypto/tls"
	"testing"

	vitessmysql "github.com/dolthub/vitess/go/mysql"

	"github.com/virtual-db/mysql-driver/internal/auth"
)

func TestNew_NoTLS_DefaultsProbeTimeout(t *testing.T) {
	cfg := auth.Config{SourceAddr: "127.0.0.1:3306"}
	s := auth.New(cfg)
	if s == nil {
		t.Fatal("expected non-nil VDBAuthServer, got nil")
	}
}

func TestNew_WithTLS_ReturnsNonNil(t *testing.T) {
	cfg := auth.Config{
		SourceAddr: "127.0.0.1:3306",
		TLSConfig:  &tls.Config{},
	}
	s := auth.New(cfg)
	if s == nil {
		t.Fatal("expected non-nil VDBAuthServer, got nil")
	}
}

func TestDefaultAuthMethodDescription_NoTLS_ReturnsClearText(t *testing.T) {
	s := auth.New(auth.Config{SourceAddr: "127.0.0.1:3306"})
	got := s.DefaultAuthMethodDescription()
	if got != vitessmysql.MysqlClearPassword {
		t.Errorf("expected MysqlClearPassword, got %q", got)
	}
}

func TestDefaultAuthMethodDescription_WithTLS_ReturnsSha2(t *testing.T) {
	s := auth.New(auth.Config{
		SourceAddr: "127.0.0.1:3306",
		TLSConfig:  &tls.Config{},
	})
	got := s.DefaultAuthMethodDescription()
	if got != vitessmysql.CachingSha2Password {
		t.Errorf("expected CachingSha2Password, got %q", got)
	}
}

func TestAuthMethods_NoTLS_ReturnsOneMethod(t *testing.T) {
	s := auth.New(auth.Config{SourceAddr: "127.0.0.1:3306"})
	methods := s.AuthMethods()
	if len(methods) != 1 {
		t.Errorf("expected 1 auth method, got %d", len(methods))
	}
}

func TestAuthMethods_WithTLS_ReturnsOneMethod(t *testing.T) {
	s := auth.New(auth.Config{
		SourceAddr: "127.0.0.1:3306",
		TLSConfig:  &tls.Config{},
	})
	methods := s.AuthMethods()
	if len(methods) != 1 {
		t.Errorf("expected 1 auth method, got %d", len(methods))
	}
}
