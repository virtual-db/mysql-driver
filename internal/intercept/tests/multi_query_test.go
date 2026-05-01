package intercept_test

import (
	"context"
	"testing"

	vitessmysql "github.com/dolthub/vitess/go/mysql"

	. "github.com/virtual-db/vdb-mysql-driver/internal/intercept"
)

func TestMultiQuery_CallsQueryReceived(t *testing.T) {
	var called bool

	b := &stubEventBridge{
		queryReceived: func(_ uint32, _ string, _ string) (string, error) {
			called = true
			return "", nil
		},
	}
	qi := NewInterceptor(b)

	_, err := qi.MultiQuery(context.Background(), &stubChain{}, &vitessmysql.Conn{}, "SELECT 1; SELECT 2", noopQueryCallback)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("QueryReceived was not called from MultiQuery")
	}
}
