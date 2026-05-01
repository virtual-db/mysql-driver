package auth_test

import (
	"context"
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	vitessmysql "github.com/dolthub/vitess/go/mysql"

	"github.com/virtual-db/vdb-mysql-driver/internal/auth"
)

func TestNoCacheStorage_AlwaysReturnsAuthNeedMoreData(t *testing.T) {
	s := auth.NewNoCacheStorageForTest()
	_, state, err := s.UserEntryWithCacheHash(nil, nil, "", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != vitessmysql.AuthNeedMoreData {
		t.Errorf("expected AuthNeedMoreData, got %v", state)
	}
}

func TestAcceptAllValidator_AlwaysReturnsTrue(t *testing.T) {
	v := auth.NewAcceptAllValidatorForTest()
	if !v.HandleUser("anyone", nil) {
		t.Error("expected HandleUser to return true for any user")
	}
	if !v.HandleUser("", nil) {
		t.Error("expected HandleUser to return true for empty username")
	}
	if !v.HandleUser("root", nil) {
		t.Error("expected HandleUser to return true for root")
	}
}

func TestFetchGrants_ReturnsScopesFromDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"Grants for current_user()"}).
		AddRow("GRANT ALL PRIVILEGES ON *.* TO 'alice'@'%'").
		AddRow("GRANT SELECT ON `db`.* TO 'alice'@'%'")

	mock.ExpectQuery("SHOW GRANTS FOR CURRENT_USER\\(\\)").WillReturnRows(rows)

	scopes, err := auth.FetchGrantsForTest(context.Background(), db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(scopes) != 2 {
		t.Fatalf("expected 2 scopes, got %d", len(scopes))
	}
	if scopes[0] != "GRANT ALL PRIVILEGES ON *.* TO 'alice'@'%'" {
		t.Errorf("scopes[0]: got %q", scopes[0])
	}
	if scopes[1] != "GRANT SELECT ON `db`.* TO 'alice'@'%'" {
		t.Errorf("scopes[1]: got %q", scopes[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

func TestFetchGrants_EmptyResult_ReturnsNilSlice(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"Grants for current_user()"})
	mock.ExpectQuery("SHOW GRANTS FOR CURRENT_USER\\(\\)").WillReturnRows(rows)

	scopes, err := auth.FetchGrantsForTest(context.Background(), db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(scopes) != 0 {
		t.Errorf("expected empty/nil scopes, got %v", scopes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

func TestFetchGrants_DBError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS FOR CURRENT_USER\\(\\)").
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = auth.FetchGrantsForTest(context.Background(), db)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}
