package schema_test

import (
	"errors"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	. "github.com/virtual-db/mysql-driver/internal/schema"
)

func TestGetSchema_ConcurrentCallsDoNotRace(t *testing.T) {
	const goroutines = 10
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	type col = struct {
		name          string
		dataType      string
		columnType    string
		isNullable    string
		columnKey     string
		columnDefault *string
		extra         string
		charMaxLength *int64
	}

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			db, mock, err := sqlmock.New()
			if err != nil {
				errs <- err
				return
			}
			defer db.Close()

			mock.ExpectQuery("SELECT").
				WithArgs("testdb", "shared").
				WillReturnRows(columnRows(
					col{"id", "int", "int", "NO", "PRI", nil, "auto_increment", nil},
					col{"val", "varchar", "varchar(32)", "YES", "", nil, "", i64Ptr(32)},
				))

			mock.ExpectQuery("SELECT COLUMN_NAME").
				WithArgs("testdb", "shared").
				WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id"))

			p := NewSQLProvider(db, "testdb")
			cols, _, err := p.GetSchema("shared")
			if err != nil {
				errs <- err
				return
			}
			if len(cols) == 0 {
				errs <- errors.New("got empty columns")
			}
		}()
	}

	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("goroutine error: %v", e)
	}
}

func TestNewSQLSchemaProvider_ReturnsSchemaProvider(t *testing.T) {
	db, _ := newMockDB(t)
	var _ Provider = NewSQLProvider(db, "testdb")
}
