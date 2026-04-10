package rows

import (
	"io"

	gmssql "github.com/dolthub/go-mysql-server/sql"
)

// Iter wraps a []gmssql.Row slice to implement gmssql.RowIter.
type Iter struct {
	rows []gmssql.Row
	pos  int
}

// NewIter creates a new Iter from a slice of rows.
func NewIter(rows []gmssql.Row) *Iter {
	return &Iter{rows: rows}
}

func (i *Iter) Next(_ *gmssql.Context) (gmssql.Row, error) {
	if i.pos >= len(i.rows) {
		return nil, io.EOF
	}
	row := i.rows[i.pos]
	i.pos++
	return row, nil
}

func (i *Iter) Close(_ *gmssql.Context) error { return nil }
