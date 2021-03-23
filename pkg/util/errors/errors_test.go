package errors

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
)

func TestIs(t *testing.T) {
	badConn := mysql.ErrBadConn
	err := errors.AddStack(badConn)
	t.Log(Is(err, badConn))
}
