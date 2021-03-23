package errors

import (
	stderrors "errors"
	"testing"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
)

func TestIs(t *testing.T) {
	badConn := mysql.ErrBadConn
	err := errors.Wrapf(badConn, "same error type")
	assert.True(t, Is(err, badConn))
}

func TestStdIs(t *testing.T) {
	badConn := mysql.ErrBadConn
	err := errors.Wrapf(badConn, "another error type")
	assert.False(t, stderrors.Is(err, badConn))
}
