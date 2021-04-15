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

func TestCheckAndGetMyError_True(t *testing.T) {
	myErr := mysql.NewError(1105, "unknown")
	err, is := CheckAndGetMyError(myErr)
	assert.True(t, is)
	assert.NotNil(t, err)
}

func TestCheckAndGetMyError_False(t *testing.T) {
	myErr := errors.New("not a myError")
	err, is := CheckAndGetMyError(myErr)
	assert.False(t, is)
	assert.Nil(t, err)
}

func TestCheckAndGetMyError_Cause_True(t *testing.T) {
	myErr := errors.Wrapf(mysql.NewError(1105, "unknown"), "wrap error")
	err, is := CheckAndGetMyError(myErr)
	assert.True(t, is)
	assert.NotNil(t, err)
}
