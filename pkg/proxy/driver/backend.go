package driver

import (
	"context"

	"github.com/siddontang/go-mysql/mysql"
)

type Backend interface {
	GetConn(context.Context) (BackendConn, error)
	PutConn(context.Context, BackendConn) error
}

type BackendConn interface {
	Close() error
	Ping() error
	UseDB(dbName string) error
	GetDB() string
	Execute(command string, args ...interface{}) (*mysql.Result, error)
	Begin() error
	Commit() error
	Rollback() error
	SetCharset(charset string) error
	FieldList(table string, wildcard string) ([]*mysql.Field, error)
	SetAutoCommit() error
	IsAutoCommit() bool
	IsInTransaction() bool
	GetCharset() string
	GetConnectionID() uint32
	GetStatus() uint16
}
