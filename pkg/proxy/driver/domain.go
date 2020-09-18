package driver

import (
	"context"

	"github.com/siddontang/go-mysql/mysql"
)

type NamespaceManager interface {
	Auth(username string, pwd, salt []byte) (Namespace, bool)
}

type Namespace interface {
	Name() string
	Auth(username string, pwd, salt []byte) bool
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	GetPooledConn(context.Context) (PooledBackendConn, error)
}

type PooledBackendConn interface {
	PutBack()
	BackendConn
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
	SetAutoCommit(bool) error
	IsAutoCommit() bool
	IsInTransaction() bool
	GetCharset() string
	GetConnectionID() uint32
	GetStatus() uint16
}
