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
	Frontend() Frontend
	Backend() Backend
	Closed() bool
}

type Backend interface {
	GetConn(context.Context) (BackendConn, error)
	GetPooledConn(context.Context) (PooledBackendConn, error)
	Close()
}

type Frontend interface {
	Auth(username string, pwd, salt []byte) bool
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
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
	SetAutoCommit() error
	IsAutoCommit() bool
	IsInTransaction() bool
	GetCharset() string
	GetConnectionID() uint32
	GetStatus() uint16
}
