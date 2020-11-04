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
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	GetPooledConn(context.Context) (PooledBackendConn, error)
}

type PooledBackendConn interface {
	// PutBack put conn back to pool
	PutBack()

	// ErrorClose close conn and connpool create a new conn
	// call this function when conn is broken.
	ErrorClose() error
	BackendConn
}

type SimpleBackendConn interface {
	Close() error
	BackendConn
}

type BackendConn interface {
	Ping() error
	UseDB(dbName string) error
	GetDB() string
	Execute(command string, args ...interface{}) (*mysql.Result, error)
	Begin() error
	Commit() error
	Rollback() error
	StmtPrepare(sql string) (Stmt, error)
	StmtExecuteForward(data []byte) (*mysql.Result, error)
	StmtClosePrepare(stmtId int) error
	SetCharset(charset string) error
	FieldList(table string, wildcard string) ([]*mysql.Field, error)
	SetAutoCommit(bool) error
	IsAutoCommit() bool
	IsInTransaction() bool
	GetCharset() string
	GetConnectionID() uint32
	GetStatus() uint16
}

type Stmt interface {
	ID() int
	ParamNum() int
	ColumnNum() int
}
