package driver

import (
	"context"
	"crypto/tls"

	"github.com/pingcap-incubator/weir/pkg/proxy/server"
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

type DriverImpl struct {
	backend Backend
}

func NewDriverImpl(backend Backend) *DriverImpl {
	return &DriverImpl{
		backend: backend,
	}
}

func (d *DriverImpl) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState) (server.QueryCtx, error) {
	return NewQueryCtxImpl(d.backend), nil
}
