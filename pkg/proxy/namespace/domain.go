package namespace

import (
	"context"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
)

type Namespace interface {
	Name() string
	Auth(username string, passwdBytes []byte, salt []byte) bool
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	GetDeniedSQLs() map[uint32]struct{}
	GetPooledConn(context.Context) (driver.PooledBackendConn, error)
	Close()
	GetBreaker() (driver.Breaker, error)
}

type Frontend interface {
	Auth(username string, passwdBytes []byte, salt []byte) bool
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	GetDeniedSQLs() map[uint32]struct{}
}

type Backend interface {
	Close()
	GetPooledConn(context.Context) (driver.PooledBackendConn, error)
}
