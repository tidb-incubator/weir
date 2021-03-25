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
	GetPooledConn(context.Context) (driver.PooledBackendConn, error)
	Close()
	GetBreaker() (driver.Breaker, error)
}

type Frontend interface {
	Auth(username string, passwdBytes []byte, salt []byte) bool
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
}

type Backend interface {
	Close()
	GetPooledConn(context.Context) (driver.PooledBackendConn, error)
}

type BreakerHolder interface {
	GetBreaker() (driver.Breaker, error)
}
