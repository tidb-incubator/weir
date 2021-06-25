package namespace

import (
	"context"

	"github.com/tidb-incubator/weir/pkg/proxy/driver"
)

type Namespace interface {
	Name() string
	Auth(username string, passwdBytes []byte, salt []byte) bool
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	IsDeniedSQL(sqlFeature uint32) bool
	IsAllowedSQL(sqlFeature uint32) bool
	GetPooledConn(context.Context) (driver.PooledBackendConn, error)
	Close()
	GetBreaker() (driver.Breaker, error)
	GetRateLimiter() driver.RateLimiter
}

type Frontend interface {
	Auth(username string, passwdBytes []byte, salt []byte) bool
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	IsDeniedSQL(sqlFeature uint32) bool
	IsAllowedSQL(sqlFeature uint32) bool
}

type Backend interface {
	Close()
	GetPooledConn(context.Context) (driver.PooledBackendConn, error)
}
