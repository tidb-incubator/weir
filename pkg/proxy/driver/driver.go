package driver

import (
	"crypto/tls"

	"github.com/pingcap-incubator/weir/pkg/proxy/server"
)

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
