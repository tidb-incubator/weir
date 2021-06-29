package driver

import (
	"crypto/tls"

	"github.com/tidb-incubator/weir/pkg/proxy/server"
)

type DriverImpl struct {
	nsmgr NamespaceManager
}

func NewDriverImpl(nsmgr NamespaceManager) *DriverImpl {
	return &DriverImpl{
		nsmgr: nsmgr,
	}
}

func (d *DriverImpl) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState) (server.QueryCtx, error) {
	return NewQueryCtxImpl(d.nsmgr, connID), nil
}
