package driver

import (
	"crypto/tls"

	"github.com/pingcap-incubator/weir/pkg/proxy"
)

type DriverImpl struct {
}

func NewDriverImpl() *DriverImpl {
	return &DriverImpl{}
}

func (*DriverImpl) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState) (server.QueryCtx, error) {
	return NewQueryCtxImpl(), nil
}
