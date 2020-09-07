package backend

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/weir/pkg/proxy/backend/client"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/pingcap-incubator/weir/pkg/util/sync2"
)

type Config struct {
	Addr     string
	UserName string
	Password string
}

// implement a single connection database for demo
type SingleConnDatabaseImpl struct {
	cfg   *Config
	conn  *client.Conn
	inUse sync2.AtomicBool
}

func NewSingleConnDatabaseImpl(cfg *Config) *SingleConnDatabaseImpl {
	return &SingleConnDatabaseImpl{
		cfg: cfg,
	}
}

func (d *SingleConnDatabaseImpl) Init() error {
	conn, err := client.Connect(d.cfg.Addr, d.cfg.UserName, d.cfg.Password, "")
	if err != nil {
		return err
	}
	d.conn = conn
	return nil
}

func (d *SingleConnDatabaseImpl) GetConn(ctx context.Context) (driver.BackendConn, error) {
	if !d.inUse.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("SingleConnDatabase conn in use")
	}
	return d.conn, nil
}

func (d *SingleConnDatabaseImpl) PutConn(ctx context.Context, conn driver.BackendConn) error {
	if !d.inUse.CompareAndSwap(true, false) {
		return fmt.Errorf("SingleConnDatabase conn is already put back")
	}
	return nil
}

func (d *SingleConnDatabaseImpl) Close() error {
	return d.conn.Close()
}
