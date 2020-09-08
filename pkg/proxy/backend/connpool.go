package backend

import (
	"context"
	"time"

	"github.com/pingcap-incubator/weir/pkg/proxy/backend/client"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/pingcap-incubator/weir/pkg/util/pool"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type ConnPoolConfig struct {
	Config
	Capacity    int
	IdleTimeout time.Duration
}

type ConnPool struct {
	cfg  *ConnPoolConfig
	pool *pool.ResourcePool
}

type connWrapper struct {
	*client.Conn
	addr     string
	username string
}

// this struct is only used for fitting pool.Resource interface
type noErrorCloseConnWrapper struct {
	*connWrapper
}

func NewConnPool(cfg *ConnPoolConfig) *ConnPool {
	return &ConnPool{
		cfg: cfg,
	}
}

func newConnWrapper(conn *client.Conn, addr, username string) *connWrapper {
	return &connWrapper{
		Conn:     conn,
		addr:     addr,
		username: username,
	}
}

func (c *ConnPool) Init() error {
	connFactory := func(context.Context) (pool.Resource, error) {
		// TODO: add connect timeout
		conn, err := client.Connect(c.cfg.Addr, c.cfg.UserName, c.cfg.Password, "")
		if err != nil {
			return nil, err
		}
		return &noErrorCloseConnWrapper{newConnWrapper(conn, c.cfg.Addr, c.cfg.UserName)}, nil
	}

	c.pool = pool.NewResourcePool(connFactory, c.cfg.Capacity, c.cfg.Capacity, c.cfg.IdleTimeout, 0, nil)
	return nil
}

func (c *ConnPool) GetConn(ctx context.Context) (driver.BackendConn, error) {
	rs, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	return rs.(*noErrorCloseConnWrapper).connWrapper, nil
}

func (c *ConnPool) PutConn(ctx context.Context, conn driver.BackendConn) error {
	w := &noErrorCloseConnWrapper{conn.(*connWrapper)}
	// FIXME: put into a full pool may cause panic!
	c.pool.Put(w)
	return nil
}

func (c *ConnPool) Close() error {
	c.pool.Close()
	return nil
}

func (cw *connWrapper) Close() error {
	return cw.Conn.Close()
}

func (cw *noErrorCloseConnWrapper) Close() {
	if err := cw.connWrapper.Close(); err != nil {
		// TODO: log namespace info
		logutil.BgLogger().Error("close backend conn error", zap.String("addr", cw.addr),
			zap.String("username", cw.username), zap.Error(err))
	}
}
