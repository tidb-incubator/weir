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

type Config struct {
	Addr     string
	UserName string
	Password string
}

type ConnPool struct {
	cfg  *ConnPoolConfig
	pool *pool.ResourcePool
}

type backendPooledConnWrapper struct {
	*client.Conn
	addr     string
	username string
	pool     *pool.ResourcePool
}

// this struct is only used for fitting pool.Resource interface
type noErrorCloseConnWrapper struct {
	*backendPooledConnWrapper
}

func NewConnPool(cfg *ConnPoolConfig) *ConnPool {
	return &ConnPool{
		cfg: cfg,
	}
}

func newConnWrapper(pool *pool.ResourcePool, conn *client.Conn, addr, username string) *backendPooledConnWrapper {
	return &backendPooledConnWrapper{
		Conn:     conn,
		addr:     addr,
		username: username,
		pool:     pool,
	}
}

func (c *ConnPool) Init() error {
	connFactory := func(context.Context) (pool.Resource, error) {
		// TODO: add connect timeout
		conn, err := client.Connect(c.cfg.Addr, c.cfg.UserName, c.cfg.Password, "")
		if err != nil {
			return nil, err
		}
		return &noErrorCloseConnWrapper{newConnWrapper(c.pool, conn, c.cfg.Addr, c.cfg.UserName)}, nil
	}

	c.pool = pool.NewResourcePool(connFactory, c.cfg.Capacity, c.cfg.Capacity, c.cfg.IdleTimeout, 0, nil)
	return nil
}

func (c *ConnPool) GetConn(ctx context.Context) (driver.PooledBackendConn, error) {
	rs, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	return rs.(*noErrorCloseConnWrapper).backendPooledConnWrapper, nil
}

func (c *ConnPool) Close() error {
	c.pool.Close()
	return nil
}

func (cw *backendPooledConnWrapper) PutBack() {
	w := &noErrorCloseConnWrapper{cw}
	cw.pool.Put(w)
}

func (cw *backendPooledConnWrapper) Close() error {
	return cw.Conn.Close()
}

func (cw *noErrorCloseConnWrapper) Close() {
	if err := cw.backendPooledConnWrapper.Close(); err != nil {
		// TODO: log namespace info
		logutil.BgLogger().Error("close backend conn error", zap.String("addr", cw.addr),
			zap.String("username", cw.username), zap.Error(err))
	}
}
