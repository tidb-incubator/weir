package backend

import (
	"context"
	"time"

	"github.com/pingcap-incubator/weir/pkg/proxy/backend/client"
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
	conn     *client.Conn
	addr     string
	username string
}

func NewConnPool(cfg *ConnPoolConfig) *ConnPool {
	return &ConnPool{
		cfg: cfg,
	}
}

func newConnWrapper(conn *client.Conn, addr, username string) *connWrapper {
	return &connWrapper{
		conn:     conn,
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
		return newConnWrapper(conn, c.cfg.Addr, c.cfg.UserName), nil
	}

	c.pool = pool.NewResourcePool(connFactory, c.cfg.Capacity, c.cfg.Capacity, c.cfg.IdleTimeout, 0, nil)
	return nil
}

func (c *ConnPool) GetConn(ctx context.Context) (*client.Conn, error) {
	rs, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	return rs.(*connWrapper).conn, nil
}

func (c *ConnPool) PutConn(ctx context.Context, conn *client.Conn) error {
	w := newConnWrapper(conn, c.cfg.Addr, c.cfg.UserName)
	// FIXME: put into a full pool may cause panic!
	c.pool.Put(w)
	return nil
}

func (c *ConnPool) Close() error {
	c.pool.Close()
	return nil
}

func (cw *connWrapper) Close() {
	if err := cw.conn.Close(); err != nil {
		// TODO: log namespace info
		logutil.BgLogger().Error("close backend conn error", zap.String("addr", cw.addr),
			zap.String("username", cw.username), zap.Error(err))
	}
}
