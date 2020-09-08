package backend

import (
	"context"
	"errors"
	"time"

	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/pingcap-incubator/weir/pkg/util/sync2"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	ErrNoBackendAddr   = errors.New("no backend addr")
	ErrBackendClosed   = errors.New("backend is closed")
	ErrBackendNotFound = errors.New("backend not found")
)

type BackendConfig struct {
	Addrs        map[string]struct{}
	UserName     string
	Password     string
	Capacity     int
	IdleTimeout  time.Duration
	SelectorType int
}

type BackendImpl struct {
	cfg       *BackendConfig
	connPools map[string]*ConnPool // key: addr
	instances []*Instance
	selector  Selector
	closed    sync2.AtomicBool
}

func NewBackendImpl(cfg *BackendConfig) *BackendImpl {
	return &BackendImpl{
		cfg:    cfg,
		closed: sync2.NewAtomicBool(false),
	}
}

func (b *BackendImpl) Init() error {
	if err := b.initSelector(); err != nil {
		return err
	}
	if err := b.initInstances(); err != nil {
		return err
	}
	if err := b.initConnPools(); err != nil {
		return err
	}
	return nil
}

func (b *BackendImpl) initSelector() error {
	selector, err := CreateSelector(b.cfg.SelectorType)
	if err != nil {
		return err
	}
	b.selector = selector
	return nil
}

func (b *BackendImpl) initInstances() error {
	instances, err := createInstances(b.cfg)
	if err != nil {
		return err
	}
	b.instances = instances
	return nil
}

func (b *BackendImpl) initConnPools() error {
	connPools := make(map[string]*ConnPool)
	for addr := range b.cfg.Addrs {
		poolCfg := &ConnPoolConfig{
			Config:      Config{Addr: addr, UserName: b.cfg.UserName, Password: b.cfg.Password},
			Capacity:    b.cfg.Capacity,
			IdleTimeout: b.cfg.IdleTimeout,
		}
		connPool := NewConnPool(poolCfg)
		connPools[addr] = connPool
	}

	successfulInitConnPoolAddrs := make(map[string]struct{})
	var initConnPoolErr error
	for addr, connPool := range connPools {
		if err := connPool.Init(); err != nil {
			initConnPoolErr = err
			break
		}
		successfulInitConnPoolAddrs[addr] = struct{}{}
	}

	if initConnPoolErr != nil {
		for addr := range successfulInitConnPoolAddrs {
			if err := connPools[addr].Close(); err != nil {
				logutil.BgLogger().Sugar().Error("close inited conn pool error, addr: %s, err: %v", addr, err)
			}
		}
		return initConnPoolErr
	}

	b.connPools = connPools
	return nil
}

func (b *BackendImpl) GetConn(ctx context.Context) (driver.BackendConn, error) {
	if b.closed.Get() {
		return nil, ErrBackendClosed
	}

	instance, err := b.route(b.instances)
	if err != nil {
		return nil, err
	}

	connPool, ok := b.connPools[instance.Addr()]
	if !ok {
		return nil, ErrBackendNotFound
	}

	return connPool.GetConn(ctx)
}

func (b *BackendImpl) PutConn(ctx context.Context, conn driver.BackendConn) error {
	connWrapper := conn.(*connWrapper)
	connPool, ok := b.connPools[connWrapper.addr]
	if !ok {
		return ErrBackendNotFound
	}

	return connPool.PutConn(ctx, conn)
}

func (b *BackendImpl) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}

	for addr, connPool := range b.connPools {
		if err := connPool.Close(); err != nil {
			logutil.BgLogger().Error("close conn pool error, addr: %s, err: %v", zap.String("addr", addr), zap.Error(err))
		}
	}
}

func (b *BackendImpl) route(instances []*Instance) (*Instance, error) {
	instance, err := b.selector.Select(b.instances)
	if err != nil {
		return nil, err
	}

	return instance, nil
}

func createInstances(cfg *BackendConfig) ([]*Instance, error) {
	if len(cfg.Addrs) == 0 {
		return nil, ErrNoBackendAddr
	}

	var ret []*Instance
	for addr := range cfg.Addrs {
		ins := &Instance{addr: addr}
		ret = append(ret, ins)
	}
	return ret, nil
}
