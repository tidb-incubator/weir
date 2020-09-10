package backend

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap-incubator/weir/pkg/proxy/backend"
	"github.com/pingcap-incubator/weir/pkg/util/pool"
	"github.com/stretchr/testify/assert"
)

const (
	testBackendAddr     = "localhost:3306"
	testBackendUserName = "root"
	testBackendPassword = "123456"
)

func TestConnPool_OneConn_GetAndPut_Success(t *testing.T) {
	cfg := getTestConnPoolConfig(testBackendAddr, testBackendUserName, testBackendPassword, 1, 0)
	connPool := backend.NewConnPool(cfg)
	err := connPool.Init()
	assert.NoError(t, err)
	defer connPool.Close()

	ctx := context.Background()
	conn, err := connPool.GetConn(ctx)
	assert.NoError(t, err)
	defer conn.PutBack()
	err = conn.Ping()
	assert.NoError(t, err)
}

func TestConnPool_OneConn_Concurrent_Get_Error(t *testing.T) {
	cfg := getTestConnPoolConfig(testBackendAddr, testBackendUserName, testBackendPassword, 1, 1)
	connPool := backend.NewConnPool(cfg)
	err := connPool.Init()
	assert.NoError(t, err)
	defer connPool.Close()

	ctx := context.Background()
	conn, err := connPool.GetConn(ctx)
	assert.NoError(t, err)
	defer conn.PutBack()

	err = conn.Ping()
	assert.NoError(t, err)

	ctx2, cancelFunc := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancelFunc()
	conn2, err := connPool.GetConn(ctx2)
	assert.Nil(t, conn2)
	assert.EqualError(t, err, pool.ErrTimeout.Error())
}

func TestConnPool_TwoConn_GetAndPut_Success(t *testing.T) {
	cfg := getTestConnPoolConfig(testBackendAddr, testBackendUserName, testBackendPassword, 2, 0)
	connPool := backend.NewConnPool(cfg)
	err := connPool.Init()
	assert.NoError(t, err)
	defer connPool.Close()

	ctx := context.Background()
	conn, err := connPool.GetConn(ctx)
	assert.NoError(t, err)
	defer conn.PutBack()
	err = conn.Ping()
	assert.NoError(t, err)

	conn2, err := connPool.GetConn(ctx)
	assert.NoError(t, err)
	defer conn.PutBack()
	err = conn2.Ping()
	assert.NoError(t, err)
}

func getTestConnPoolConfig(addr, username, password string, capacity int, idleTimeout time.Duration) *backend.ConnPoolConfig {
	cfg := &backend.ConnPoolConfig{
		Config: backend.Config{
			Addr:     addr,
			UserName: username,
			Password: password,
		},
		Capacity:    capacity,
		IdleTimeout: idleTimeout,
	}
	return cfg
}
