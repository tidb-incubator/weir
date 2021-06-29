package backend

import (
	"context"
	"testing"
	"time"

	"github.com/tidb-incubator/weir/pkg/proxy/backend"
	"github.com/stretchr/testify/require"
)

func TestConnPool_ErrorClose_Success(t *testing.T) {
	cfg := backend.ConnPoolConfig{
		Config: backend.Config{
			Addr:"127.0.0.1:3306",
			UserName:"root",
			Password:"123456",
		},
		Capacity:1, // pool size is set to 1
		IdleTimeout:0,
	}
	pool := backend.NewConnPool("test", &cfg)
	err := pool.Init()
	require.NoError(t, err)

	ctx, cancelFunc := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelFunc()

	conn1, err := pool.GetConn(ctx)
	require.NoError(t, err)

	// conn is closed, and another conn is created by pool
	err = conn1.ErrorClose()
	require.NoError(t, err)

	conn2, err := pool.GetConn(ctx)
	require.NoError(t, err)

	err = conn2.ErrorClose()
	require.NoError(t, err)
}

