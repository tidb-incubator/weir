package proxy

import (
	"testing"
	"time"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/proxy/backend"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/pingcap-incubator/weir/pkg/proxy/server"
	"github.com/stretchr/testify/assert"
)

func Test_ProxyServer(t *testing.T) {
	cfg := &config.Config{}

	backendCfg := &backend.BackendConfig{
		Addrs:        map[string]struct{}{"127.0.0.1:3306": {}},
		UserName:     "root",
		Password:     "123456",
		Capacity:     1,
		IdleTimeout:  0,
		SelectorType: backend.SelectorTypeRandom,
	}
	backendDatabase := backend.NewBackendImpl(backendCfg)
	err := backendDatabase.Init()
	assert.NoError(t, err, "backend init error")
	defer backendDatabase.Close()

	drv := driver.NewDriverImpl(backendDatabase)
	s, err := server.NewServer(cfg, drv)
	assert.NoError(t, err)
	go func() {
		err := s.Run()
		if err != nil {
			t.Logf("server run error: %v", err)
		}
	}()

	defer s.Close()
	time.Sleep(1 * time.Second)
}
