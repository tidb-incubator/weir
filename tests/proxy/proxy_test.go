package proxy

import (
	"testing"
	"time"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/proxy"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/stretchr/testify/assert"
)

func Test_ProxyServer(t *testing.T) {
	cfg := &config.Config{}
	drv := driver.NewDriverImpl()
	s, err := proxy.NewServer(cfg, drv)
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
