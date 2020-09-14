package proxy

import (
	"io/ioutil"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/proxy"
	"github.com/stretchr/testify/assert"
)

func Test_CreateProxyFromConfig(t *testing.T) {
	_, localFile, _, _ := runtime.Caller(0)
	currDir := filepath.Dir(localFile)
	proxyConfigFileName := filepath.Join(currDir, "proxy.yaml")
	proxyConfigData, err := ioutil.ReadFile(proxyConfigFileName)
	assert.NoError(t, err)

	proxyCfg, err := config.UnmarshalProxyConfig(proxyConfigData)
	assert.NoError(t, err)

	p := proxy.NewProxy(proxyCfg)

	err = p.Init()
	assert.NoError(t, err)

	go func() {
		if err := p.Run(); err != nil {
			t.Errorf("proxy run error: %v", err)
			t.FailNow()
		}
	}()

	time.Sleep(60 * time.Second)
	p.Close()
}
