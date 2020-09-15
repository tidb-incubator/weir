package server

import (
	"testing"
	"time"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/proxy/backend"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/pingcap-incubator/weir/pkg/proxy/server"
	"github.com/stretchr/testify/assert"
)

type fakeNamespaceManager struct {
	ns *fakeNamespace
}
type fakeNamespace struct {
	fe driver.Frontend
	be driver.Backend
}

type fakeFrontend struct {
}

func newFakeNamespaceManager(ns *fakeNamespace) *fakeNamespaceManager {
	return &fakeNamespaceManager{ns: ns}
}

func newFakeNamespace(fe driver.Frontend, be driver.Backend) *fakeNamespace {
	return &fakeNamespace{
		fe: fe,
		be: be,
	}
}

func newFakeFrontend() *fakeFrontend {
	return &fakeFrontend{}
}

func (d *fakeNamespace) Name() string {
	return "fakeNamespace"
}

func (d *fakeNamespace) Frontend() driver.Frontend {
	return d.fe
}

func (d *fakeNamespace) Backend() driver.Backend {
	return d.be
}

func (*fakeNamespace) Closed() bool {
	return false
}

func (m *fakeNamespaceManager) Auth(username string, pwd, salt []byte) (driver.Namespace, bool) {
	if username != "hello" {
		return nil, false
	}
	return m.ns, true
}

func (*fakeFrontend) Auth(username string, pwd, salt []byte) bool {
	return true
}

func (*fakeFrontend) IsDatabaseAllowed(db string) bool {
	return true
}

func (*fakeFrontend) ListDatabases() []string {
	return []string{"test_db_0", "test_db_1"}
}

func Test_ProxyServer(t *testing.T) {
	cfg := &config.Proxy{
		ProxyServer: config.ProxyServer{
			Addr: "0.0.0.0:7000",
		},
	}

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

	nsmgr := newFakeNamespaceManager(newFakeNamespace(newFakeFrontend(), backendDatabase))
	drv := driver.NewDriverImpl(nsmgr)
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
