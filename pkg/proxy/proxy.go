package proxy

import (
	"time"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/configcenter"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/pingcap-incubator/weir/pkg/proxy/namespace"
	"github.com/pingcap-incubator/weir/pkg/proxy/server"
)

const defaultCloseBackendDuration = 30 * time.Second

type Proxy struct {
	cfg          *config.Proxy
	svr          *server.Server
	configCenter configcenter.ConfigCenter
}

func NewProxy(cfg *config.Proxy) *Proxy {
	return &Proxy{
		cfg: cfg,
	}
}

func (p *Proxy) Init() error {
	cc, err := configcenter.CreateConfigCenter(p.cfg.ConfigCenter)
	if err != nil {
		return err
	}
	p.configCenter = cc

	nss, err := cc.ListAllNamespace()
	if err != nil {
		return err
	}

	nsmgr, err := namespace.CreateNamespaceManagerImpl(
		nss, namespace.BuildFrontend, namespace.BuildBackend,
		defaultCloseBackendDuration, namespace.DefaultCloseBackendFunc)
	if err != nil {
		return err
	}

	driverImpl := driver.NewDriverImpl(nsmgr)
	svr, err := server.NewServer(p.cfg, driverImpl)
	if err != nil {
		return err
	}

	p.svr = svr
	return nil
}

func (p *Proxy) Run() error {
	return p.svr.Run()
}

func (p *Proxy) Close() {
	p.svr.Close()
}
