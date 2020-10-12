package proxy

import (
	"time"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/configcenter"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/pingcap-incubator/weir/pkg/proxy/namespace"
	"github.com/pingcap-incubator/weir/pkg/proxy/server"
)

type Proxy struct {
	cfg          *config.Proxy
	svr          *server.Server
	apiServer    *HttpApiServer
	nsmgr        *namespace.NamespaceManager
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

	nsmgr, err := namespace.CreateNamespaceManager(nss, namespace.BuildNamespace, namespace.DefaultAsyncCloseNamespace)
	if err != nil {
		return err
	}
	p.nsmgr = nsmgr

	driverImpl := driver.NewDriverImpl(nsmgr)
	svr, err := server.NewServer(p.cfg, driverImpl)
	if err != nil {
		return err
	}
	p.svr = svr

	apiServer, err := CreateHttpApiServer(svr, nsmgr, cc, p.cfg)
	if err != nil {
		return err
	}
	p.apiServer = apiServer

	return nil
}

// TODO(eastfisher): refactor this function
func (p *Proxy) Run() error {
	go func() {
		time.Sleep(200 * time.Millisecond)
		p.apiServer.Run()
	}()
	return p.svr.Run()
}

func (p *Proxy) Close() {
	if p.apiServer != nil {
		p.apiServer.Close()
	}
	if p.svr != nil {
		p.svr.Close()
	}
}
