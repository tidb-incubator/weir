package namespace

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/pingcap-incubator/weir/pkg/proxy/metrics"
	"github.com/pingcap/errors"
)

type NamespaceHolder struct {
	nss map[string]Namespace
}

type NamespaceWrapper struct {
	nsmgr       *NamespaceManager
	name        string
	connCounter int64
}

func CreateNamespaceHolder(cfgs []*config.Namespace, build NamespaceBuilder) (*NamespaceHolder, error) {
	nss := make(map[string]Namespace, len(cfgs))

	for _, cfg := range cfgs {
		ns, err := build(cfg)
		if err != nil {
			return nil, errors.WithMessage(err, fmt.Sprintf("create namespace error, namespace: %s", cfg.Namespace))
		}
		nss[cfg.Namespace] = ns
	}

	holder := &NamespaceHolder{
		nss: nss,
	}
	return holder, nil
}

func (n *NamespaceHolder) Get(name string) (Namespace, bool) {
	ns, ok := n.nss[name]
	return ns, ok
}

func (n *NamespaceHolder) Set(name string, ns Namespace) {
	n.nss[name] = ns
}

func (n *NamespaceHolder) Delete(name string) {
	delete(n.nss, name)
}

func (n *NamespaceHolder) Clone() *NamespaceHolder {
	nss := make(map[string]Namespace)
	for name, ns := range n.nss {
		nss[name] = ns
	}
	return &NamespaceHolder{
		nss: nss,
	}
}

func (n *NamespaceWrapper) Name() string {
	return n.name
}

func (n *NamespaceWrapper) IsDatabaseAllowed(db string) bool {
	return n.mustGetCurrentNamespace().IsDatabaseAllowed(db)
}

func (n *NamespaceWrapper) ListDatabases() []string {
	return n.mustGetCurrentNamespace().ListDatabases()
}

func (n *NamespaceWrapper) GetPooledConn(ctx context.Context) (driver.PooledBackendConn, error) {
	return n.mustGetCurrentNamespace().GetPooledConn(ctx)
}

func (n *NamespaceWrapper) IncrConnCount() {
	currCnt := atomic.AddInt64(&n.connCounter, 1)
	metrics.QueryCtxGauge.WithLabelValues(n.name).Set(float64(currCnt))
}

func (n *NamespaceWrapper) DescConnCount() {
	currCnt := atomic.AddInt64(&n.connCounter, -1)
	metrics.QueryCtxGauge.WithLabelValues(n.name).Set(float64(currCnt))
}

func (n *NamespaceWrapper) Closed() bool {
	_, ok := n.nsmgr.getCurrentNamespaces().Get(n.name)
	return !ok
}

func (n *NamespaceWrapper) mustGetCurrentNamespace() Namespace {
	ns, ok := n.nsmgr.getCurrentNamespaces().Get(n.name)
	if !ok {
		panic(errors.New("namespace not found"))
	}
	return ns
}
