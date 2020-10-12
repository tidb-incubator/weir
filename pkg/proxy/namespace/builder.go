package namespace

import (
	"time"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/proxy/backend"
	"github.com/pingcap-incubator/weir/pkg/util/datastructure"
	"github.com/pingcap/errors"
)

type NsWrapper struct {
	name string
	Backend
	Frontend
}

func BuildNamespace(cfg *config.Namespace) (Namespace, error) {
	be, err := BuildBackend(&cfg.Backend)
	if err != nil {
		return nil, errors.WithMessage(err, "build backend error")
	}
	fe, err := BuildFrontend(&cfg.Frontend)
	if err != nil {
		return nil, errors.WithMessage(err, "build frontend error")
	}

	wrapper := &NsWrapper{
		name:     cfg.Namespace,
		Backend:  be,
		Frontend: fe,
	}
	return wrapper, nil
}

func (n *NsWrapper) Name() string {
	return n.name
}

func BuildBackend(cfg *config.BackendNamespace) (Backend, error) {
	bcfg, err := parseBackendConfig(cfg)
	if err != nil {
		return nil, err
	}

	b := backend.NewBackendImpl(bcfg)
	if err := b.Init(); err != nil {
		return nil, err
	}

	return b, nil
}

func BuildFrontend(cfg *config.FrontendNamespace) (Frontend, error) {
	fns := &FrontendNamespace{
		allowedDBs: cfg.AllowedDBs,
	}
	fns.allowedDBSet = datastructure.StringSliceToSet(cfg.AllowedDBs)

	userPasswds := make(map[string]string)
	for _, u := range cfg.Users {
		userPasswds[u.Username] = u.Password
	}
	fns.userPasswd = userPasswds

	return fns, nil
}

func parseBackendConfig(cfg *config.BackendNamespace) (*backend.BackendConfig, error) {
	selectorType, valid := backend.SelectorNameToType(cfg.SelectorType)
	if !valid {
		return nil, ErrInvalidSelectorType
	}

	addrs := make(map[string]struct{})
	for _, ins := range cfg.Instances {
		addrs[ins] = struct{}{}
	}

	bcfg := &backend.BackendConfig{
		Addrs:        addrs,
		UserName:     cfg.Username,
		Password:     cfg.Password,
		Capacity:     cfg.PoolSize,
		IdleTimeout:  time.Duration(cfg.IdleTimeout) * time.Second,
		SelectorType: selectorType,
	}
	return bcfg, nil
}
