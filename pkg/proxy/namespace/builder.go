package namespace

import (
	"time"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/proxy/backend"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	"github.com/pingcap-incubator/weir/pkg/util/datastructure"
)

func BuildBackend(cfg *config.BackendNamespace) (driver.Backend, error) {
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

func BuildFrontend(cfg *config.FrontendNamespace) (driver.Frontend, error) {
	fns := &FrontendNamespace{
		allowedDBs: cfg.AllowedDBs,
	}
	fns.allowedDBSet = datastructure.StringSliceToSet(cfg.AllowedDBs)
	return fns, nil
}

func parseBackendConfig(cfg *config.BackendNamespace) (*backend.BackendConfig, error) {
	selectorType, valid := backend.SelectorNameToType(cfg.SelectorType)
	if !valid {
		return nil, ErrInvalidSelectorType
	}
	bcfg := &backend.BackendConfig{
		UserName:     cfg.Username,
		Password:     cfg.Password,
		Capacity:     cfg.PoolSize,
		IdleTimeout:  time.Duration(cfg.IdleTimeout) * time.Second,
		SelectorType: selectorType,
	}
	return bcfg, nil
}
