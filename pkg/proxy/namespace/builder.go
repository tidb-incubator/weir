package namespace

import (
	"hash/crc32"
	"time"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/proxy/backend"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	wast "github.com/pingcap-incubator/weir/pkg/util/ast"
	"github.com/pingcap-incubator/weir/pkg/util/datastructure"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
)

type NamespaceImpl struct {
	name string
	Br   driver.Breaker
	Backend
	Frontend
	rateLimiter *NamespaceRateLimiter
}

func BuildNamespace(cfg *config.Namespace) (Namespace, error) {
	be, err := BuildBackend(cfg.Namespace, &cfg.Backend)
	if err != nil {
		return nil, errors.WithMessage(err, "build backend error")
	}
	fe, err := BuildFrontend(&cfg.Frontend)
	if err != nil {
		return nil, errors.WithMessage(err, "build frontend error")
	}
	wrapper := &NamespaceImpl{
		name:     cfg.Namespace,
		Backend:  be,
		Frontend: fe,
	}
	brm, err := NewBreaker(&cfg.Breaker)
	if err != nil {
		return nil, err
	}
	br, err := brm.GetBreaker()
	if err != nil {
		return nil, err
	}
	wrapper.Br = br

	rateLimiter := NewNamespaceRateLimiter(cfg.RateLimiter.Scope, cfg.RateLimiter.QPS)
	wrapper.rateLimiter = rateLimiter

	return wrapper, nil
}

func (n *NamespaceImpl) Name() string {
	return n.name
}

func (n *NamespaceImpl) GetBreaker() (driver.Breaker, error) {
	return n.Br, nil
}

func (n *NamespaceImpl) GetRateLimiter() driver.RateLimiter {
	return n.rateLimiter
}

func BuildBackend(ns string, cfg *config.BackendNamespace) (Backend, error) {
	bcfg, err := parseBackendConfig(cfg)
	if err != nil {
		return nil, err
	}

	b := backend.NewBackendImpl(ns, bcfg)
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

	sqlBlacklist := make(map[uint32]SQLInfo)
	fns.sqlBlacklist = sqlBlacklist

	p := parser.New()
	for _, deniedSQL := range cfg.SQLBlackList {
		stmtNodes, _, err := p.Parse(deniedSQL.SQL, "", "")
		if err != nil {
			return nil, err
		}
		if len(stmtNodes) != 1 {
			return nil, nil
		}
		v, err := wast.ExtractAstVisit(stmtNodes[0])
		if err != nil {
			return nil, err
		}
		fns.sqlBlacklist[crc32.ChecksumIEEE([]byte(v.SqlFeature()))] = SQLInfo{SQL: deniedSQL.SQL}
	}

	sqlWhitelist := make(map[uint32]SQLInfo)
	fns.sqlWhitelist = sqlWhitelist
	for _, allowedSQL := range cfg.SQLWhiteList {
		stmtNodes, _, err := p.Parse(allowedSQL.SQL, "", "")
		if err != nil {
			return nil, err
		}
		if len(stmtNodes) != 1 {
			return nil, nil
		}
		v, err := wast.ExtractAstVisit(stmtNodes[0])
		if err != nil {
			return nil, err
		}
		fns.sqlWhitelist[crc32.ChecksumIEEE([]byte(v.SqlFeature()))] = SQLInfo{SQL: allowedSQL.SQL}
	}

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

func DefaultAsyncCloseNamespace(ns Namespace) error {
	nsWrapper, ok := ns.(*NamespaceImpl)
	if !ok {
		return errors.Errorf("invalid namespace type: %T", ns)
	}
	go func() {
		time.Sleep(30 * time.Second)
		//nsWrapper.BreakerHolder.CloseBreaker()
		nsWrapper.Backend.Close()
	}()
	return nil
}
