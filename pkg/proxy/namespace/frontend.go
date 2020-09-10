package namespace

import (
	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/util/datastructure"
)

type FrontendNamespace struct {
	allowedDBs   []string
	allowedDBSet map[string]struct{}
}

func CreateFrontendNamespace(namespace string, cfg *config.FrontendNamespace) (*FrontendNamespace, error) {
	fns := &FrontendNamespace{
		allowedDBs: cfg.AllowedDBs,
	}
	fns.allowedDBSet = datastructure.StringSliceToSet(cfg.AllowedDBs)
	return fns, nil
}

func (n *FrontendNamespace) IsDatabaseAllowed(db string) bool {
	_, ok := n.allowedDBSet[db]
	return ok
}

func (n *FrontendNamespace) ListAllowedDatabases() []string {
	ret := make([]string, len(n.allowedDBs))
	copy(ret, n.allowedDBs)
	return ret
}
