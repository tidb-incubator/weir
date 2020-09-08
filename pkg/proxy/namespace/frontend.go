package namespace

import (
	"fmt"
	"sync"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/util/datastructure"
	"github.com/pingcap/errors"
)

var (
	ErrDuplicateUser = errors.New("duplicated user")
)

type UserNamespaceMapper struct {
	mapper map[string]string // key: user+passwd, value: namespace
	lock   sync.RWMutex
}

type FrontendNamespace struct {
	name         string
	allowedDBs   []string
	allowedDBSet map[string]struct{}
}

func CreateUserNamespaceMapper(namespaces []*config.Namespace) (*UserNamespaceMapper, error) {
	mapper := make(map[string]string)
	for _, ns := range namespaces {
		frontendNamespace := ns.Frontend
		for _, user := range frontendNamespace.Users {
			key := getUserInfoKey(user.Username, user.Password)
			originNamespace, ok := mapper[key]
			if ok {
				return nil, errors.WithMessage(ErrDuplicateUser,
					fmt.Sprintf("user: %s, namespace: %s, %s", user.Username, originNamespace, ns.Namespace))
			}
			mapper[key] = ns.Namespace
		}
	}

	ret := &UserNamespaceMapper{mapper: mapper}
	return ret, nil
}

func CreateFrontendNamespace(namespace string, cfg *config.FrontendNamespace) (*FrontendNamespace, error) {
	fns := &FrontendNamespace{
		name:       namespace,
		allowedDBs: cfg.AllowedDBs,
	}
	fns.allowedDBSet = datastructure.StringSliceToSet(cfg.AllowedDBs)
	return fns, nil
}

func (m *UserNamespaceMapper) GetUserNamespace(username, password string) (string, bool) {
	key := getUserInfoKey(username, password)
	m.lock.RLock()
	ns, ok := m.mapper[key]
	m.lock.RUnlock()
	return ns, ok
}

func (n *FrontendNamespace) Name() string {
	return n.name
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

// TODO: username and password should not contain colon
func getUserInfoKey(username, password string) string {
	return username + ":" + password
}
