package namespace

import (
	"fmt"
	"sync"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap/errors"
)

var (
	ErrDuplicateUser = errors.New("duplicated user")
)

type UserNamespaceMapper struct {
	mapper map[string]string // key: user+passwd, value: namespace
	lock   sync.RWMutex
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

func (m *UserNamespaceMapper) GetUserNamespace(username, password string) (string, bool) {
	key := getUserInfoKey(username, password)
	m.lock.RLock()
	ns, ok := m.mapper[key]
	m.lock.RUnlock()
	return ns, ok
}

// TODO: username and password should not contain colon
func getUserInfoKey(username, password string) string {
	return username + ":" + password
}
