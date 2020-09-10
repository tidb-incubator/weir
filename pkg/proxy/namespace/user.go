package namespace

import (
	"fmt"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap/errors"
)

type UserNamespaceMapper struct {
	mapper map[string]string // key: user+password, value: namespace
}

func CreateUserNamespaceMapper(namespaces []*config.Namespace) (*UserNamespaceMapper, error) {
	mapper := make(map[string]string)
	for _, ns := range namespaces {
		frontendNamespace := ns.Frontend
		for _, user := range frontendNamespace.Users {
			key := getUserInfoKey(user.Username, user.Password)
			originNamespace, ok := mapper[key]
			if ok {
				return nil, errors.WithMessage(ErrDuplicatedUser,
					fmt.Sprintf("user: %s, namespace: %s, %s", user.Username, originNamespace, ns.Namespace))
			}
			mapper[key] = ns.Namespace
		}
	}

	ret := &UserNamespaceMapper{mapper: mapper}
	return ret, nil
}

func (u *UserNamespaceMapper) GetUserNamespace(username, password string) (string, bool) {
	key := getUserInfoKey(username, password)
	ns, ok := u.mapper[key]
	return ns, ok
}

func (u *UserNamespaceMapper) Clone() *UserNamespaceMapper {
	ret := make(map[string]string)
	for k, v := range u.mapper {
		ret[k] = v
	}
	return &UserNamespaceMapper{mapper: ret}
}

func (u *UserNamespaceMapper) RemoveNamespaceUsers(ns string) {
	for k, namespace := range u.mapper {
		if ns == namespace {
			delete(u.mapper, k)
		}
	}
}

func (u *UserNamespaceMapper) AddNamespaceUsers(ns string, cfg *config.FrontendNamespace) error {
	for _, userInfo := range cfg.Users {
		key := getUserInfoKey(userInfo.Username, userInfo.Password)
		if originNamespace, ok := u.mapper[key]; ok {
			return errors.WithMessage(ErrDuplicatedUser, fmt.Sprintf("namespace: %s", originNamespace))
		}
		u.mapper[key] = ns
	}
	return nil
}

// TODO: username and password should not contain colon
func getUserInfoKey(username, password string) string {
	return username + ":" + password
}
