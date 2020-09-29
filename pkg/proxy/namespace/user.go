package namespace

import (
	"fmt"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap/errors"
)

type UserNamespaceMapper struct {
	userToNamespace map[string]string
}

func CreateUserNamespaceMapper(namespaces []*config.Namespace) (*UserNamespaceMapper, error) {
	mapper := make(map[string]string)
	for _, ns := range namespaces {
		frontendNamespace := ns.Frontend
		for _, user := range frontendNamespace.Users {
			originNamespace, ok := mapper[user.Username]
			if ok {
				return nil, errors.WithMessage(ErrDuplicatedUser,
					fmt.Sprintf("user: %s, namespace: %s, %s", user.Username, originNamespace, ns.Namespace))
			}
			mapper[user.Username] = ns.Namespace
		}
	}

	ret := &UserNamespaceMapper{userToNamespace: mapper}
	return ret, nil
}

func (u *UserNamespaceMapper) GetUserNamespace(username string) (string, bool) {
	ns, ok := u.userToNamespace[username]
	return ns, ok
}

func (u *UserNamespaceMapper) Clone() *UserNamespaceMapper {
	ret := make(map[string]string)
	for k, v := range u.userToNamespace {
		ret[k] = v
	}
	return &UserNamespaceMapper{userToNamespace: ret}
}

func (u *UserNamespaceMapper) RemoveNamespaceUsers(ns string) {
	for k, namespace := range u.userToNamespace {
		if ns == namespace {
			delete(u.userToNamespace, k)
		}
	}
}

func (u *UserNamespaceMapper) AddNamespaceUsers(ns string, cfg *config.FrontendNamespace) error {
	for _, userInfo := range cfg.Users {
		if originNamespace, ok := u.userToNamespace[userInfo.Username]; ok {
			return errors.WithMessage(ErrDuplicatedUser, fmt.Sprintf("namespace: %s", originNamespace))
		}
		u.userToNamespace[userInfo.Username] = ns
	}
	return nil
}
