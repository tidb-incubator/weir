package namespace

import (
	"bytes"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/util/datastructure"
	"github.com/pingcap-incubator/weir/pkg/util/passwd"
)

type FrontendNamespace struct {
	allowedDBs   []string
	allowedDBSet map[string]struct{}
	userPasswd   map[string]string
}

func CreateFrontendNamespace(namespace string, cfg *config.FrontendNamespace) (*FrontendNamespace, error) {
	fns := &FrontendNamespace{
		allowedDBs: cfg.AllowedDBs,
	}
	fns.allowedDBSet = datastructure.StringSliceToSet(cfg.AllowedDBs)

	userPasswd := make(map[string]string)
	for _, userInfo := range cfg.Users {
		userPasswd[userInfo.Username] = userPasswd[userInfo.Password]
	}
	fns.userPasswd = userPasswd

	return fns, nil
}

func (n *FrontendNamespace) Auth(username string, passwdBytes []byte, salt []byte) bool {
	userPasswd, ok := n.userPasswd[username]
	if !ok {
		return false
	}
	userPasswdBytes := passwd.CalculatePassword(salt, []byte(userPasswd))
	return bytes.Equal(userPasswdBytes, passwdBytes)
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
