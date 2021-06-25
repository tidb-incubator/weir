package namespace

import (
	"bytes"

	"github.com/tidb-incubator/weir/pkg/util/passwd"
)

type SQLInfo struct {
	SQL string
}

type FrontendNamespace struct {
	allowedDBs   []string
	allowedDBSet map[string]struct{}
	userPasswd   map[string]string
	sqlBlacklist map[uint32]SQLInfo
	sqlWhitelist map[uint32]SQLInfo
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

func (n *FrontendNamespace) ListDatabases() []string {
	ret := make([]string, len(n.allowedDBs))
	copy(ret, n.allowedDBs)
	return ret
}

func (n *FrontendNamespace) IsDeniedSQL(sqlFeature uint32) bool {
	_, ok := n.sqlBlacklist[sqlFeature]
	return ok
}

func (n *FrontendNamespace) IsAllowedSQL(sqlFeature uint32) bool {
	_, ok := n.sqlWhitelist[sqlFeature]
	return ok
}
