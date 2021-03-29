package namespace

import (
	"bytes"
	"hash/crc32"

	"github.com/pingcap-incubator/weir/pkg/config"
	wast "github.com/pingcap-incubator/weir/pkg/util/ast"
	"github.com/pingcap-incubator/weir/pkg/util/datastructure"
	"github.com/pingcap-incubator/weir/pkg/util/passwd"
	"github.com/pingcap/parser"
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
	fns.sqlWhitelist = sqlWhitelist

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
