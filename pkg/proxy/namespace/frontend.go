package namespace

import (
	"bytes"

	"github.com/pingcap-incubator/weir/pkg/config"
	wast "github.com/pingcap-incubator/weir/pkg/util/ast"
	"github.com/pingcap-incubator/weir/pkg/util/datastructure"
	"github.com/pingcap-incubator/weir/pkg/util/passwd"
	"github.com/pingcap/parser"
	"hash/crc32"
	"time"
)

type DeniedSQLInfo struct {
	Sql string
	Ttl int64
}

type FrontendNamespace struct {
	allowedDBs   []string
	allowedDBSet map[string]struct{}
	userPasswd   map[string]string
	deniedSQLs   map[uint32]DeniedSQLInfo
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

	deniedSQLs := make(map[uint32]DeniedSQLInfo)
	fns.deniedSQLs = deniedSQLs

	p := parser.New()
	for _, deniedSQL := range cfg.DeniedSQLs {
		stmtNodes, _, err := p.Parse(deniedSQL.Sql, "", "")
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
		if deniedSQL.Ttl != 0 {
			deniedSQL.Ttl = time.Now().Unix() + deniedSQL.Ttl
		}
		fns.deniedSQLs[crc32.ChecksumIEEE([]byte(v.SqlFeature()))] = DeniedSQLInfo{Sql: deniedSQL.Sql, Ttl: deniedSQL.Ttl}
	}

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
	if val, ok := n.deniedSQLs[sqlFeature]; ok {
		if val.Ttl == 0 {
			return true
		}
		if time.Now().Unix() > val.Ttl {
			return false
		}
		return true
	}
	return false
}
