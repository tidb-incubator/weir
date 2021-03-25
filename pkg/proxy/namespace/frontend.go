package namespace

import (
	"bytes"
	"hash/crc32"
	"strings"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/util/datastructure"
	"github.com/pingcap-incubator/weir/pkg/util/passwd"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type FrontendNamespace struct {
	allowedDBs   []string
	allowedDBSet map[string]struct{}
	userPasswd   map[string]string
	deniedSQLs   map[uint32]struct{}
}

type frontendSqlVisitor struct{}

func (v *frontendSqlVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch in := in.(type) {
	case *ast.PatternInExpr:
		if len(in.List) == 0 {
			return in, false
		}
		if _, ok := in.List[0].(*driver.ValueExpr); ok {
			in.List = in.List[:1]
		}
	case *driver.ValueExpr:
		in.SetValue("?")
	}

	return in, false
}

func (v *frontendSqlVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func CreateFrontendNamespace(cfg *config.FrontendNamespace) (*FrontendNamespace, error) {
	fns := &FrontendNamespace{
		allowedDBs: cfg.AllowedDBs,
	}
	fns.allowedDBSet = datastructure.StringSliceToSet(cfg.AllowedDBs)

	userPasswd := make(map[string]string)
	for _, userInfo := range cfg.Users {
		userPasswd[userInfo.Username] = userPasswd[userInfo.Password]
	}
	fns.userPasswd = userPasswd

	fns.deniedSQLs = make(map[uint32]struct{})

	p := parser.New()
	sb := strings.Builder{}
	for _, sql := range cfg.DeniedSQLs {

		stmtNodes, _, err := p.Parse(sql, "", "")
		if err != nil {
			return nil, err
		}
		for _, stmtNode := range stmtNodes {
			v := frontendSqlVisitor{}
			sb.Reset()
			stmtNode.Accept(&v)
			stmtNode.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
			fns.deniedSQLs[crc32.ChecksumIEEE([]byte(sb.String()))] = struct{}{}
		}
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
func (n *FrontendNamespace) GetDeniedSQLs() map[uint32]struct{} {
	return n.deniedSQLs
}
