package ast

import (
	"context"
	"encoding/binary"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

const ctxAstTableNameKey = "ctx_ast_table_name"

func CtxWithAstTableName(ctx context.Context, tableName string) context.Context {
	return context.WithValue(ctx, ctxAstTableNameKey, tableName)
}

func GetAstTableNameFromCtx(ctx context.Context) (string, bool) {
	tableName := ctx.Value(ctxAstTableNameKey)
	if tableName == nil {
		return "", false
	}
	tableNameStr, ok := tableName.(string)
	if !ok {
		return "", false
	}
	return tableNameStr, true
}

type FirstTableNameVisitor struct {
	table string
	found bool
}

func (f *FirstTableNameVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch nn := n.(type) {
	case *ast.TableName:
		f.table = nn.Name.String()
		f.found = true
		return n, true
	}
	return n, false
}

func (f *FirstTableNameVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, !f.found
}

func (f *FirstTableNameVisitor) TableName() string {
	return f.table
}

func ExtractFirstTableNameFromStmt(stmt ast.StmtNode) string {
	visitor := &FirstTableNameVisitor{}
	stmt.Accept(visitor)
	return visitor.table
}

type AstVisitor struct {
	sqlFeature string
}

func ExtractAstVisit(stmt ast.StmtNode) (*AstVisitor, error) {
	visitor := &AstVisitor{}

	stmt.Accept(visitor)

	sb := strings.Builder{}
	if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return nil, err
	}
	visitor.sqlFeature = sb.String()

	return visitor, nil
}

func (f *AstVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch nn := n.(type) {
	case *ast.PatternInExpr:
		if len(nn.List) == 0 {
			return nn, false
		}
		if _, ok := nn.List[0].(*driver.ValueExpr); ok {
			nn.List = nn.List[:1]
		}
	case *driver.ValueExpr:
		nn.SetValue("?")
	}
	return n, false
}

func (f *AstVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

func (f *AstVisitor) SqlFeature() string {
	return f.sqlFeature
}

func UInt322Bytes(n uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, n)
	return b
}

func Bytes2Uint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}
