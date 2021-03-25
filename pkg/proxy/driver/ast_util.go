package driver

import (
	"context"

	"github.com/pingcap/parser/ast"
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

func extractFirstTableNameFromStmt(stmt ast.StmtNode) string {
	visitor := &FirstTableNameVisitor{}
	stmt.Accept(visitor)
	return visitor.table
}
