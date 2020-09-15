package driver

import (
	"context"

	"github.com/pingcap-incubator/weir/pkg/proxy/server"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

func (q *QueryCtxImpl) execute(ctx context.Context, sql string) ([]server.ResultSet, error) {
	charsetInfo, collation := q.sessionVars.GetCharsetInfo()
	stmt, err := q.parser.ParseOneStmt(sql, charsetInfo, collation)
	if err != nil {
		return nil, err
	}

	return q.executeStmt(ctx, sql, stmt)
}

// TODO: implement this function
func (q *QueryCtxImpl) executeStmt(ctx context.Context, sql string, stmtNode ast.StmtNode) ([]server.ResultSet, error) {
	switch stmt := stmtNode.(type) {
	case *ast.UseStmt:
		err := q.useDB(ctx, stmt.DBName)
		return nil, err
	case *ast.ShowStmt:
		return q.executeShowStmt(ctx, sql, stmt)
	case *ast.SelectStmt:
		return q.executeInBackend(ctx, sql, stmtNode)
	default:
		return nil, mysql.NewErrf(mysql.ErrUnknown, "stmt %T not supported now", stmtNode)
	}
}

// TODO: implement this function
func (q *QueryCtxImpl) executeShowStmt(ctx context.Context, sql string, stmt *ast.ShowStmt) ([]server.ResultSet, error) {
	switch stmt.Tp {
	case ast.ShowDatabases:
		databases := q.ns.Frontend().ListDatabases()
		result, err := createShowDatabasesResult(databases)
		if err != nil {
			return nil, err
		}
		return []server.ResultSet{wrapMySQLResult(result)}, nil
	case ast.ShowTables:
		return q.executeInBackend(ctx, sql, stmt)
	default:
		return nil, mysql.NewErrf(mysql.ErrUnknown, "show type %v not supported now", stmt.Tp)
	}
}

func createShowDatabasesResult(dbNames []string) (*gomysql.Result, error) {
	var values [][]interface{}
	for _, db := range dbNames {
		values = append(values, []interface{}{db})
	}

	rs, err := gomysql.BuildSimpleTextResultset([]string{"Database"}, values)
	if err != nil {
		return nil, err
	}

	result := &gomysql.Result{
		Status:    0,
		Resultset: rs,
	}

	// copied from go-mysql client/conn.readResultRows()
	// since convertFieldsToColumnInfos() only read result.Value,
	// so we have to write row data back to value
	// FIXME(eastfisher): remove this when weir client is finished
	if cap(result.Values) < len(result.RowDatas) {
		result.Values = make([][]gomysql.FieldValue, len(result.RowDatas))
	} else {
		result.Values = result.Values[:len(result.RowDatas)]
	}

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, false, result.Values[i])

		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return result, nil
}

func (q *QueryCtxImpl) executeInBackend(ctx context.Context, sql string, stmtNode ast.StmtNode) ([]server.ResultSet, error) {
	conn, err := q.ns.Backend().GetPooledConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.PutBack()

	if err := conn.UseDB(q.currentDB); err != nil {
		return nil, err
	}

	result, err := conn.Execute(sql)
	if err != nil {
		return nil, err
	}

	resultSet := wrapMySQLResult(result)
	return []server.ResultSet{resultSet}, nil
}

func (q *QueryCtxImpl) useDB(ctx context.Context, db string) error {
	if !q.ns.Frontend().IsDatabaseAllowed(db) {
		return mysql.NewErrf(mysql.ErrDBaccessDenied, "db %s access denied", db)
	}
	q.currentDB = db
	return nil
}
