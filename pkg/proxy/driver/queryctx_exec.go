package driver

import (
	"context"
	"hash/crc32"
	"strings"

	"github.com/tidb-incubator/weir/pkg/proxy/constant"
	wast "github.com/tidb-incubator/weir/pkg/util/ast"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

func (q *QueryCtxImpl) isStmtDenied(ctx context.Context, sqlDigest uint32) bool {
	return q.ns.IsDeniedSQL(sqlDigest)
}

func (q *QueryCtxImpl) isStmtAllowed(ctx context.Context, sqlDigest uint32) bool {
	return q.ns.IsAllowedSQL(sqlDigest)
}

func (q *QueryCtxImpl) getBreakerName(ctx context.Context, sql string, breaker Breaker) (string, bool) {
	switch breaker.GetBreakerScope() {
	case "namesapce":
		return q.ns.Name(), true
	case "db":
		return q.currentDB, true
	case "table":
		firstTableName, _ := wast.GetAstTableNameFromCtx(ctx)
		return firstTableName, true
	case "sql":
		sqlParadigm, err := q.extractSqlParadigm(ctx, sql)
		if err != nil {
			logutil.BgLogger().Error("extract sql paradigm error", zap.String("namespace", q.ns.Name()), zap.String("database", q.currentDB), zap.Uint64("connId", q.connId), zap.String("sql", sql), zap.Error(err))
			return "", false
		}
		sqlDigest := crc32.ChecksumIEEE([]byte(sqlParadigm))
		return string(wast.UInt322Bytes(sqlDigest)), true
	default:
		return "", false
	}
}

func (q *QueryCtxImpl) getRateLimiterKey(ctx context.Context, rateLimiter RateLimiter) (string, bool) {
	switch rateLimiter.Scope() {
	case "namespace":
		return q.ns.Name(), true
	case "db":
		return q.currentDB, true
	case "table":
		firstTableName, _ := wast.GetAstTableNameFromCtx(ctx)
		return firstTableName, true
	default:
		return "", false
	}
}

func (q *QueryCtxImpl) extractSqlParadigm(ctx context.Context, sql string) (string, error) {
	charsetInfo, collation := q.sessionVars.GetCharsetInfo()
	featureStmt, err := q.parser.ParseOneStmt(sql, charsetInfo, collation)
	if err != nil {
		return "", err
	}
	visitor, err := wast.ExtractAstVisit(featureStmt)
	if err != nil {
		return "", err
	}
	return visitor.SqlFeature(), nil
}

func (q *QueryCtxImpl) executeStmt(ctx context.Context, sql string, stmtNode ast.StmtNode) (*gomysql.Result, error) {
	switch stmt := stmtNode.(type) {
	case *ast.SetStmt:
		return nil, q.setVariable(ctx, stmt)
	case *ast.UseStmt:
		return nil, q.useDB(ctx, stmt.DBName)
	case *ast.ShowStmt:
		return q.executeShowStmt(ctx, sql, stmt)
	case *ast.BeginStmt:
		return nil, q.begin(ctx)
	case *ast.CommitStmt:
		return nil, q.commitOrRollback(ctx, true)
	case *ast.RollbackStmt:
		return nil, q.commitOrRollback(ctx, false)
	default:
		return q.executeInBackend(ctx, sql, stmtNode)
	}
}

func (q *QueryCtxImpl) executeShowStmt(ctx context.Context, sql string, stmt *ast.ShowStmt) (*gomysql.Result, error) {
	switch stmt.Tp {
	case ast.ShowDatabases:
		databases := q.ns.ListDatabases()
		result, err := createShowDatabasesResult(databases)
		return result, err
	default:
		return q.executeInBackend(ctx, sql, stmt)
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

func (q *QueryCtxImpl) executeInBackend(ctx context.Context, sql string, stmtNode ast.StmtNode) (*gomysql.Result, error) {
	ctx = context.WithValue(ctx, constant.ContextKeySessionVariable, q.sessionVars.GetAllSystemVars())

	result, err := q.connMgr.Query(ctx, q.currentDB, sql)
	if err != nil {
		return nil, err
	}

	q.sessionVars.SetAffectRows(result.AffectedRows)
	q.sessionVars.SetLastInsertID(result.InsertId)

	if result.Resultset == nil {
		return nil, nil
	}

	return result, nil
}

func (q *QueryCtxImpl) useDB(ctx context.Context, db string) error {
	if !q.ns.IsDatabaseAllowed(db) {
		return mysql.NewErrf(mysql.ErrDBaccessDenied, "db %s access denied", db)
	}
	q.currentDB = db
	return nil
}

func (q *QueryCtxImpl) setVariable(ctx context.Context, stmt *ast.SetStmt) error {
	var autoCommitVar *ast.VariableAssignment
	var sysVars []*ast.VariableAssignment

	for _, v := range stmt.Variables {
		switch strings.ToLower(v.Name) {
		case variable.AutoCommit:
			autoCommitVar = v
			//default:
			//	if v.IsGlobal {
			//		return errors.Errorf("cannot set variable in global scope")
			//	}
			//	sysVars = append(sysVars, v)
		}
	}

	if len(sysVars) != 0 {
		if err := q.setSysVars(ctx, sysVars); err != nil {
			return err
		}
	}

	if autoCommitVar != nil {
		if err := q.setAutoCommit(ctx, autoCommitVar); err != nil {
			return err
		}
	}

	return nil
}

// set other system variables except autocommit
func (q *QueryCtxImpl) setSysVars(ctx context.Context, vars []*ast.VariableAssignment) error {
	for _, v := range vars {
		if err := q.sessionVars.CheckSessionSysVarValid(v.Name); err != nil {
			return errors.WithMessage(err, "check sysvar error")
		}
	}

	for _, v := range vars {
		if _, ok := v.Value.(*ast.DefaultExpr); ok {
			q.sessionVars.SetSystemVarDefault(v.Name)
		} else {
			q.sessionVars.SetSystemVarAST(v.Name, v)
		}
	}

	return nil
}

func (q *QueryCtxImpl) setAutoCommit(ctx context.Context, v *ast.VariableAssignment) error {
	var err error
	autocommit, err := getAutoCommitValue(v.Value)
	if err != nil {
		return err
	}

	err = q.connMgr.SetAutoCommit(ctx, autocommit)
	q.connMgr.MergeStatus(q.sessionVars)
	return err
}

func getAutoCommitValue(v ast.ExprNode) (bool, error) {
	if _, ok := v.(*ast.DefaultExpr); ok {
		return true, nil
	}
	value, ok := v.(ast.ValueExpr)
	if !ok {
		return false, errors.Errorf("invalid autocommit value type %T", v)
	}
	autocommitInt64, ok := value.GetValue().(int64)
	if !ok {
		return false, errors.Errorf("autocommit value is not int64, type %T", value.GetValue())
	}
	return autocommitInt64 == 1, nil
}

func (q *QueryCtxImpl) begin(ctx context.Context) error {
	err := q.connMgr.Begin(ctx)
	q.connMgr.MergeStatus(q.sessionVars)
	return err
}

func (q *QueryCtxImpl) commitOrRollback(ctx context.Context, commit bool) error {
	err := q.connMgr.CommitOrRollback(ctx, commit)
	q.connMgr.MergeStatus(q.sessionVars)
	return err
}
