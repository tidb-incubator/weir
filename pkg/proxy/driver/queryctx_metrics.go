package driver

import (
	"context"

	"github.com/pingcap-incubator/weir/pkg/proxy/metrics"
	"github.com/pingcap/parser/ast"
)

func (q *QueryCtxImpl) recordQueryMetrics(ctx context.Context, stmt ast.StmtNode, err error, durationMilliSecond float64) {
	ns := q.ns.Name()
	db := q.currentDB
	firstTableName, _ := GetAstTableNameFromCtx(ctx)
	stmtType := metrics.GetStmtTypeName(stmt)
	retLabel := metrics.RetLabel(err)

	metrics.QueryCtxQueryCounter.WithLabelValues(ns, db, firstTableName, stmtType, retLabel).Inc()
	metrics.QueryCtxQueryDurationHistogram.WithLabelValues(ns, db, firstTableName, stmtType).Observe(durationMilliSecond)
}

func (q *QueryCtxImpl) recordDeniedQueryMetrics(ctx context.Context, stmt ast.StmtNode) {
	ns := q.ns.Name()
	db := q.currentDB
	firstTableName, _ := GetAstTableNameFromCtx(ctx)
	stmtType := metrics.GetStmtTypeName(stmt)

	metrics.QueryCtxQueryDeniedCounter.WithLabelValues(ns, db, firstTableName, stmtType).Inc()
}
