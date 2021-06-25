package driver

import (
	"context"

	"github.com/tidb-incubator/weir/pkg/proxy/metrics"
	wast "github.com/tidb-incubator/weir/pkg/util/ast"
	"github.com/pingcap/parser/ast"
)

func (q *QueryCtxImpl) recordQueryMetrics(ctx context.Context, stmt ast.StmtNode, err error, durationMilliSecond float64) {
	ns := q.ns.Name()
	db := q.currentDB
	firstTableName, _ := wast.GetAstTableNameFromCtx(ctx)
	stmtType := metrics.GetStmtTypeName(stmt)
	retLabel := metrics.RetLabel(err)

	metrics.QueryCtxQueryCounter.WithLabelValues(ns, db, firstTableName, stmtType, retLabel).Inc()
	metrics.QueryCtxQueryDurationHistogram.WithLabelValues(ns, db, firstTableName, stmtType).Observe(durationMilliSecond)
}

func (q *QueryCtxImpl) recordDeniedQueryMetrics(ctx context.Context, stmt ast.StmtNode) {
	ns := q.ns.Name()
	db := q.currentDB
	firstTableName, _ := wast.GetAstTableNameFromCtx(ctx)
	stmtType := metrics.GetStmtTypeName(stmt)

	metrics.QueryCtxQueryDeniedCounter.WithLabelValues(ns, db, firstTableName, stmtType).Inc()
}
