package driver

import (
	"github.com/pingcap-incubator/weir/pkg/proxy/metrics"
	"github.com/pingcap/parser/ast"
)

func (q *QueryCtxImpl) recordQueryMetrics(stmt ast.StmtNode, err error, durationMilliSecond float64) {
	ns := q.ns.Name()
	db := q.currentDB
	stmtType := metrics.GetStmtTypeName(stmt)
	retLabel := metrics.RetLabel(err)

	metrics.QueryCtxQueryCounter.WithLabelValues(ns, db, stmtType, retLabel).Inc()
	metrics.QueryCtxQueryDurationHistogram.WithLabelValues(ns, db, stmtType).Observe(durationMilliSecond)
}

func (q *QueryCtxImpl) recordDeniedQueryMetrics(stmt ast.StmtNode) {
	ns := q.ns.Name()
	db := q.currentDB
	stmtType := metrics.GetStmtTypeName(stmt)

	metrics.QueryCtxQueryDeniedCounter.WithLabelValues(ns, db, stmtType).Inc()
}
