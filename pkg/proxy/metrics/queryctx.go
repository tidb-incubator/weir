package metrics

import (
	"github.com/pingcap/parser/ast"
	"github.com/prometheus/client_golang/prometheus"
)

type AstStmtType int

const (
	StmtTypeUnknown AstStmtType = iota
	StmtTypeSelect
	StmtTypeInsert
	StmtTypeUpdate
	StmtTypeDelete
	StmtTypeDDL
	StmtTypeBegin
	StmtTypeCommit
	StmtTypeRollback
	StmtTypeSet
	StmtTypeShow
	StmtTypeUse
	StmtTypeComment
)

const (
	StmtNameUnknown  = "unknown"
	StmtNameSelect   = "select"
	StmtNameInsert   = "insert"
	StmtNameUpdate   = "update"
	StmtNameDelete   = "delete"
	StmtNameDDL      = "ddl"
	StmtNameBegin    = "begin"
	StmtNameCommit   = "commit"
	StmtNameRollback = "rollback"
	StmtNameSet      = "set"
	StmtNameShow     = "show"
	StmtNameUse      = "use"
	StmtNameComment  = "comment"
)

var (
	QueryCtxQueryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelQueryCtx,
			Name:      "query_total",
			Help:      "Counter of queries.",
		}, []string{LblNamespace, LblDb, LblSQLType, LblResult})

	QueryCtxQueryDeniedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelQueryCtx,
			Name:      "query_denied",
			Help:      "Counter of denied queries.",
		}, []string{LblNamespace, LblDb, LblSQLType})


	QueryCtxQueryDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelQueryCtx,
			Name:      "handle_query_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblNamespace, LblDb, LblSQLType})

	QueryCtxGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelQueryCtx,
			Name:      "queryctx",
			Help:      "Number of queryctx (equals to client connection).",
		}, []string{LblNamespace})

	QueryCtxAttachedConnGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelQueryCtx,
			Name:      "attached_connections",
			Help:      "Number of attached backend connections.",
		}, []string{LblNamespace})

	QueryCtxTransactionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "transaction_duration_seconds",
			Help:      "Bucketed histogram of a transaction execution duration, including retry.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblNamespace, LblDb, LblSQLType})
)

func GetStmtType(stmt ast.StmtNode) AstStmtType {
	switch stmt.(type) {
	case *ast.SelectStmt:
		return StmtTypeSelect
	case *ast.InsertStmt:
		return StmtTypeInsert
	case *ast.UpdateStmt:
		return StmtTypeUpdate
	case *ast.DeleteStmt:
		return StmtTypeDelete
	case *ast.BeginStmt:
		return StmtTypeBegin
	case *ast.CommitStmt:
		return StmtTypeCommit
	case *ast.RollbackStmt:
		return StmtTypeRollback
	case *ast.SetStmt:
		return StmtTypeSet
	case *ast.ShowStmt:
		return StmtTypeShow
	case *ast.UseStmt:
		return StmtTypeUse
	default:
		return StmtTypeUnknown
	}
}

func GetStmtTypeName(stmt ast.StmtNode) string {
	switch stmt.(type) {
	case *ast.SelectStmt:
		return StmtNameSelect
	case *ast.InsertStmt:
		return StmtNameInsert
	case *ast.UpdateStmt:
		return StmtNameUpdate
	case *ast.DeleteStmt:
		return StmtNameDelete
	case *ast.BeginStmt:
		return StmtNameBegin
	case *ast.CommitStmt:
		return StmtNameCommit
	case *ast.RollbackStmt:
		return StmtNameRollback
	case *ast.SetStmt:
		return StmtNameSet
	case *ast.ShowStmt:
		return StmtNameShow
	case *ast.UseStmt:
		return StmtNameUse
	default:
		return StmtNameUnknown
	}
}
