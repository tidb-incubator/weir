package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	ModuleWeirProxy = "weirproxy"
)

// metrics labels.
const (
	LabelServer    = "server"
	LabelQueryCtx  = "queryctx"
	LabelBackend   = "backend"
	LabelSession   = "session"
	LabelDomain    = "domain"
	LabelDDLOwner  = "ddl-owner"
	LabelDDL       = "ddl"
	LabelDDLWorker = "ddl-worker"
	LabelDDLSyncer = "ddl-syncer"
	LabelGCWorker  = "gcworker"
	LabelAnalyze   = "analyze"

	LabelBatchRecvLoop = "batch-recv-loop"
	LabelBatchSendLoop = "batch-send-loop"

	opSucc   = "ok"
	opFailed = "err"

	LableScope   = "scope"
	ScopeGlobal  = "global"
	ScopeSession = "session"
)

// RetLabel returns "ok" when err == nil and "err" when err != nil.
// This could be useful when you need to observe the operation result.
func RetLabel(err error) string {
	if err == nil {
		return opSucc
	}
	return opFailed
}

func RegisterProxyMetrics(cluster string) {
	curryingLabelsWithLblCluster := map[string]string{LblCluster: cluster}

	PanicCounter = PanicCounter.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(PanicCounter)
	QueryTotalCounter = QueryTotalCounter.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(QueryTotalCounter)
	ExecuteErrorCounter = ExecuteErrorCounter.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(ExecuteErrorCounter)
	ConnGauge = ConnGauge.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(ConnGauge)

	// query ctx metrics
	QueryCtxQueryCounter = QueryCtxQueryCounter.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(QueryCtxQueryCounter)
	QueryCtxQueryDeniedCounter = QueryCtxQueryDeniedCounter.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(QueryCtxQueryDeniedCounter)
	QueryCtxQueryDurationHistogram = QueryCtxQueryDurationHistogram.MustCurryWith(curryingLabelsWithLblCluster).(*prometheus.HistogramVec)
	prometheus.MustRegister(QueryCtxQueryDurationHistogram)
	QueryCtxGauge = QueryCtxGauge.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(QueryCtxGauge)
	QueryCtxAttachedConnGauge = QueryCtxAttachedConnGauge.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(QueryCtxAttachedConnGauge)
	QueryCtxTransactionDuration = QueryCtxTransactionDuration.MustCurryWith(curryingLabelsWithLblCluster).(*prometheus.HistogramVec)
	prometheus.MustRegister(QueryCtxTransactionDuration)

	// backend metrics
	BackendEventCounter = BackendEventCounter.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(BackendEventCounter)
	BackendQueryCounter = BackendQueryCounter.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(BackendQueryCounter)
	BackendConnInUseGauge = BackendConnInUseGauge.MustCurryWith(curryingLabelsWithLblCluster)
	prometheus.MustRegister(BackendConnInUseGauge)
}
