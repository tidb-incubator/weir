package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	ModuleWeirProxy = "weirproxy"
)

// metrics labels.
const (
	LabelServer    = "server"
	LabelQueryCtx  = "queryctx"
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

func RegisterProxyMetrics() {
	prometheus.MustRegister(PanicCounter)
	prometheus.MustRegister(QueryTotalCounter)
	prometheus.MustRegister(ExecuteErrorCounter)
	prometheus.MustRegister(CriticalErrorCounter)
	prometheus.MustRegister(ConnGauge)
	prometheus.MustRegister(ServerEventCounter)
	prometheus.MustRegister(GetTokenDurationHistogram)
	prometheus.MustRegister(HandShakeErrorCounter)

	// query ctx metrics
	prometheus.MustRegister(QueryCtxQueryCounter)
	prometheus.MustRegister(QueryCtxQueryDeniedCounter)
	prometheus.MustRegister(QueryCtxQueryDurationHistogram)
	prometheus.MustRegister(QueryCtxGauge)
	prometheus.MustRegister(QueryCtxAttachedConnGauge)
	prometheus.MustRegister(QueryCtxTransactionDuration)
}
