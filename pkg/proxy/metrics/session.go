package metrics

// Label constants.
const (
	LblUnretryable = "unretryable"
	LblReachMax    = "reach_max"
	LblOK          = "ok"
	LblError       = "error"
	LblCommit      = "commit"
	LblAbort       = "abort"
	LblRollback    = "rollback"
	LblType        = "type"
	LblDb          = "db"
	LblResult      = "result"
	LblSQLType     = "sql_type"
	LblGeneral     = "general"
	LblInternal    = "internal"
	LbTxnMode      = "txn_mode"
	LblPessimistic = "pessimistic"
	LblOptimistic  = "optimistic"
	LblStore       = "store"
	LblAddress     = "address"
	LblBatchGet    = "batch_get"
	LblGet         = "get"
)
