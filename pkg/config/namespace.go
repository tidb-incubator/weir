package config

type Namespace struct {
	Version   string            `yaml:"version"`
	Namespace string            `yaml:"namespace"`
	Frontend  FrontendNamespace `yaml:"frontend"`
	Backend   BackendNamespace  `yaml:"backend"`
	Breaker   BreakerInfo       `yaml:"breaker"`
}

type FrontendNamespace struct {
	AllowedDBs      []string           `yaml:"allowed_dbs"`
	SlowSQLTime     int                `yaml:"slow_sql_time"`
	DeniedIPs       []string           `yaml:"denied_ips"`
	IdleTimeout     int                `yaml:"idle_timeout"`
	IsGlobalBreaker bool               `yaml:"global_breaker_switch"`
	Users           []FrontendUserInfo `yaml:"users"`
	SQLBlackList    []SQLInfo          `yaml:"sql_blacklist"`
	SQLWhiteList    []SQLInfo          `yaml:"sql_whitelist"`
}

type FrontendUserInfo struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type SQLInfo struct {
	SQL string `yaml:"sql"`
}

type BackendNamespace struct {
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"`
	Instances    []string `yaml:"instances"`
	SelectorType string   `yaml:"selector_type"`
	PoolSize     int      `yaml:"pool_size"`
	IdleTimeout  int      `yaml:"idle_timeout"`
}

type StrategyInfo struct {
	MinQps               int64 `yaml:"min_qps"`
	SqlTimeoutMs         int64 `yaml:"sql_timeout_ms"`
	FailureRatethreshold int64 `yaml:"failure_ratethreshold"`
	FailureNum           int64 `yaml:"failure_num"`
	OpenStatusDurationMs int64 `yaml:"open_status_duratio_ms"`
	Size                 int64 `yaml:"size"`
	CellIntervalMs       int64 `yaml:"cellInterval_ms"`
}

type BreakerInfo struct {
	Scope      string         `yaml:"scope"`
	Strategies []StrategyInfo `yaml:"strategies"`
}
