package config

type Namespace struct {
	Version   string            `yaml:"version"`
	Namespace string            `yaml:"namespace"`
	Frontend  FrontendNamespace `yaml:"frontend"`
	Backend   BackendNamespace  `yaml:"backend"`
}

type FrontendNamespace struct {
	AllowedDBs  []string           `yaml:"allowed_dbs"`
	SlowSQLTime int                `yaml:"slow_sql_time"`
	DeniedSQLs  []string           `yaml:"denied_sqls"`
	DeniedIPs   []string           `yaml:"denied_ips"`
	IdleTimeout int                `yaml:"idle_timeout"`
	Users       []FrontendUserInfo `yaml:"users"`
}

type FrontendUserInfo struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type BackendNamespace struct {
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"`
	Instances    []string `yaml:"instances"`
	SelectorType string   `yaml:"selector_type"`
	PoolSize     int      `yaml:"pool_size"`
	IdleTimeout  int      `yaml:"idle_timeout"`
}
