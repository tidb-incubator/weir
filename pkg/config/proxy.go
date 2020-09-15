package config

type Proxy struct {
	Version      string       `yaml:"version"`
	ProxyServer  ProxyServer  `yaml:"proxy_server"`
	AdminServer  AdminServer  `yaml:"admin_server"`
	Log          Log          `yaml:"log"`
	Registry     Registry     `yaml:"registry"`
	ConfigCenter ConfigCenter `yaml:"config_center"`
	Performance  Performance  `yaml:"performance"`
}

type ProxyServer struct {
	Addr           string `yaml:"addr"`
	MaxConnections uint32 `yaml:"max_connections"`
	TokenLimit     uint   `yaml:"token_limit"`
}

type AdminServer struct {
	Addr            string `yaml:"addr"`
	EnableBasicAuth bool   `yaml:"enable_basic_auth"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
}

type Log struct {
	Level   string  `yaml:"level"`
	Format  string  `yaml:"format"`
	LogFile LogFile `yaml:"log_file"`
}

type LogFile struct {
	Filename   string `yaml:"filename"`
	MaxSize    int    `yaml:"max_size"`
	MaxDays    int    `yaml:"max_days"`
	MaxBackups int    `yaml:"max_backups"`
}

type Registry struct {
	Enable bool     `yaml:"enable"`
	Type   string   `yaml:"type"`
	Addrs  []string `yaml:"addrs"`
}

type ConfigCenter struct {
	Type       string     `yaml:"type"`
	ConfigFile ConfigFile `yaml:"config_file"`
}

type ConfigFile struct {
	Path string `yaml:"path"`
}

type Performance struct {
	TCPKeepAlive bool `yaml:"tcp_keep_alive"`
}
