package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testNamespaceConfig = Namespace{
	Version:   "v1",
	Namespace: "test_ns",
	Frontend: FrontendNamespace{
		AllowedDBs:  []string{"db0", "db1"},
		SlowSQLTime: 10,
		DeniedIPs:   []string{"127.0.0.0", "128.0.0.0"},
		IdleTimeout: 10,
		Users: []FrontendUserInfo{
			{Username: "user0", Password: "pwd0"},
			{Username: "user1", Password: "pwd1"},
		},
	},
	Backend: BackendNamespace{
		Username:     "user0",
		Password:     "pwd0",
		Instances:    []string{"127.0.0.1:4000", "127.0.0.1:4001"},
		SelectorType: "random",
		PoolSize:     1,
		IdleTimeout:  20,
	},
}

var testProxyConfig = Proxy{
	Version: "v1",
	ProxyServer: ProxyServer{
		Addr:           "0.0.0.0:4000",
		MaxConnections: 1,
	},
	AdminServer: AdminServer{
		Addr:            "0.0.0.0:4001",
		EnableBasicAuth: false,
		User:            "user",
		Password:        "pwd",
	},
	Log: Log{
		Level:  "info",
		Format: "console",
		LogFile: LogFile{
			Filename:   ".",
			MaxSize:    10,
			MaxDays:    1,
			MaxBackups: 1,
		},
	},
	Registry: Registry{
		Enable: false,
		Type:   "etcd",
		Addrs:  []string{"127.0.0.1:4000", "127.0.0.1:4001"},
	},
	ConfigCenter: ConfigCenter{
		Type: "file",
		ConfigFile: ConfigFile{
			Path: ".",
		},
	},
	Performance: Performance{
		TCPKeepAlive: true,
	},
}

func TestNamespaceConfigEncodeAndDecode(t *testing.T) {
	data, err := MarshalNamespaceConfig(&testNamespaceConfig)
	assert.NoError(t, err)
	cfg, err := UnmarshalNamespaceConfig(data)
	assert.NoError(t, err)
	assert.Equal(t, testNamespaceConfig, *cfg)
}

func TestProxyConfigEncodeAndDecode(t *testing.T) {
	data, err := MarshalProxyConfig(&testProxyConfig)
	assert.NoError(t, err)
	cfg, err := UnmarshalProxyConfig(data)
	assert.NoError(t, err)
	assert.Equal(t, testProxyConfig, *cfg)
}
