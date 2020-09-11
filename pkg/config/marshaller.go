package config

import "github.com/goccy/go-yaml"

func UnmarshalNamespaceConfig(data []byte) (*Namespace, error) {
	var cfg Namespace
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func MarshalNamespaceConfig(cfg *Namespace) ([]byte, error) {
	return yaml.Marshal(cfg)
}

func UnmarshalProxyConfig(data []byte) (*Proxy, error) {
	var cfg Proxy
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func MarshalProxyConfig(cfg *Proxy) ([]byte, error) {
	return yaml.Marshal(cfg)
}
