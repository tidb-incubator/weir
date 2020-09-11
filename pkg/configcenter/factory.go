package configcenter

import (
	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap/errors"
)

const (
	ConfigCenterTypeFile = "file"
)

type ConfigCenter interface {
	GetNamespace(ns string) (*config.Namespace, error)
	ListAllNamespace() ([]*config.Namespace, error)
}

func CreateConfigCenter(cfg config.ConfigCenter) (ConfigCenter, error) {
	switch cfg.Type {
	case ConfigCenterTypeFile:
		return CreateFileConfigCenter(cfg.ConfigFile.Path)
	default:
		return nil, errors.New("invalid config center type")
	}
}
