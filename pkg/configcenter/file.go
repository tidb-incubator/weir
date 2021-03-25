package configcenter

import (
	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap/errors"
	"io/ioutil"
	"path"
	"path/filepath"
)

var (
	ErrNamespaceNotFound = errors.New("namespace not found")
)

// FileConfigCenter is only for test use,
// please do not use it in production environment.
type FileConfigCenter struct {
	dir    string
	cfgs   map[string]*config.Namespace // key: namespace
	nspath map[string]string            // key: namespace, value: config file path
}

func CreateFileConfigCenter(nsdir string) (*FileConfigCenter, error) {
	yamlFiles, err := listAllYamlFiles(nsdir)
	if err != nil {
		return nil, err
	}

	c := newFileConfigCenter(nsdir)

	for _, yamlFile := range yamlFiles {
		fileData, err := ioutil.ReadFile(yamlFile)
		if err != nil {
			return nil, err
		}
		cfg, err := config.UnmarshalNamespaceConfig(fileData)
		if err != nil {
			return nil, err
		}
		c.cfgs[cfg.Namespace] = cfg
		c.nspath[cfg.Namespace] = yamlFile
	}
	return c, nil
}

func newFileConfigCenter(dir string) *FileConfigCenter {
	return &FileConfigCenter{
		dir:    dir,
		cfgs:   make(map[string]*config.Namespace),
		nspath: make(map[string]string),
	}
}

func listAllYamlFiles(dir string) ([]string, error) {
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var ret []string
	for _, info := range infos {
		fileName := info.Name()
		if path.Ext(fileName) == ".yaml" {
			ret = append(ret, filepath.Join(dir, fileName))
		}
	}

	return ret, nil
}

func (f *FileConfigCenter) GetNamespace(ns string) (*config.Namespace, error) {
	cfg, ok := f.cfgs[ns]
	if !ok {
		return nil, ErrNamespaceNotFound
	}
	return cfg, nil
}

func (f *FileConfigCenter) ListAllNamespace() ([]*config.Namespace, error) {
	var ret []*config.Namespace
	for _, cfg := range f.cfgs {
		ret = append(ret, cfg)
	}
	return ret, nil
}
