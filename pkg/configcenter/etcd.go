package configcenter

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/tidb-incubator/weir/pkg/config"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

const (
	DefaultEtcdDialTimeout = 3 * time.Second
)

type EtcdConfigCenter struct {
	etcdClient  *clientv3.Client
	kv          clientv3.KV
	basePath    string
	strictParse bool
}

func CreateEtcdConfigCenter(cfg config.ConfigEtcd) (*EtcdConfigCenter, error) {
	etcdConfig := clientv3.Config{
		Endpoints:   cfg.Addrs,
		Username:    cfg.Username,
		Password:    cfg.Password,
		DialTimeout: DefaultEtcdDialTimeout,
	}
	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, errors.WithMessage(err, "create etcd config center error")
	}

	center := NewEtcdConfigCenter(etcdClient, cfg.BasePath, cfg.StrictParse)
	return center, nil
}

func NewEtcdConfigCenter(etcdClient *clientv3.Client, basePath string, strictParse bool) *EtcdConfigCenter {
	return &EtcdConfigCenter{
		etcdClient:  etcdClient,
		kv:          clientv3.NewKV(etcdClient),
		basePath:    basePath,
		strictParse: strictParse,
	}
}

func (e *EtcdConfigCenter) get(ctx context.Context, key string) (*mvccpb.KeyValue, error) {
	resp, err := e.kv.Get(ctx, getNamespacePath(e.basePath, key))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("key not found")
	}
	return resp.Kvs[0], nil
}

func (e *EtcdConfigCenter) list(ctx context.Context) ([]*mvccpb.KeyValue, error) {
	baseDir := appendSlashToDirPath(e.basePath)
	resp, err := e.kv.Get(ctx, baseDir, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	return resp.Kvs, nil
}

func (e *EtcdConfigCenter) GetNamespace(ns string) (*config.Namespace, error) {
	ctx := context.Background()
	etcdKeyValue, err := e.get(ctx, ns)
	if err != nil {
		return nil, err
	}

	return config.UnmarshalNamespaceConfig(etcdKeyValue.Value)
}

func (e *EtcdConfigCenter) ListAllNamespace() ([]*config.Namespace, error) {
	ctx := context.Background()
	etcdKeyValues, err := e.list(ctx)
	if err != nil {
		return nil, err
	}

	var ret []*config.Namespace
	for _, kv := range etcdKeyValues {
		nsCfg, err := config.UnmarshalNamespaceConfig(kv.Value)
		if err != nil {
			if e.strictParse {
				return nil, err
			} else {
				logutil.BgLogger().Warn("parse namespace config error", zap.Error(err), zap.ByteString("namespace", kv.Key))
				continue
			}
		}
		ret = append(ret, nsCfg)
	}

	return ret, nil
}

func (e *EtcdConfigCenter) SetNamespace(ns string, value string) error {
	ctx := context.Background()
	_, err := e.kv.Put(ctx, ns, value)
	return err
}

func (e *EtcdConfigCenter) DelNamespace(ns string) error {
	ctx := context.Background()
	_, err := e.kv.Delete(ctx, ns)
	return err
}

func (e *EtcdConfigCenter) Close() {
	if err := e.etcdClient.Close(); err != nil {
		logutil.BgLogger().Error("close etcd client error", zap.Error(err))
	}
}

func getNamespacePath(basePath, ns string) string {
	return path.Join(basePath, ns)
}

// avoid base dir path prefix equal
func appendSlashToDirPath(dir string) string {
	if len(dir) == 0 {
		return ""
	}
	if dir[len(dir)-1] == '/' {
		return dir
	}
	return dir + "/"
}
