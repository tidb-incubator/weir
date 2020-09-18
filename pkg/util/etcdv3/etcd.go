package etcdv3

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pingcap/errors"
)

const (
	DefaultDialTimeOut = 5
)

type EtcdClient struct {
	KV    clientv3.KV
	Lease clientv3.Lease
	Watch clientv3.Watcher
}

func NewEtcdClientWithClient(kv clientv3.KV, lease clientv3.Lease, watch clientv3.Watcher) *EtcdClient {
	return &EtcdClient{
		KV:    kv,
		Lease: lease,
		Watch: watch,
	}
}

func CreateEtcdClient(clusterAddr []string) (*EtcdClient, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   clusterAddr,
		DialTimeout: DefaultDialTimeOut * time.Second,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create new etcd client error")
	}
	kv := clientv3.NewKV(cli)
	lease := clientv3.NewLease(cli)
	watch := clientv3.NewWatcher(cli)
	return NewEtcdClientWithClient(kv, lease, watch), nil
}

func (m *EtcdClient) Get(ctx context.Context, key string) (string, error) {
	r, err := m.GetNode(ctx, key)
	if err != nil {
		return "", err
	}
	return string(r.Value), nil
}

func (m *EtcdClient) GetNode(ctx context.Context, key string) (*mvccpb.KeyValue, error) {
	r, err := m.KV.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if r.Count == 0 {
		return nil, fmt.Errorf("not find value, key: %s", key)
	}
	return r.Kvs[0], nil
}

func (m *EtcdClient) GetNodesByPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	r, err := m.KV.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	if r.Count == 0 {
		return nil, fmt.Errorf("not find value, prefix: %s", prefix)
	}
	return r.Kvs, nil
}

func (m *EtcdClient) Put(ctx context.Context, key, value string) error {
	_, err := m.KV.Put(ctx, key, value)
	return err
}

func (m *EtcdClient) PutAndGetPrevValStr(ctx context.Context, key, value string) (string, error) {
	r, err := m.KV.Put(ctx, key, value, clientv3.WithPrevKV())
	if err != nil {
		return "", errors.Wrapf(err, "put and get previous value error, key: %s, val:  %s", key, value)
	}
	if r.PrevKv == nil {
		return "", nil
	}
	return string(r.PrevKv.Value), nil
}

func (m *EtcdClient) PutTTL(ctx context.Context, key, val string, ttl time.Duration) error {
	lease, err := m.Lease.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return errors.Wrap(err, "create new lease error")
	}
	_, err = m.KV.Put(ctx, key, val, clientv3.WithLease(lease.ID))
	if err != nil {
		return errors.Wrapf(err, "put data with lease error key: %s, val: %s, lease.ID: %v", key, val, lease.ID)
	}
	return nil
}

func (m *EtcdClient) Delete(ctx context.Context, key string) error {
	_, err := m.KV.Delete(ctx, key)
	if err != nil {
		return errors.Wrapf(err, "delete key error, key: %s", key)
	}
	return nil
}

func (m *EtcdClient) DeleteByPrefix(ctx context.Context, prefix string) error {
	_, err := m.KV.Delete(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrapf(err, "delete by prefix error, perfix: %s", prefix)
	}
	return nil
}

func (m *EtcdClient) DeleteAndGetPrevValStr(ctx context.Context, key string) (string, error) {
	r, err := m.KV.Delete(ctx, key, clientv3.WithPrevKV())
	if err != nil {
		return "", errors.Wrapf(err, "delete and get previous value error, key: %s", key)
	}
	if len(r.PrevKvs) < 1 {
		return "", nil
	}
	return string(r.PrevKvs[0].Value), nil
}

//maybe want use keys
func (m *EtcdClient) DeleteByPrefixAndGetPrevKV(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	r, err := m.KV.Delete(ctx, prefix, clientv3.WithPrevKV(), clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrapf(err, "delete by prefix and get prev kv error, key: %s", prefix)
	}
	return r.PrevKvs, nil
}
