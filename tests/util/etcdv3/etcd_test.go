package etcdv3

import (
	"context"
	. "github.com/pingcap-incubator/weir/pkg/util/etcdv3"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	testEtcdAddr = "127.0.0.1:2379"
)

func deleteTestData(cli *EtcdClient) {
	_ = cli.DeleteByPrefix(context.TODO(), "test")
}

func TestEtcdClient_Put(t *testing.T) {
	test := struct {
		key   string
		value string
	}{
		key:   "testPutKey",
		value: "testPutResult",
	}

	etcdCli, err := CreateEtcdClient([]string{testEtcdAddr})
	assert.NoError(t, err, "create etcd client error")

	err = etcdCli.Put(context.TODO(), test.key, test.value)
	assert.NoError(t, err, "Put data error")

	val, err := etcdCli.Get(context.TODO(), test.key)
	assert.NoError(t, err, "get data error")
	assert.Equal(t, test.value, val, "data error")

	deleteTestData(etcdCli)
}

func TestEtcdClient_PutTTL(t *testing.T) {
	test := struct {
		key   string
		value string
		ttl   time.Duration
	}{
		key:   "testPutTTLKey",
		value: "testPutTTLResult",
		ttl:   5 * time.Second,
	}

	etcdCli, err := CreateEtcdClient([]string{testEtcdAddr})
	assert.NoError(t, err, "CreateEtcdClient error")

	err = etcdCli.PutTTL(context.TODO(), test.key, test.value, test.ttl)
	assert.NoError(t, err, "PutTTL error")

	val, err := etcdCli.Get(context.TODO(), test.key)
	assert.NoError(t, err, "Get error error")
	assert.Equal(t, test.value, val)

	time.Sleep(test.ttl + time.Second)

	val, err = etcdCli.Get(context.TODO(), test.key)
	assert.Error(t, err, "ttl not ok")

	deleteTestData(etcdCli)
}

func TestEtcdClient_PutAndGetPrevValStr(t *testing.T) {
	test := struct {
		key    string
		oldVal string
		newVal string
	}{
		key:    "testPutAndGetPrevValStrKey",
		oldVal: "testPutAndGetPrevValStrOldResult",
		newVal: "testPutAndGetPrevValStrNewResult",
	}

	etcdCli, err := CreateEtcdClient([]string{testEtcdAddr})
	assert.NoError(t, err, "CreateEtcdClient error")

	err = etcdCli.Put(context.TODO(), test.key, test.oldVal)
	assert.NoError(t, err, "Put error")

	prev, err := etcdCli.PutAndGetPrevValStr(context.TODO(), test.key, test.newVal)
	assert.NoError(t, err, "PutAndGetPrevValStr error")
	assert.Equal(t, test.oldVal, prev)

	deleteTestData(etcdCli)
}

func TestEtcdClient_Get(t *testing.T) {
	test := struct {
		key   string
		value string
	}{
		key:   "testGetKey",
		value: "testGetResult",
	}

	etcdCli, err := CreateEtcdClient([]string{testEtcdAddr})
	assert.NoError(t, err, "CreateEtcdClient error")

	err = etcdCli.Put(context.TODO(), test.key, test.value)
	assert.NoError(t, err, "Put error")

	val, err := etcdCli.Get(context.TODO(), test.key)
	assert.NoError(t, err, "Get error")
	assert.Equal(t, test.value, val)

	deleteTestData(etcdCli)
}

func TestEtcdClient_GetNodesByPrefix(t *testing.T) {
	testPrefix := "testGetNodeByPrefixKey"
	testMap := map[string]string{
		"testGetNodeByPrefixKey_1": "testGetNodeByPrefixVal_1",
		"testGetNodeByPrefixKey_2": "testGetNodeByPrefixVal_2",
		"testGetNodeByPrefixKey_3": "testGetNodeByPrefixVal_3",
		"testGetNodeByPrefixKey_4": "testGetNodeByPrefixVal_4",
	}

	etcdCli, err := CreateEtcdClient([]string{testEtcdAddr})
	assert.NoError(t, err, "create etcd client error")

	for key, val := range testMap {
		err = etcdCli.Put(context.TODO(), key, val)
		assert.NoError(t, err, "Put error")
	}
	val, err := etcdCli.GetNodesByPrefix(context.TODO(), testPrefix)
	assert.NoError(t, err, "GetNodesByPrefix error")
	assert.Equal(t, len(val), len(testMap), "not find all item")
	for _, item := range val {
		assert.Equal(t, testMap[string(item.Key)], string(item.Value), "not find item")
	}

	deleteTestData(etcdCli)
}

func TestEtcdClient_Delete(t *testing.T) {
	test := struct {
		key   string
		value string
	}{
		key:   "testDeleteKey",
		value: "testDeleteResult",
	}
	etcdCli, err := CreateEtcdClient([]string{testEtcdAddr})
	assert.NoError(t, err, "CreateEtcdClient error")

	err = etcdCli.Put(context.TODO(), test.key, test.value)
	assert.NoError(t, err, "Put error")

	err = etcdCli.Delete(context.TODO(), test.key)
	assert.NoError(t, err, "Delete error")

	_, err = etcdCli.Get(context.TODO(), test.key)
	assert.Error(t, err, "not delete item")

	deleteTestData(etcdCli)
}

func TestEtcdClient_DeleteAndGetPrevValStr(t *testing.T) {
	test := struct {
		key   string
		value string
	}{
		key:   "testDeleteAndGetPrevValStrKey",
		value: "testDeleteAndGetPrevValStr",
	}
	etcdCli, err := CreateEtcdClient([]string{testEtcdAddr})
	assert.NoError(t, err, "CreateEtcdClient error")

	err = etcdCli.Put(context.TODO(), test.key, test.value)
	assert.NoError(t, err, "Put error")
	oldVal, err := etcdCli.DeleteAndGetPrevValStr(context.TODO(), test.key)
	assert.NoError(t, err, "DeleteAndGetPrevValStr error")
	assert.Equal(t, oldVal, test.value)
}

func TestEtcdClient_DeleteByPrefixAndGetPrevKV(t *testing.T) {

	testPrefix := "testDeleteByPrefixAndGetPrevKVKey"
	testMap := map[string]string{
		"testDeleteByPrefixAndGetPrevKVKey_1": "testDeleteByPrefixAndGetPrevKVVal_1",
		"testDeleteByPrefixAndGetPrevKVKey_2": "testDeleteByPrefixAndGetPrevKVVal_2",
		"testDeleteByPrefixAndGetPrevKVKey_3": "testDeleteByPrefixAndGetPrevKVVal_3",
		"testDeleteByPrefixAndGetPrevKVKey_4": "testDeleteByPrefixAndGetPrevKVVal_4",
	}

	etcdCli, err := CreateEtcdClient([]string{testEtcdAddr})
	assert.NoError(t, err, "CreateEtcdClient error")

	for key, val := range testMap {
		err = etcdCli.Put(context.TODO(), key, val)
		assert.NoError(t, err, "Put error")
	}

	prevVal, err := etcdCli.DeleteByPrefixAndGetPrevKV(context.TODO(), testPrefix)

	for _, item := range prevVal {
		assert.Equal(t, testMap[string(item.Key)], string(item.Value))
	}

	for key, _ := range testMap {
		_, err := etcdCli.Get(context.TODO(), key)
		assert.Error(t, err, "delete item error")
	}
}

func TestEtcdClient_DeleteByPrefix(t *testing.T) {
	testPrefix := "testDeleteByPrefixKey"
	testMap := map[string]string{
		"testDeleteByPrefixKey_1": "testDeleteByPrefixVal_1",
		"testDeleteByPrefixKey_2": "testDeleteByPrefixVal_2",
		"testDeleteByPrefixKey_3": "testDeleteByPrefixVal_3",
		"testDeleteByPrefixKey_4": "testDeleteByPrefixVal_4",
	}

	etcdCli, err := CreateEtcdClient([]string{testEtcdAddr})
	assert.NoError(t, err, "CreateEtcdClient error")

	for key, val := range testMap {
		err = etcdCli.Put(context.TODO(), key, val)
		assert.NoError(t, err, "Put error")
	}

	err = etcdCli.DeleteByPrefix(context.TODO(), testPrefix)
	assert.NoError(t, err, "DeleteByPrefix error")

	for key, _ := range testMap {
		_, err := etcdCli.Get(context.TODO(), key)
		assert.Error(t, err, "delete item error")
	}
}
