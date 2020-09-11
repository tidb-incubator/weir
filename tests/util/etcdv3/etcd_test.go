package etcdv3

import (
	"context"
	"fmt"
	. "github.com/pingcap-incubator/weir/pkg/util/etcdv3"
	"testing"
	"time"
)

func TestEtcdClient_Put(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	err = etcdCli.Put(context.TODO(), "test22", "asdf1")
	fmt.Println(err)
	err = etcdCli.Put(context.TODO(), "test23", "asdf2")
	fmt.Println(err)
	err = etcdCli.Put(context.TODO(), "test24", "asdf3")
	fmt.Println(err)
}

func TestEtcdClient_PutTTL(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	err = etcdCli.PutTTL(context.TODO(), "test21", "asdf0", time.Second*5)
	fmt.Println(err)
	val, err := etcdCli.Get(context.TODO(), "test21")
	fmt.Println(val, err)
	time.Sleep(time.Second * 5)
	val, err = etcdCli.Get(context.TODO(), "test21")
	fmt.Println(val, err)
}

func TestEtcdClient_PutAndGetPrevValStr(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	prev, err := etcdCli.PutAndGetPrevValStr(context.TODO(), "test22", "asd")
	fmt.Println(prev, err)
}

func TestEtcdClient_Get(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	val, err := etcdCli.Get(context.TODO(), "test22")
	fmt.Println(val, err)
}

func TestEtcdClient_GetNode(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	val, err := etcdCli.GetNode(context.TODO(), "test22")
	fmt.Println(val, err)
}

func TestEtcdClient_GetNodeByPrefix(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	val, err := etcdCli.GetNodesByPrefix(context.TODO(), "test")
	fmt.Println(val, err)
}

func TestEtcdClient_Delete(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	err = etcdCli.Delete(context.TODO(), "test22")
	fmt.Println(err)
}

func TestEtcdClient_DeleteAndGetPrevValStr(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	prevVal, err := etcdCli.DeleteAndGetPrevValStr(context.TODO(), "test23")
	fmt.Println(prevVal)
}

func TestEtcdClient_DeleteByPrefixAndGetPrevKV(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	prevVal, err := etcdCli.DeleteByPrefixAndGetPrevKV(context.TODO(), "test")
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, item := range prevVal {
		fmt.Println(item)
	}
}

func TestEtcdClient_DeleteByPrefix(t *testing.T) {
	etcdCli, err := CreateEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
	}
	err = etcdCli.DeleteByPrefix(context.TODO(), "test")
	fmt.Println(err)
}
