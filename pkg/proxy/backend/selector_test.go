package backend

import (
	"strconv"
	"testing"

	"github.com/pingcap-incubator/weir/pkg/util/rand2"
	"github.com/stretchr/testify/assert"
)

type testSource struct {
	val int64
}

func (t *testSource) Int63() int64 {
	return t.val
}

func (*testSource) Seed(seed int64) {
}

func TestRandomSelector_Select_Success(t *testing.T) {
	source := &testSource{}
	rd := rand2.New(source)
	selector := NewRandomSelector(rd)

	host := "127.0.0.1"
	ports := []int{4000, 4001, 4002}
	instances := prepareInstances(host, ports)

	for i := 0; i < len(ports); i++ {
		source.val = int64(i)
		instance, err := selector.Select(instances)
		assert.NoError(t, err)
		assert.Equal(t, getAddr(host, ports[i]), instance.addr)
	}
}

func TestRandomSelector_Select_ErrNoInstanceToSelect(t *testing.T) {
	source := &testSource{}
	rd := rand2.New(source)
	selector := NewRandomSelector(rd)

	host := "127.0.0.1"
	var ports []int
	instances := prepareInstances(host, ports)

	instance, err := selector.Select(instances)
	assert.Nil(t, instance)
	assert.EqualError(t, err, ErrNoInstanceToSelect.Error())
}

func prepareInstances(host string, ports []int) []*Instance {
	var instances []*Instance
	for _, p := range ports {
		instance := &Instance{
			addr: getAddr(host, p),
		}
		instances = append(instances, instance)
	}
	return instances
}

func getAddr(host string, port int) string {
	return host + ":" + strconv.Itoa(port)
}
