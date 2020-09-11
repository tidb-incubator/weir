package configcenter

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pingcap-incubator/weir/pkg/configcenter"
	"github.com/stretchr/testify/assert"
)

func Test_CreateFileConfigCenter(t *testing.T) {
	nsName := "test_namespace"
	_, localFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(localFile)
	c, err := configcenter.CreateFileConfigCenter(dir)
	assert.NoError(t, err)

	rets, err := c.ListAllNamespace()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rets))
	assert.Equal(t, nsName, rets[0].Namespace)

	ret, err := c.GetNamespace(nsName)
	assert.NoError(t, err)
	assert.Equal(t, nsName, ret.Namespace)

	_, err = c.GetNamespace("unknown")
	assert.EqualError(t, err, configcenter.ErrNamespaceNotFound.Error())
}
