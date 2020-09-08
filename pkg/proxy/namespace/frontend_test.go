package namespace

import (
	"fmt"
	"testing"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
)

const (
	duplicateUsername = "ns_user"
	duplicatePassword = "ns_passwd"
	testAllowedDB     = "db0"
	testNamespace = "ns0"
)

func Test_CreateUserNamespaceMapper_Success(t *testing.T) {
	nss := prepareNamespaces([]string{"ns0", "ns1"}, false)
	ret, err := CreateUserNamespaceMapper(nss)
	assert.NotNil(t, ret)
	assert.Nil(t, err)
}

func Test_CreateUserNamespaceMapper_Error_DuplicatedUser(t *testing.T) {
	nss := prepareNamespaces([]string{"ns0", "ns1"}, true)
	ret, err := CreateUserNamespaceMapper(nss)
	assert.Nil(t, ret)
	assert.EqualError(t, errors.Cause(err), ErrDuplicateUser.Error())
}

func TestUserNamespaceMapper_GetUserNamespace_Success_Found(t *testing.T) {
	nss := prepareNamespaces([]string{"ns0"}, false)
	ret, err := CreateUserNamespaceMapper(nss)
	assert.Nil(t, err)

	ns, ok := ret.GetUserNamespace("ns0_user_0", "ns0_passwd_0")
	assert.True(t, ok)
	assert.Equal(t, "ns0", ns)
}

func TestUserNamespaceMapper_GetUserNamespace_Success_NotFound(t *testing.T) {
	nss := prepareNamespaces([]string{"ns0"}, false)
	ret, err := CreateUserNamespaceMapper(nss)
	assert.Nil(t, err)

	_, ok := ret.GetUserNamespace("unknown", "unknown")
	assert.False(t, ok)
}

func Test_CreateFrontendNamespace_Success(t *testing.T) {
	nss := prepareNamespaces([]string{testNamespace}, false)
	fns, err := CreateFrontendNamespace(testNamespace, &nss[0].Frontend)
	assert.NoError(t, err)
	assert.NotNil(t, fns)
}

func TestFrontendNamespace_Name(t *testing.T) {
	fns := prepareFrontendNamespace(testNamespace)
	assert.Equal(t, testNamespace, fns.Name())
}

func TestFrontendNamespace_IsDatabaseAllowed(t *testing.T) {
	fns := prepareFrontendNamespace(testNamespace)
	assert.True(t, fns.IsDatabaseAllowed(testAllowedDB))
	assert.False(t, fns.IsDatabaseAllowed("unknown"))
}

func TestFrontendNamespace_ListAllowedDatabases(t *testing.T) {
	fns := prepareFrontendNamespace(testNamespace)
	assert.Equal(t, []string{testAllowedDB}, fns.ListAllowedDatabases())
}

func prepareNamespaces(names []string, withDuplicatedUser bool) []*config.Namespace {
	var ret []*config.Namespace
	for _, name := range names {
		userInfos := getTestNamespaceUserInfo(name, 2)
		if withDuplicatedUser {
			userInfos = append(userInfos, getDuplicatedUserInfo())
		}
		ns := &config.Namespace{
			Namespace: name,
			Frontend: config.FrontendNamespace{
				AllowedDBs: []string{testAllowedDB},
				Users:      userInfos,
			},
		}
		ret = append(ret, ns)
	}
	return ret
}

func prepareFrontendNamespace(name string) *FrontendNamespace {
	nss := prepareNamespaces([]string{name}, false)
	fns, _ := CreateFrontendNamespace(name, &nss[0].Frontend)
	return fns
}

func getTestNamespaceUserInfo(name string, count int) []config.FrontendUserInfo {
	var ret []config.FrontendUserInfo
	for i := 0; i < count; i++ {
		ret = append(ret, getUserInfo(name, i))
	}
	return ret
}

func getUserInfo(ns string, idx int) config.FrontendUserInfo {
	return config.FrontendUserInfo{
		Username: fmt.Sprintf("%s_user_%d", ns, idx),
		Password: fmt.Sprintf("%s_passwd_%d", ns, idx),
	}
}

func getDuplicatedUserInfo() config.FrontendUserInfo {
	return config.FrontendUserInfo{
		Username: fmt.Sprintf("%s_user", duplicateUsername),
		Password: fmt.Sprintf("%s_passwd", duplicatePassword),
	}
}
