package driver

import (
	"context"
	"errors"
	"testing"

	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var mockError = errors.New("mock error")

type AttachedConnTestSuite struct {
	suite.Suite
	mockNs     *MockNamespace
	mockHolder *AttachedConnHolder
}

func (a *AttachedConnTestSuite) SetupSuite() {
}

func (a *AttachedConnTestSuite) SetupTest() {
	a.mockNs = new(MockNamespace)
	a.mockNs.On("Name").Return("mock_namespace")
	a.mockHolder = NewAttachedConnHolder(a.mockNs)
}

func (a *AttachedConnTestSuite) TearDownTest() {
}

func (a *AttachedConnTestSuite) TearDownSuite() {
}

func (a *AttachedConnTestSuite) TestNew() {
	require.Equal(a.T(), defaultInTransFlag, a.mockHolder.IsInTransaction())
	require.Equal(a.T(), defaultAutoCommitFlag, a.mockHolder.IsAutoCommit())
}

func (a *AttachedConnTestSuite) Test_Begin_AutoCommit_Success() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(nil).Once()

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())
}

func (a *AttachedConnTestSuite) Test_Begin_AutoCommit_Twice_Success() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(nil).Once()

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())

	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(nil).Once()

	err = a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())
}

func (a *AttachedConnTestSuite) Test_Begin_AutoCommit_Error_Begin() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(mockError).Once()
	mockPooledBackendConn.On("ErrorClose").Return(nil).Once()

	err := a.mockHolder.Begin(ctx)
	require.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_Commit_AutoCommit_WithoutBegin_Success() {
	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("Commit").Return(nil).Once()

	err := a.mockHolder.CommitOrRollback(true)
	require.NoError(a.T(), err)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_Rollback_AutoCommit_WithoutBegin_Success() {
	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("Rollback").Return(nil).Once()

	err := a.mockHolder.CommitOrRollback(false)
	require.NoError(a.T(), err)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_Commit_AutoCommit_WithBegin_Success() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(nil).Once()

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)

	mockPooledBackendConn.On("Commit").Return(nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("PutBack").Return().Once()

	err = a.mockHolder.CommitOrRollback(true)
	require.NoError(a.T(), err)
	mockPooledBackendConn.AssertCalled(a.T(), "PutBack")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_Commit_AutoCommit_WithBegin_ErrorCommit() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(nil).Once()

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)

	mockPooledBackendConn.On("Commit").Return(mockError).Once()
	mockPooledBackendConn.On("ErrorClose").Return(nil).Once()

	err = a.mockHolder.CommitOrRollback(true)
	require.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "Commit")
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_AutoCommit_Disable_Success() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("SetAutoCommit", false).Return(nil).Once()

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.NoError(a.T(), err)
	a.mockNs.AssertCalled(a.T(), "GetPooledConn", ctx)
	mockPooledBackendConn.AssertCalled(a.T(), "SetAutoCommit", false)
	require.Equal(a.T(), false, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_AutoCommit_DisableAndThenEnable_Success() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("SetAutoCommit", false).Return(nil).Once()

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.NoError(a.T(), err)
	a.mockNs.AssertCalled(a.T(), "GetPooledConn", ctx)
	mockPooledBackendConn.AssertCalled(a.T(), "SetAutoCommit", false)
	require.Equal(a.T(), false, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)

	mockPooledBackendConn.On("IsAutoCommit").Return(false).Once()
	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("PutBack").Return().Once()

	err = a.mockHolder.SetAutoCommit(ctx, true)
	require.NoError(a.T(), err)
	mockPooledBackendConn.AssertCalled(a.T(), "SetAutoCommit", true)
	mockPooledBackendConn.AssertCalled(a.T(), "PutBack")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_AutoCommit_Error_GetPooledConn() {
	ctx := context.Background()

	a.mockNs.On("GetPooledConn", ctx).Return(nil, mockError).Once()

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.EqualError(a.T(), err, mockError.Error())
	a.mockNs.AssertCalled(a.T(), "GetPooledConn", ctx)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_AutoCommit_Error_DisableAutoCommit() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("SetAutoCommit", false).Return(mockError).Once()
	mockPooledBackendConn.On("ErrorClose").Return(nil).Once()

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.EqualError(a.T(), err, mockError.Error())
	a.mockNs.AssertCalled(a.T(), "GetPooledConn", ctx)
	mockPooledBackendConn.AssertCalled(a.T(), "SetAutoCommit", false)
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_AutoCommit_DisableSuccess_AndThen_EnableError() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("SetAutoCommit", false).Return(nil).Once()

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.NoError(a.T(), err)
	a.mockNs.AssertCalled(a.T(), "GetPooledConn", ctx)
	mockPooledBackendConn.AssertCalled(a.T(), "SetAutoCommit", false)
	require.Equal(a.T(), false, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)

	mockPooledBackendConn.On("IsAutoCommit").Return(false).Once()
	mockPooledBackendConn.On("SetAutoCommit", true).Return(mockError).Once()
	mockPooledBackendConn.On("ErrorClose").Return(nil).Once()

	err = a.mockHolder.SetAutoCommit(ctx, true)
	require.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "SetAutoCommit", true)
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), false, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_Begin_AndThen_ExecuteQuery_AndThen_Commit_Success_EnableAutoCommit() {
	ctx := context.Background()

	// begin
	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(nil).Once()
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	a.mockNs.AssertCalled(a.T(), "GetPooledConn", ctx)
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())

	// execute
	sql := "SELECT * FROM tbl1"
	expectResult := &gomysql.Result{}
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Execute", sql).Return(expectResult, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()

	queryFunc := func(ctx context.Context, conn PooledBackendConn) (*gomysql.Result, error) {
		return conn.Execute(sql)
	}

	ret, err := a.mockHolder.ExecuteQuery(ctx, queryFunc)
	assert.NoError(a.T(), err)
	assert.Equal(a.T(), expectResult, ret)
	mockPooledBackendConn.AssertCalled(a.T(), "Execute", sql)
	assert.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())

	// commit
	mockPooledBackendConn.On("Commit").Return(nil)
	mockPooledBackendConn.On("PutBack").Return()

	err = a.mockHolder.CommitOrRollback(true)
	require.NoError(a.T(), err)
	mockPooledBackendConn.AssertCalled(a.T(), "Commit")
	mockPooledBackendConn.AssertCalled(a.T(), "PutBack")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_Begin_AndThen_ExecuteQuery_Error_AndThen_Commit_Success_EnableAutoCommit() {
	ctx := context.Background()

	// begin
	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(nil).Once()
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	a.mockNs.AssertCalled(a.T(), "GetPooledConn", ctx)
	mockPooledBackendConn.AssertCalled(a.T(), "Begin")
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())

	// execute
	sql := "SELECT * FROM tbl1" // this is a correct statement, but execution may cause error
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Execute", sql).Return(nil, mockError).Once()

	queryFunc := func(ctx context.Context, conn PooledBackendConn) (*gomysql.Result, error) {
		return conn.Execute(sql)
	}

	_, err = a.mockHolder.ExecuteQuery(ctx, queryFunc)
	assert.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "Execute", sql)
	assert.Nil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())

	// commit
	mockPooledBackendConn.On("Commit").Return(nil)
	mockPooledBackendConn.On("PutBack").Return()

	err = a.mockHolder.CommitOrRollback(true)
	require.NoError(a.T(), err)
	mockPooledBackendConn.AssertCalled(a.T(), "Commit")
	mockPooledBackendConn.AssertCalled(a.T(), "PutBack")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_Begin_AndThen_ExecuteQuery_Error_AndThen_Commit_Error_EnableAutoCommit() {
	ctx := context.Background()

	// begin
	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(nil).Once()
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	a.mockNs.AssertCalled(a.T(), "GetPooledConn", ctx)
	mockPooledBackendConn.AssertCalled(a.T(), "Begin")
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())

	// execute
	sql := "SELECT * FROM tbl1" // this is a correct statement, but execution may cause error
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Execute", sql).Return(nil, mockError).Once()

	queryFunc := func(ctx context.Context, conn PooledBackendConn) (*gomysql.Result, error) {
		return conn.Execute(sql)
	}

	_, err = a.mockHolder.ExecuteQuery(ctx, queryFunc)
	assert.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "Execute", sql)
	assert.Nil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())

	// commit error should close backend conn
	mockPooledBackendConn.On("Commit").Return(mockError)
	mockPooledBackendConn.On("ErrorClose").Return(nil)

	err = a.mockHolder.CommitOrRollback(true)
	require.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "Commit")
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

// FIXME(eastfisher): need rollback
func (a *AttachedConnTestSuite) Test_Begin_AndThen_ExecuteQuery_AndThen_Commit_Error_EnableAutoCommit() {
	ctx := context.Background()

	// begin
	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Begin").Return(nil).Once()
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil).Once()

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	a.mockNs.AssertCalled(a.T(), "GetPooledConn", ctx)
	mockPooledBackendConn.AssertCalled(a.T(), "Begin")
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())

	// execute
	sql := "SELECT * FROM tbl1"
	expectResult := &gomysql.Result{}
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Execute", sql).Return(expectResult, nil).Once()
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()

	queryFunc := func(ctx context.Context, conn PooledBackendConn) (*gomysql.Result, error) {
		return conn.Execute(sql)
	}

	ret, err := a.mockHolder.ExecuteQuery(ctx, queryFunc)
	assert.NoError(a.T(), err)
	assert.Equal(a.T(), expectResult, ret)
	mockPooledBackendConn.AssertCalled(a.T(), "Execute", sql)
	assert.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())

	// commit
	mockPooledBackendConn.On("IsAutoCommit").Return(true).Once()
	mockPooledBackendConn.On("Commit").Return(mockError).Once()
	//mockPooledBackendConn.On("Rollback").Return(nil).Once()
	mockPooledBackendConn.On("ErrorClose").Return(nil).Once()

	err = a.mockHolder.CommitOrRollback(true)
	require.EqualError(a.T(), err, mockError.Error())

	// FIXME(eastfisher): rollback
	//mockPooledBackendConn.AssertCalled(a.T(), "Rollback")
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func TestAttachedConnTestSuite(t *testing.T) {
	suite.Run(t, new(AttachedConnTestSuite))
}
