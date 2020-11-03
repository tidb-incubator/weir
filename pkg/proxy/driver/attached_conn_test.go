package driver

import (
	"context"
	"errors"
	"testing"

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
	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil)
	mockPooledBackendConn.On("Begin").Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())
}

func (a *AttachedConnTestSuite) Test_Begin_AutoCommit_Twice_Success() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil)
	mockPooledBackendConn.On("Begin").Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())

	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil)
	mockPooledBackendConn.On("Begin").Return(nil)

	err = a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.NotNil(a.T(), a.mockHolder.txnConn)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())
}

func (a *AttachedConnTestSuite) Test_Begin_AutoCommit_Error_SetAutoCommit() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", true).Return(mockError)
	mockPooledBackendConn.On("ErrorClose").Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.Begin(ctx)
	require.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_Begin_AutoCommit_Error_Begin() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil)
	mockPooledBackendConn.On("Begin").Return(mockError)
	mockPooledBackendConn.On("ErrorClose").Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.Begin(ctx)
	require.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

// FIXME
func (a *AttachedConnTestSuite) Test_Commit_AutoCommit_WithoutBegin_Success() {
	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil)
	mockPooledBackendConn.On("Commit").Return(nil)

	err := a.mockHolder.CommitOrRollback(true)
	require.NoError(a.T(), err)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

// FIXME
func (a *AttachedConnTestSuite) Test_Rollback_AutoCommit_WithoutBegin_Success() {
	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil)
	mockPooledBackendConn.On("Rollback").Return(nil)

	err := a.mockHolder.CommitOrRollback(false)
	require.NoError(a.T(), err)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_Commit_AutoCommit_WithBegin_Success() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil)
	mockPooledBackendConn.On("Begin").Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)

	mockPooledBackendConn.On("Commit").Return(nil)
	mockPooledBackendConn.On("PutBack").Return()

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
	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil)
	mockPooledBackendConn.On("Begin").Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.Begin(ctx)
	require.NoError(a.T(), err)
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), true, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)

	mockPooledBackendConn.On("Commit").Return(mockError)
	mockPooledBackendConn.On("ErrorClose").Return(nil)

	err = a.mockHolder.CommitOrRollback(true)
	require.EqualError(a.T(), err, mockError.Error())
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_AutoCommit_Disable_Success() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", false).Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.NoError(a.T(), err)
	require.Equal(a.T(), false, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_AutoCommit_DisableAndThenEnable_Success() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", false).Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.NoError(a.T(), err)
	require.Equal(a.T(), false, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)

	mockPooledBackendConn.On("SetAutoCommit", true).Return(nil)
	mockPooledBackendConn.On("PutBack").Return()

	err = a.mockHolder.SetAutoCommit(ctx, true)
	require.NoError(a.T(), err)
	mockPooledBackendConn.AssertCalled(a.T(), "PutBack")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_AutoCommit_Error_GetPooledConn() {
	ctx := context.Background()

	a.mockNs.On("GetPooledConn", ctx).Return(nil, mockError)

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.EqualError(a.T(), err, mockError.Error())
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func (a *AttachedConnTestSuite) Test_AutoCommit_Error_DisableAutoCommit() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", false).Return(mockError)
	mockPooledBackendConn.On("ErrorClose").Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), true, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

// FIXME
func (a *AttachedConnTestSuite) Test_AutoCommit_DisableSuccess_AndThen_EnableError() {
	ctx := context.Background()

	mockPooledBackendConn := new(MockPooledBackendConn)
	mockPooledBackendConn.On("SetAutoCommit", false).Return(nil)
	a.mockNs.On("GetPooledConn", ctx).Return(mockPooledBackendConn, nil)

	err := a.mockHolder.SetAutoCommit(ctx, false)
	require.NoError(a.T(), err)
	require.Equal(a.T(), false, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.NotNil(a.T(), a.mockHolder.txnConn)

	mockPooledBackendConn.On("SetAutoCommit", true).Return(mockError)
	mockPooledBackendConn.On("ErrorClose").Return(nil)

	err = a.mockHolder.SetAutoCommit(ctx, true)
	require.EqualError(a.T(), err, mockError.Error())
	mockPooledBackendConn.AssertCalled(a.T(), "ErrorClose")
	require.Equal(a.T(), false, a.mockHolder.IsAutoCommit())
	require.Equal(a.T(), false, a.mockHolder.IsInTransaction())
	require.Nil(a.T(), a.mockHolder.txnConn)
}

func TestAttachedConnTestSuite(t *testing.T) {
	suite.Run(t, new(AttachedConnTestSuite))
}
