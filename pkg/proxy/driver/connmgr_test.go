package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var connmgrMockError = errors.New("mock error")

type BackendConnManagerTestSuite struct {
	suite.Suite

	mockConn *MockPooledBackendConn
	mockNs   *MockNamespace
	mockMgr  *BackendConnManager
}

func (b *BackendConnManagerTestSuite) SetupSuite() {
}

func (b *BackendConnManagerTestSuite) SetupTest() {
	b.mockConn = new(MockPooledBackendConn)
	b.mockNs = new(MockNamespace)
	b.mockNs.On("Name").Return("mock_namespace")
	b.mockMgr = NewBackendConnManager(getGlobalFSM(), b.mockNs)
}

func (b *BackendConnManagerTestSuite) TearDownTest() {
}

func (b *BackendConnManagerTestSuite) TearDownSuite() {
}

func (b *BackendConnManagerTestSuite) prepareConnMgrStatus(state FSMState) {
	b.mockMgr.state = state
	if !b.mockMgr.state.IsAutoCommit() || b.mockMgr.state.IsInTransaction() || b.mockMgr.state.IsPrepare() {
		b.mockMgr.txnConn = b.mockConn
	}
}

func (b *BackendConnManagerTestSuite) assertConnMgrStatusCorrect(state FSMState) {
	switch state {
	case State0:
		require.NotNil(b.T(), b.mockMgr.txnConn)
		require.False(b.T(), b.mockMgr.isPrepared)
	case State1:
		require.NotNil(b.T(), b.mockMgr.txnConn)
		require.False(b.T(), b.mockMgr.isPrepared)
	case State2:
		require.Nil(b.T(), b.mockMgr.txnConn)
		require.False(b.T(), b.mockMgr.isPrepared)
	case State3:
		require.NotNil(b.T(), b.mockMgr.txnConn)
		require.False(b.T(), b.mockMgr.isPrepared)
	case State4:
		require.NotNil(b.T(), b.mockMgr.txnConn)
		require.True(b.T(), b.mockMgr.isPrepared)
	case State5:
		require.NotNil(b.T(), b.mockMgr.txnConn)
		require.True(b.T(), b.mockMgr.isPrepared)
	case State6:
		require.Nil(b.T(), b.mockMgr.txnConn)
		require.True(b.T(), b.mockMgr.isPrepared)
	case State7:
		require.NotNil(b.T(), b.mockMgr.txnConn)
		require.True(b.T(), b.mockMgr.isPrepared)
	default:
		b.T().FailNow()
	}
}

type BackendConnManagerTestCase struct {
	suite *BackendConnManagerTestSuite

	CurrentState FSMState
	TargetState  FSMState

	Prepare      func()
	RunAndAssert func()
}

func (b *BackendConnManagerTestCase) Run() {
	b.suite.prepareConnMgrStatus(b.CurrentState)
	b.Prepare()
	b.RunAndAssert()
	require.Equal(b.suite.T(), b.TargetState, b.suite.mockMgr.state)
	b.suite.assertConnMgrStatusCorrect(b.TargetState)
}

func (b *BackendConnManagerTestSuite) Test_State0_Begin_Success() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State0,
		TargetState:  State1,
		Prepare: func() {
			b.mockConn.On("Begin").Return(nil).Once()
		},
		RunAndAssert: func() {
			ctx := context.Background()
			err := b.mockMgr.Begin(ctx)
			require.NoError(b.T(), err)
			b.mockConn.AssertCalled(b.T(), "Begin")
		},
	}

	tc.Run()
}

func (b *BackendConnManagerTestSuite) Test_State0_Begin_Error() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State0,
		TargetState:  State0,
		Prepare: func() {
			b.mockConn.On("Begin").Return(connmgrMockError).Once()
		},
		RunAndAssert: func() {
			ctx := context.Background()
			err := b.mockMgr.Begin(ctx)
			require.EqualError(b.T(), err, connmgrMockError.Error())
			b.mockConn.AssertCalled(b.T(), "Begin")
		},
	}

	tc.Run()
}

func TestBackendConnManagerTestSuite(t *testing.T) {
	suite.Run(t, new(BackendConnManagerTestSuite))
}
