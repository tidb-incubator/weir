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
	if b.mockMgr.state.IsPrepare() {
		b.mockMgr.isPrepared = true
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
		require.NotNil(b.T(), b.mockMgr.txnConn)
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

	Prepare      func(ctx context.Context)
	RunAndAssert func(ctx context.Context)
}

func (b *BackendConnManagerTestCase) Run() {
	ctx := context.Background()
	b.suite.prepareConnMgrStatus(b.CurrentState)
	b.Prepare(ctx)
	b.RunAndAssert(ctx)
	require.Equal(b.suite.T(), b.TargetState, b.suite.mockMgr.state)
	b.suite.assertConnMgrStatusCorrect(b.TargetState)
}

func (b *BackendConnManagerTestSuite) Test_State0_Begin_Success() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State0,
		TargetState:  State1,
		Prepare: func(ctx context.Context) {
			b.mockConn.On("Begin").Return(nil).Once()
		},
		RunAndAssert: func(ctx context.Context) {
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
		Prepare: func(ctx context.Context) {
			b.mockConn.On("Begin").Return(connmgrMockError).Once()
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.EqualError(b.T(), err, connmgrMockError.Error())
			b.mockConn.AssertCalled(b.T(), "Begin")
		},
	}

	tc.Run()
}

func (b *BackendConnManagerTestSuite) Test_State1_Begin_Success() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State1,
		TargetState:  State1,
		Prepare: func(ctx context.Context) {
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.NoError(b.T(), err)
		},
	}

	tc.Run()
}

func (b *BackendConnManagerTestSuite) Test_State2_Begin_Success() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State2,
		TargetState:  State3,
		Prepare: func(ctx context.Context) {
			b.mockNs.On("GetPooledConn", ctx).Return(b.mockConn, nil)
			b.mockConn.On("Begin").Return(nil).Once()
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.NoError(b.T(), err)
			b.mockNs.AssertCalled(b.T(), "GetPooledConn", ctx)
			b.mockConn.AssertCalled(b.T(), "Begin")
		},
	}

	tc.Run()
}

func (b *BackendConnManagerTestSuite) Test_State2_Begin_Error_GetPooledConn() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State2,
		TargetState:  State2,
		Prepare: func(ctx context.Context) {
			b.mockNs.On("GetPooledConn", ctx).Return(nil, connmgrMockError)
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.EqualError(b.T(), err, connmgrMockError.Error())
			b.mockNs.AssertCalled(b.T(), "GetPooledConn", ctx)
		},
	}

	tc.Run()
}

func (b *BackendConnManagerTestSuite) Test_State2_Begin_Error_Begin() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State2,
		TargetState:  State2,
		Prepare: func(ctx context.Context) {
			b.mockNs.On("GetPooledConn", ctx).Return(b.mockConn, nil)
			b.mockConn.On("Begin").Return(connmgrMockError).Once()
			b.mockConn.On("ErrorClose").Return(nil).Once()
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.EqualError(b.T(), err, connmgrMockError.Error())
			b.mockNs.AssertCalled(b.T(), "GetPooledConn", ctx)
			b.mockConn.AssertCalled(b.T(), "Begin")
			b.mockConn.AssertCalled(b.T(), "ErrorClose")
		},
	}

	tc.Run()
}

func (b *BackendConnManagerTestSuite) Test_State3_Begin_Success() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State3,
		TargetState:  State3,
		Prepare: func(ctx context.Context) {
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.NoError(b.T(), err)
		},
	}

	tc.Run()
}

// same with State0
func (b *BackendConnManagerTestSuite) Test_State4_Begin_Success() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State4,
		TargetState:  State5,
		Prepare: func(ctx context.Context) {
			b.mockConn.On("Begin").Return(nil).Once()
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.NoError(b.T(), err)
			b.mockConn.AssertCalled(b.T(), "Begin")
		},
	}

	tc.Run()
}

// same with State0
func (b *BackendConnManagerTestSuite) Test_State4_Begin_Error() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State4,
		TargetState:  State4,
		Prepare: func(ctx context.Context) {
			b.mockConn.On("Begin").Return(connmgrMockError).Once()
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.EqualError(b.T(), err, connmgrMockError.Error())
			b.mockConn.AssertCalled(b.T(), "Begin")
		},
	}

	tc.Run()
}

// same with State1
func (b *BackendConnManagerTestSuite) Test_State5_Begin_Success() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State5,
		TargetState:  State5,
		Prepare: func(ctx context.Context) {
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.NoError(b.T(), err)
		},
	}

	tc.Run()
}

// same with State0
func (b *BackendConnManagerTestSuite) Test_State6_Begin_Success() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State6,
		TargetState:  State7,
		Prepare: func(ctx context.Context) {
			b.mockConn.On("Begin").Return(nil).Once()
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.NoError(b.T(), err)
			b.mockConn.AssertCalled(b.T(), "Begin")
		},
	}

	tc.Run()
}

// same with State0
func (b *BackendConnManagerTestSuite) Test_State6_Begin_Error() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State6,
		TargetState:  State6,
		Prepare: func(ctx context.Context) {
			b.mockConn.On("Begin").Return(connmgrMockError).Once()
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.EqualError(b.T(), err, connmgrMockError.Error())
			b.mockConn.AssertCalled(b.T(), "Begin")
		},
	}

	tc.Run()
}

// same with State3
func (b *BackendConnManagerTestSuite) Test_State7_Begin_Success() {
	tc := &BackendConnManagerTestCase{
		suite:        b,
		CurrentState: State7,
		TargetState:  State7,
		Prepare: func(ctx context.Context) {
		},
		RunAndAssert: func(ctx context.Context) {
			err := b.mockMgr.Begin(ctx)
			require.NoError(b.T(), err)
		},
	}

	tc.Run()
}

func TestBackendConnManagerTestSuite(t *testing.T) {
	suite.Run(t, new(BackendConnManagerTestSuite))
}
