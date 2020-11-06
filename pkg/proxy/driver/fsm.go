package driver

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/siddontang/go-mysql/mysql"
)

type FSMState int
type FSMEvent int

const (
	StateUnknown FSMState = -1
	State0       FSMState = iota
	State1
	State2
	State3
	State4
	State5
	State6
	State7
)

const (
	stateInitial = State2
)

const (
	EventUnknown FSMEvent = -1
	EventQuery   FSMEvent = iota
	EventBegin
	EventCommitOrRollback
	EventDisableAutoCommit
	EventEnableAutoCommit

	EventStmtPrepare
	EventStmtForwardData // execute, reset, send_long_data
	EventStmtClose
)

type FSM struct {
	handlersV2 map[FSMState]map[FSMEvent]*FSMHandlerWrapper
}

type FSMHandlerWrapper struct {
	NewState        FSMState
	MustChangeState bool
	Handle          FSMHandler
}

type FSMHandler func(conn *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error)

func noopHandler(conn *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	return nil, nil
}

func NewFSM() *FSM {
	return &FSM{
		handlersV2: make(map[FSMState]map[FSMEvent]*FSMHandlerWrapper),
	}
}

func (q *FSM) Init() {
	// in state0, txnConn must be non nil, so we don't check txnConn nil in handlers
	q.MustRegisterActionV2(State0, State0, EventDisableAutoCommit, false, noopHandler)
	q.MustRegisterActionV2(State0, State0, EventCommitOrRollback, false, noopHandler)
	q.MustRegisterActionV2(State0, State1, EventBegin, false, fsmHandler_State0_EventBegin)
	q.MustRegisterActionV2(State0, State1, EventQuery, false, fsmHandler_Transaction_EventQuery)
	q.MustRegisterActionV2(State0, State2, EventEnableAutoCommit, true, fsmHandler_State0_EventEnableAutoCommit)

	q.MustRegisterActionV2(State1, State1, EventDisableAutoCommit, false, noopHandler)
	q.MustRegisterActionV2(State1, State1, EventBegin, false, noopHandler)
	q.MustRegisterActionV2(State1, State1, EventQuery, false, fsmHandler_Transaction_EventQuery)
	q.MustRegisterActionV2(State1, State0, EventCommitOrRollback, true, fsmHandler_State1_EventCommitOrRollback)  // 防止出现死循环, ok不变, err关闭后重新拿一个连接出来 (这里就有坑了, 如果拿不出来要怎么办?)
	q.MustRegisterActionV2(State1, State3, EventEnableAutoCommit, false, fsmHandler_State1_EventEnableAutoCommit) // 小心! error时不能关闭连接 (还在事务中), 所以只能保持状态不变 (是否要关闭连接, 回滚事务?)

	q.MustRegisterActionV2(State2, State2, EventEnableAutoCommit, false, noopHandler)
	q.MustRegisterActionV2(State2, State2, EventCommitOrRollback, false, noopHandler)
	q.MustRegisterActionV2(State2, State2, EventQuery, false, fsmHandler_NoTransaction_EventQuery)
	q.MustRegisterActionV2(State2, State0, EventDisableAutoCommit, false, fsmHandler_State2_EventDisableAutoCommit) // 设置失败时直接关闭连接
	q.MustRegisterActionV2(State2, State3, EventBegin, false, fsmHandler_State2_EventBegin)

	q.MustRegisterActionV2(State3, State3, EventEnableAutoCommit, false, noopHandler)
	q.MustRegisterActionV2(State3, State3, EventBegin, false, noopHandler)
	q.MustRegisterActionV2(State3, State3, EventQuery, false, fsmHandler_Transaction_EventQuery)
	q.MustRegisterActionV2(State3, State1, EventDisableAutoCommit, false, fsmHandler_State3_EventDisableAutoCommit) // 小心! error时不能关闭连接 (是否要关闭连接, 回滚事务?)
	q.MustRegisterActionV2(State3, State2, EventCommitOrRollback, true, fsmHandler_State3_EventCommitOrRollback)    // ok回收, err关闭
}

func (q *FSM) MustRegisterActionV2(state FSMState, newState FSMState, event FSMEvent, mustChangeState bool, handler FSMHandler) {
	handlerWrapper := &FSMHandlerWrapper{
		NewState:        newState,
		MustChangeState: mustChangeState,
		Handle:          handler,
	}

	_, ok := q.handlersV2[state]
	if !ok {
		q.handlersV2[state] = make(map[FSMEvent]*FSMHandlerWrapper)
	}
	_, ok = q.handlersV2[state][event]
	if ok {
		logutil.BgLogger().Panic("duplicated fsm handler")
	}
	q.handlersV2[state][event] = handlerWrapper
}

func (q *FSM) CallV2(ctx context.Context, event FSMEvent, conn *BackendConnManager, args ...interface{}) (*mysql.Result, error) {
	action, ok := q.getActionV2(conn.state, event)
	if !ok {
		return nil, fmt.Errorf("fsm handler not found")
	}
	ret, err := action.Handle(conn, ctx, args...)
	if action.MustChangeState || err == nil {
		conn.state = action.NewState
	}
	return ret, err
}

func (q *FSM) getActionV2(state FSMState, event FSMEvent) (*FSMHandlerWrapper, bool) {
	eventHandlers, ok := q.handlersV2[state]
	if !ok {
		return nil, false
	}
	eventHandler, ok := eventHandlers[event]
	return eventHandler, ok
}

func fsmHandler_Transaction_EventQuery(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	sql := args[0].(string)
	return b.queryInTxn(ctx, sql)
}

func fsmHandler_State0_EventBegin(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	err := b.txnConn.Begin()
	return nil, err
}

func fsmHandler_State0_EventEnableAutoCommit(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	err := b.txnConn.SetAutoCommit(true)
	b.releaseAttachedConn(err)
	return nil, err
}

// TODO(eastfisher): if backend conn is broken, how to exit FSM?
func fsmHandler_State1_EventCommitOrRollback(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	commit := args[0].(bool)
	var err error
	if commit {
		err = b.txnConn.Commit()
	} else {
		err = b.txnConn.Rollback()
	}
	return nil, err
}

func fsmHandler_State1_EventEnableAutoCommit(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	err := b.txnConn.SetAutoCommit(true)
	return nil, err
}

func fsmHandler_NoTransaction_EventQuery(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	sql := args[0].(string)
	return b.queryWithoutTxn(ctx, sql)
}

func fsmHandler_State2_EventDisableAutoCommit(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	conn, err := b.ns.GetPooledConn(ctx)
	if err != nil {
		return nil, err
	}
	err = conn.SetAutoCommit(false)
	if err != nil {
		errClosePooledBackendConn(conn, b.ns.Name())
		return nil, err
	}
	b.txnConn = conn
	return nil, nil
}

func fsmHandler_State2_EventBegin(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	conn, err := b.ns.GetPooledConn(ctx)
	if err != nil {
		return nil, err
	}
	if err = conn.Begin(); err != nil {
		errClosePooledBackendConn(conn, b.ns.Name())
		return nil, err
	}
	b.txnConn = conn
	return nil, nil
}

func fsmHandler_State3_EventDisableAutoCommit(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	err := b.txnConn.SetAutoCommit(false)
	return nil, err
}

func fsmHandler_State3_EventCommitOrRollback(b *BackendConnManager, ctx context.Context, args ...interface{}) (*mysql.Result, error) {
	commit := args[0].(bool)
	var err error
	if commit {
		err = b.txnConn.Commit()
	} else {
		err = b.txnConn.Rollback()
	}

	if err != nil {
		_ = b.txnConn.Rollback()
		errClosePooledBackendConn(b.txnConn, b.ns.Name())
	} else {
		b.txnConn.PutBack()
	}

	b.txnConn = nil
	return nil, err
}
