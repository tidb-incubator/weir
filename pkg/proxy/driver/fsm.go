package driver

import (
	"context"
	"errors"
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

type FSMActionResult struct {
	StateChange bool
	NewState    FSMState
	Result      *mysql.Result
	Error       error
}

type EventHandler struct {
	State FSMState
}

type FSMAction func(conn *BackendConnManager, ctx context.Context, args ...interface{}) FSMActionResult

type FSM struct {
	handlers   map[FSMState]map[FSMEvent]FSMAction
	handlersV2 map[FSMState]map[FSMEvent]*FSMHandlerWrapper
}

func NewFSMActionResultWithoutStateChange(ret *mysql.Result, err error) FSMActionResult {
	return NewFSMActionResult(StateUnknown, ret, err)
}

func NewFSMActionResult(newState FSMState, ret *mysql.Result, err error) FSMActionResult {
	return FSMActionResult{
		NewState: newState,
		Result:   ret,
		Error:    err,
	}
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
		handlers: make(map[FSMState]map[FSMEvent]FSMAction),
	}
}

func (q *FSM) Init() {
	q.MustRegisterActionV2(State0, State0, EventDisableAutoCommit, false, noopHandler)
	q.MustRegisterActionV2(State0, State0, EventCommitOrRollback, false, noopHandler)
	q.MustRegisterActionV2(State0, State1, EventBegin, false, nil)
	q.MustRegisterActionV2(State0, State1, EventQuery, false, nil)
	q.MustRegisterActionV2(State0, State2, EventEnableAutoCommit, true, fsmHandler_State0_EventEnableAutoCommit)

	q.MustRegisterActionV2(State1, State1, EventDisableAutoCommit, false, noopHandler)
	q.MustRegisterActionV2(State1, State1, EventBegin, false, noopHandler)
	q.MustRegisterActionV2(State1, State1, EventQuery, false, nil)
	q.MustRegisterActionV2(State1, State0, EventCommitOrRollback, true, nil)  // 防止出现死循环, ok不变, err关闭后重新拿一个连接出来 (这里就有坑了, 如果拿不出来要怎么办?)
	q.MustRegisterActionV2(State1, State3, EventEnableAutoCommit, false, nil) // 小心! error时不能关闭连接 (还在事务中), 所以只能保持状态不变 (是否要关闭连接, 回滚事务?)

	q.MustRegisterActionV2(State2, State2, EventEnableAutoCommit, false, noopHandler)
	q.MustRegisterActionV2(State2, State2, EventCommitOrRollback, false, noopHandler)
	q.MustRegisterActionV2(State2, State2, EventQuery, false, nil)
	q.MustRegisterActionV2(State2, State0, EventDisableAutoCommit, false, nil) // 设置失败时直接关闭连接
	q.MustRegisterActionV2(State2, State3, EventBegin, false, nil)

	q.MustRegisterActionV2(State3, State3, EventEnableAutoCommit, false, noopHandler)
	q.MustRegisterActionV2(State3, State3, EventBegin, false, noopHandler)
	q.MustRegisterActionV2(State3, State3, EventQuery, false, nil)
	q.MustRegisterActionV2(State3, State1, EventDisableAutoCommit, false, nil) // 小心! error时不能关闭连接 (是否要关闭连接, 回滚事务?)
	q.MustRegisterActionV2(State3, State2, EventCommitOrRollback, true, nil)   // ok回收, err关闭
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

func (q *FSM) RegisterAction(state FSMState, event FSMEvent, action FSMAction) error {
	_, ok := q.handlers[state]
	if !ok {
		q.handlers[state] = make(map[FSMEvent]FSMAction)
	}
	_, ok = q.handlers[state][event]
	if ok {
		return errors.New("duplicated handler")
	}
	q.handlers[state][event] = action
	return nil
}

func (q *FSM) Call(ctx context.Context, state FSMState, event FSMEvent, conn *BackendConnManager, args ...interface{}) FSMActionResult {
	action, ok := q.getAction(state, event)
	if !ok {
		NewFSMActionResult(state, nil, fmt.Errorf("fsm action not found"))
	}
	return action(conn, ctx, args...)
}

func (q *FSM) getAction(state FSMState, event FSMEvent) (FSMAction, bool) {
	eventHandlers, ok := q.handlers[state]
	if !ok {
		return nil, false
	}
	eventHandler, ok := eventHandlers[event]
	return eventHandler, ok
}

func (q *FSM) getActionV2(state FSMState, event FSMEvent) (*FSMHandlerWrapper, bool) {
	eventHandlers, ok := q.handlersV2[state]
	if !ok {
		return nil, false
	}
	eventHandler, ok := eventHandlers[event]
	return eventHandler, ok
}
