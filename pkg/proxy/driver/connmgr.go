package driver

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/util/logutil"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

type BackendConnManager struct {
	fsm   *FSM
	state FSMState

	ns Namespace

	mu      sync.Mutex
	txnConn PooledBackendConn
}

func NewBackendConnManager(fsm *FSM, ns Namespace) *BackendConnManager {
	return &BackendConnManager{
		fsm:   fsm,
		state: stateInitial,
		ns:    ns,
	}
}

func (f *BackendConnManager) Query(ctx context.Context, sql string) (*gomysql.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.fsm.CallV2(ctx, EventQuery, f, sql)
}

func (f *BackendConnManager) SetAutoCommit(ctx context.Context, autocommit bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	panic("unimplemented")
}

func (f *BackendConnManager) Begin(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	panic("unimplemented")
}

func (f *BackendConnManager) CommitOrRollback(ctx context.Context, commit bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	panic("unimplemented")
}

func (f *BackendConnManager) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	panic("unimplemented")
}

func (f *BackendConnManager) queryWithoutTxn(ctx context.Context, sql string) (*gomysql.Result, error) {
	conn, err := f.ns.GetPooledConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.PutBack()

	return conn.Execute(sql)
}

func (f *BackendConnManager) handleReleaseTxnConn(err error) {
	if err != nil {
		if errClose := f.txnConn.ErrorClose(); errClose != nil {
			logutil.BgLogger().Error("close backend conn error", zap.Error(err), zap.String("namespace", f.ns.Name()))
		}
	} else {
		f.txnConn.PutBack()
	}
}

func fsmHandler_State0_EventEnableAutoCommit(f *BackendConnManager, ctx context.Context, args ...interface{}) (*gomysql.Result, error) {
	// in state0, txnConn must be non nil
	err := f.txnConn.SetAutoCommit(true)
	f.handleReleaseTxnConn(err)
	return nil, err
}