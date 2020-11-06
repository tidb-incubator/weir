package driver

import (
	"context"
	"sync"

	"github.com/pingcap/parser/mysql"
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
func (f *BackendConnManager) MergeStatus(svw *SessionVarsWrapper) {
	f.mu.Lock()
	defer f.mu.Unlock()

	svw.SetStatusFlag(mysql.ServerStatusInTrans, f.isInTransaction())
	svw.SetStatusFlag(mysql.ServerStatusAutocommit, f.isAutoCommit())
}

func (f *BackendConnManager) Query(ctx context.Context, db, sql string) (*gomysql.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.fsm.CallV2(ctx, EventQuery, f, db, sql)
}

func (f *BackendConnManager) SetAutoCommit(ctx context.Context, autocommit bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var err error
	if autocommit {
		_, err = f.fsm.CallV2(ctx, EventEnableAutoCommit, f)
	} else {
		_, err = f.fsm.CallV2(ctx, EventDisableAutoCommit, f)
	}
	return err
}

func (f *BackendConnManager) Begin(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := f.fsm.CallV2(ctx, EventBegin, f)
	return err
}

func (f *BackendConnManager) CommitOrRollback(ctx context.Context, commit bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := f.fsm.CallV2(ctx, EventCommitOrRollback, f, commit)
	return err
}

// TODO(eastfisher): is it possible to use FSM to manage close?
func (f *BackendConnManager) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.txnConn != nil {
		errClosePooledBackendConn(f.txnConn, f.ns.Name())
	}
	f.state = stateInitial
	f.txnConn = nil
	return nil
}

func (f *BackendConnManager) queryWithoutTxn(ctx context.Context, db, sql string) (*gomysql.Result, error) {
	conn, err := f.ns.GetPooledConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.PutBack()

	if err = conn.UseDB(db); err != nil {
		return nil, err
	}

	return conn.Execute(sql)
}

func (f *BackendConnManager) queryInTxn(ctx context.Context, db, sql string) (*gomysql.Result, error) {
	if err := f.txnConn.UseDB(db); err != nil {
		return nil, err
	}
	return f.txnConn.Execute(sql)
}

func (f *BackendConnManager) releaseAttachedConn(err error) {
	if err != nil {
		errClosePooledBackendConn(f.txnConn, f.ns.Name())
	} else {
		f.txnConn.PutBack()
	}
	f.txnConn = nil
}

func (f *BackendConnManager) isInTransaction() bool {
	return (f.state & FSMStateFlagInTransaction) != 0
}

func (f *BackendConnManager) isAutoCommit() bool {
	return (f.state & FSMStateFlagIsAutoCommit) != 0
}

func (f *BackendConnManager) isInPrepare() bool {
	return (f.state & FSMStateFlagInPrepare) != 0
}

func errClosePooledBackendConn(conn PooledBackendConn, ns string) {
	if err := conn.ErrorClose(); err != nil {
		logutil.BgLogger().Error("close backend conn error", zap.Error(err), zap.String("namespace", ns))
	}
}
