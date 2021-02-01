package driver

import (
	"context"
	"database/sql/driver"
	"sync"

	"github.com/pingcap-incubator/weir/pkg/proxy/metrics"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pkg/errors"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

type BackendConnManager struct {
	fsm   *FSM
	state FSMState

	ns Namespace

	mu      sync.Mutex
	txnConn PooledBackendConn

	// TODO: use stmt id set
	isPrepared bool
}

func NewBackendConnManager(fsm *FSM, ns Namespace) *BackendConnManager {
	return &BackendConnManager{
		fsm:        fsm,
		state:      stateInitial,
		ns:         ns,
		isPrepared: false,
	}
}

func (f *BackendConnManager) MergeStatus(svw *SessionVarsWrapper) {
	f.mu.Lock()
	defer f.mu.Unlock()

	svw.SetStatusFlag(mysql.ServerStatusInTrans, f.state.IsInTransaction())
	svw.SetStatusFlag(mysql.ServerStatusAutocommit, f.state.IsAutoCommit())
}

func (f *BackendConnManager) Query(ctx context.Context, db, sql string) (*gomysql.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	ret, err := f.fsm.Call(ctx, EventQuery, f, db, sql)
	if err != nil {
		return nil, err
	}
	return ret.(*gomysql.Result), nil
}

func (f *BackendConnManager) SetAutoCommit(ctx context.Context, autocommit bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var err error
	if autocommit {
		_, err = f.fsm.Call(ctx, EventEnableAutoCommit, f)
	} else {
		_, err = f.fsm.Call(ctx, EventDisableAutoCommit, f)
	}
	return err
}

func (f *BackendConnManager) Begin(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := f.fsm.Call(ctx, EventBegin, f)
	return err
}

func (f *BackendConnManager) CommitOrRollback(ctx context.Context, commit bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := f.fsm.Call(ctx, EventCommitOrRollback, f, commit)
	return err
}

func (f *BackendConnManager) StmtPrepare(ctx context.Context, db, sql string) (Stmt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	ret, err := f.fsm.Call(ctx, EventStmtPrepare, f, db, sql)
	if err != nil {
		return nil, err
	}
	return ret.(Stmt), nil
}

func (f *BackendConnManager) StmtExecuteForward(ctx context.Context, stmtId int, data []byte) (*gomysql.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	ret, err := f.fsm.Call(ctx, EventStmtForwardData, f, stmtId, data)
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, nil
	}
	return ret.(*gomysql.Result), nil
}

func (f *BackendConnManager) StmtClose(ctx context.Context, stmtId int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := f.fsm.Call(ctx, EventStmtClose, f, stmtId)
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
	f.unsetAttachedConn()
	return nil
}

func (f *BackendConnManager) queryWithoutTxn(ctx context.Context, db, sql string) (*gomysql.Result, error) {
	var err error
	conn, err := f.ns.GetPooledConn(ctx)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil && isConnError(err) {
			if errClose := conn.ErrorClose(); err != nil {
				logutil.BgLogger().Error("close backend conn error", zap.Error(errClose))
			}
		} else {
			conn.PutBack()
		}
	}()

	if err = conn.UseDB(db); err != nil {
		return nil, err
	}

	var ret *gomysql.Result
	ret, err = conn.Execute(sql)
	return ret, err
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
	f.unsetAttachedConn()
}

func (f *BackendConnManager) setAttachedConn(conn PooledBackendConn) {
	f.txnConn = conn
	metrics.QueryCtxAttachedConnGauge.WithLabelValues(f.ns.Name()).Inc()
}

func (f *BackendConnManager) unsetAttachedConn() {
	f.txnConn = nil
	metrics.QueryCtxAttachedConnGauge.WithLabelValues(f.ns.Name()).Desc()
}

func errClosePooledBackendConn(conn PooledBackendConn, ns string) {
	if err := conn.ErrorClose(); err != nil {
		logutil.BgLogger().Error("close backend conn error", zap.Error(err), zap.String("namespace", ns))
	}
}

func isConnError(err error) bool {
	return errors.As(err, &gomysql.ErrBadConn) || errors.As(err, &driver.ErrBadConn)
}
