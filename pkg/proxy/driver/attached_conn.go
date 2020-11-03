package driver

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

const (
	defaultAutoCommitFlag = true
	defaultInTransFlag    = false
)

type AttachedConnHolder struct {
	isAutoCommitFlag bool
	isInTransFlag    bool
	ns               Namespace
	txnConn          PooledBackendConn
	txnLock          sync.Mutex
}

func NewAttachedConnHolder(ns Namespace) *AttachedConnHolder {
	return &AttachedConnHolder{
		ns:               ns,
		isAutoCommitFlag: defaultAutoCommitFlag,
		isInTransFlag:    defaultInTransFlag,
	}
}

func (a *AttachedConnHolder) MergeStatus(svw *SessionVarsWrapper) {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	svw.SetStatusFlag(mysql.ServerStatusInTrans, a.isInTransaction())
	svw.SetStatusFlag(mysql.ServerStatusAutocommit, a.isAutoCommit())
}

func (a *AttachedConnHolder) IsInTransaction() bool {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	return a.isInTransaction()
}

func (a *AttachedConnHolder) IsAutoCommit() bool {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	return a.isAutoCommit()
}

func (a *AttachedConnHolder) SetAutoCommit(ctx context.Context, autocommit bool) error {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	var err error
	originAutoCommit := a.isAutoCommit()
	defer func() {
		if err != nil {
			a.setAutoCommit(originAutoCommit)
		}
		a.postUseTxnConn(err)
	}()

	a.setAutoCommit(autocommit)
	err = a.initTxnConn(ctx)
	return err
}

func (a *AttachedConnHolder) Begin(ctx context.Context) error {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	var err error
	defer func() {
		a.postUseTxnConn(err)
	}()

	if err = a.initTxnConn(ctx); err != nil {
		return err
	}

	if err = a.txnConn.Begin(); err != nil {
		return err
	}

	a.setInTrans(true)
	return nil
}

func (a *AttachedConnHolder) ExecuteQuery(ctx context.Context, query func(ctx context.Context, conn PooledBackendConn) (*gomysql.Result, error)) (*gomysql.Result, error) {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	var err error
	defer func() {
		a.postUseTxnConn(err)
	}()

	if err = a.initTxnConn(ctx); err != nil {
		return nil, err
	}

	var ret *gomysql.Result
	ret, err = query(ctx, a.txnConn)
	return ret, err
}

func (a *AttachedConnHolder) CommitOrRollback(commit bool) error {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	if a.txnConn == nil {
		a.setInTrans(false)
		return errors.New("txn conn is not set")
	}

	var err error
	defer func() {
		a.postUseTxnConn(err)
	}()

	if commit {
		err = a.txnConn.Commit()
	} else {
		err = a.txnConn.Rollback()
	}
	if err != nil {
		return err
	}

	a.setInTrans(false)
	return nil
}

func (a *AttachedConnHolder) Close() error {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	// TODO: handle in trans conn rollback
	if a.txnConn != nil {
		a.txnConn.PutBack()
	}
	return nil
}

func (a *AttachedConnHolder) setAutoCommit(autocommit bool) {
	a.isAutoCommitFlag = autocommit
}

func (a *AttachedConnHolder) setInTrans(inTrans bool) {
	a.isInTransFlag = inTrans
}

func (a *AttachedConnHolder) isAutoCommit() bool {
	return a.isAutoCommitFlag
}

func (a *AttachedConnHolder) isInTransaction() bool {
	return a.isInTransFlag
}

func (a *AttachedConnHolder) postUseTxnConn(err error) {
	if err != nil {
		if a.txnConn != nil {
			// TODO: if inTransaction, rollback and then close
			if errClose := a.txnConn.ErrorClose(); errClose != nil {
				logutil.BgLogger().Error("close txn conn error", zap.Error(errClose), zap.String("namespace", a.ns.Name()))
			}
			a.txnConn = nil
		}
		a.setInTrans(false)
	} else {
		if a.isAutoCommit() && !a.isInTransaction() && a.txnConn != nil {
			a.txnConn.PutBack()
			a.txnConn = nil
		}
	}
}

func (a *AttachedConnHolder) initTxnConn(ctx context.Context) error {
	if a.txnConn != nil {
		return nil
	}
	conn, err := a.ns.GetPooledConn(ctx)
	if err != nil {
		return err
	}
	a.txnConn = conn
	if err := conn.SetAutoCommit(a.isAutoCommit()); err != nil {
		return err
	}
	return nil
}
