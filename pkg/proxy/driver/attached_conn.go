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
	defaultInPrepare      = false
)

type AttachedConnHolder struct {
	isAutoCommitFlag bool
	isInTransFlag    bool
	ns               Namespace
	txnConn          PooledBackendConn
	txnLock          sync.Mutex

	prepareStmtHolder *prepareStmtHolder
}

type prepareStmtHolder struct {
	stmtIdSet map[int]struct{}
}

func NewAttachedConnHolder(ns Namespace) *AttachedConnHolder {
	return &AttachedConnHolder{
		ns:                ns,
		isAutoCommitFlag:  defaultAutoCommitFlag,
		isInTransFlag:     defaultInTransFlag,
		prepareStmtHolder: newPrepareStmtHolder(),
	}
}

func newPrepareStmtHolder() *prepareStmtHolder {
	return &prepareStmtHolder{
		stmtIdSet: make(map[int]struct{}),
	}
}

func (p *prepareStmtHolder) isInPrepare() bool {
	return len(p.stmtIdSet) != 0
}

func (p *prepareStmtHolder) addStmtId(stmtId int) bool {
	_, ok := p.stmtIdSet[stmtId]
	if ok {
		return false
	}
	p.stmtIdSet[stmtId] = struct{}{}
	return true
}

func (p *prepareStmtHolder) isStmtIdExist(stmtId int) bool {
	_, ok := p.stmtIdSet[stmtId]
	return ok
}

func (p *prepareStmtHolder) removeStmtId(stmtId int) bool {
	_, ok := p.stmtIdSet[stmtId]
	if !ok {
		return false
	}
	delete(p.stmtIdSet, stmtId)
	return true
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

	if !a.isInTransaction() && a.txnConn == nil {
		return nil
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

func (a *AttachedConnHolder) StmtPrepare(ctx context.Context, sql string) (Stmt, error) {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	var err error
	defer func() {
		a.postUseTxnConn(err)
	}()

	if err := a.initTxnConn(ctx); err != nil {
		return nil, err
	}

	stmt, err := a.txnConn.StmtPrepare(sql)
	if err != nil {
		return nil, err
	}

	a.prepareStmtHolder.addStmtId(stmt.ID())
	return stmt, nil
}

func (a *AttachedConnHolder) StmtExecuteForward(stmtId int, data []byte) (*gomysql.Result, error) {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	var err error
	defer func() {
		a.postUseTxnConn(err)
	}()

	if !a.prepareStmtHolder.isStmtIdExist(stmtId) {
		err = errors.New("stmt id not found")
		return nil, err
	}

	if a.txnConn == nil {
		err = errors.New("backend conn is released")
		return nil, err
	}

	var ret *gomysql.Result
	ret, err = a.txnConn.StmtExecuteForward(data)
	return ret, err
}

func (a *AttachedConnHolder) StmtClose(stmtId int) error {
	a.txnLock.Lock()
	defer a.txnLock.Unlock()

	var err error
	defer func() {
		a.postUseTxnConn(err)
	}()

	if !a.prepareStmtHolder.isStmtIdExist(stmtId) {
		err = errors.New("stmt id not found")
		return err
	}

	if err = a.txnConn.StmtClosePrepare(stmtId); err != nil {
		return err
	}

	a.prepareStmtHolder.removeStmtId(stmtId)
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

// TODO(eastfisher): handle prepare stmt
func (a *AttachedConnHolder) postUseTxnConn(err error) {
	if err != nil {
		a.errCloseConn()
		return
	}

	if a.isAutoCommit() && !a.isInTransaction() {
		a.normalPutBackConn()
	}
}

func (a *AttachedConnHolder) errCloseConn() {
	if a.txnConn != nil {
		// TODO: if inTransaction, rollback and then close
		if errClose := a.txnConn.ErrorClose(); errClose != nil {
			logutil.BgLogger().Error("close txn conn error", zap.Error(errClose), zap.String("namespace", a.ns.Name()))
		}
		a.txnConn = nil
	}
	a.setInTrans(false)
}

func (a *AttachedConnHolder) normalPutBackConn() {
	if a.txnConn == nil {
		return
	}
	txnConn := a.txnConn
	a.txnConn = nil
	if !txnConn.IsAutoCommit() {
		if errSet := txnConn.SetAutoCommit(true); errSet != nil {
			logutil.BgLogger().Error("postUseTxnConn putback conn: set txn conn autocommit error, close", zap.Error(errSet), zap.String("namespace", a.ns.Name()))
			if errClose := txnConn.ErrorClose(); errClose != nil {
				logutil.BgLogger().Error("postUseTxnConn putback conn: close txn conn error", zap.Error(errClose), zap.String("namespace", a.ns.Name()))
				return
			}
		}
	}
	txnConn.PutBack()
}

func (a *AttachedConnHolder) initTxnConn(ctx context.Context) error {
	if err := a.trySetTxnConn(ctx); err != nil {
		return err
	}

	if err := a.syncConnAutoCommit(); err != nil {
		return err
	}

	return nil
}

func (a *AttachedConnHolder) trySetTxnConn(ctx context.Context) error {
	if a.txnConn != nil {
		return nil
	}

	conn, err := a.ns.GetPooledConn(ctx)
	if err != nil {
		return err
	}
	a.txnConn = conn
	return nil
}

func (a *AttachedConnHolder) syncConnAutoCommit() error {
	conn := a.txnConn
	if a.isAutoCommit() != conn.IsAutoCommit() {
		if err := conn.SetAutoCommit(a.isAutoCommit()); err != nil {
			return err
		}
	}
	return nil
}
