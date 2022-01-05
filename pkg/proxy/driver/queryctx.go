package driver

import (
	"context"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/tidb-incubator/weir/pkg/proxy/server"
	wast "github.com/tidb-incubator/weir/pkg/util/ast"
	cb "github.com/tidb-incubator/weir/pkg/util/rate_limit_breaker/circuit_breaker"
)

// Server information.
const (
	ServerStatusInTrans            uint16 = 0x0001
	ServerStatusAutocommit         uint16 = 0x0002
	ServerMoreResultsExists        uint16 = 0x0008
	ServerStatusNoGoodIndexUsed    uint16 = 0x0010
	ServerStatusNoIndexUsed        uint16 = 0x0020
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSend        uint16 = 0x0080
	ServerStatusDBDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscaped uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerStatusWasSlow            uint16 = 0x0800
	ServerPSOutParams              uint16 = 0x1000
)

type QueryCtxImpl struct {
	connId      uint64
	nsmgr       NamespaceManager
	ns          Namespace
	currentDB   string
	parser      *parser.Parser
	sessionVars *SessionVarsWrapper

	connMgr *BackendConnManager
}

func NewQueryCtxImpl(nsmgr NamespaceManager, connId uint64) *QueryCtxImpl {
	return &QueryCtxImpl{
		connId:      connId,
		nsmgr:       nsmgr,
		parser:      parser.New(),
		sessionVars: NewSessionVarsWrapper(variable.NewSessionVars()),
	}
}

func (q *QueryCtxImpl) Status() uint16 {
	return q.sessionVars.Status()
}

func (q *QueryCtxImpl) LastInsertID() uint64 {
	return q.sessionVars.LastInsertID()
}

func (q *QueryCtxImpl) LastMessage() string {
	return q.sessionVars.GetMessage()
}

func (q *QueryCtxImpl) AffectedRows() uint64 {
	return q.sessionVars.AffectedRows()
}

// TODO(eastfisher): implement this function
func (*QueryCtxImpl) Value(key fmt.Stringer) interface{} {
	return nil
}

// TODO(eastfisher): implement this function
func (*QueryCtxImpl) SetValue(key fmt.Stringer, value interface{}) {
	return
}

// TODO(eastfisher): Does weir need to support this?
func (*QueryCtxImpl) SetProcessInfo(sql string, t time.Time, command byte, maxExecutionTime uint64) {
	return
}

// TODO(eastfisher): remove this function when Driver interface is changed
func (*QueryCtxImpl) CommitTxn(ctx context.Context) error {
	return nil
}

// TODO(eastfisher): remove this function when Driver interface is changed
func (*QueryCtxImpl) RollbackTxn() {
	return
}

// TODO(eastfisher): implement this function
func (*QueryCtxImpl) WarningCount() uint16 {
	return 0
}

func (q *QueryCtxImpl) CurrentDB() string {
	return q.currentDB
}

func (q *QueryCtxImpl) Execute(ctx context.Context, sql string) (*gomysql.Result, error) {
	charsetInfo, collation := q.sessionVars.GetCharsetInfo()
	stmt, err := q.parser.ParseOneStmt(sql, charsetInfo, collation)
	if err != nil {
		return nil, err
	}

	tableName := wast.ExtractFirstTableNameFromStmt(stmt)
	ctx = wast.CtxWithAstTableName(ctx, tableName)

	sqlParadigm, err := extractStmtParadigm(stmt)
	if err != nil {
		return nil, err
	}
	sqlDigest := crc32.ChecksumIEEE([]byte(sqlParadigm))

	if q.isStmtDenied(ctx, sqlDigest) {
		q.recordDeniedQueryMetrics(ctx, stmt)
		return nil, mysql.NewErrf(mysql.ErrUnknown, "statement is denied")
	}

	if q.isStmtAllowed(ctx, sqlDigest) {
		return q.execute(ctx, sql, stmt)
	}

	if !q.isStmtNeedToCheckCircuitBreaking(stmt) {
		return q.execute(ctx, sql, stmt)
	}

	if rateLimitKey, ok := q.getRateLimiterKey(ctx, q.ns.GetRateLimiter()); ok && rateLimitKey != "" {
		if err := q.ns.GetRateLimiter().Limit(ctx, rateLimitKey); err != nil {
			return nil, err
		}
	}

	return q.executeWithBreakerInterceptor(ctx, stmt, sql, sqlDigest)
}

func (q *QueryCtxImpl) executeWithBreakerInterceptor(ctx context.Context, stmtNode ast.StmtNode, sql string, sqlDigest uint32) (*gomysql.Result, error) {
	breaker, err := q.ns.GetBreaker()
	if err != nil {
		return nil, err
	}

	brName, ok := q.getBreakerName(ctx, sql, breaker)
	if !ok {
		return q.execute(ctx, sql, stmtNode)
	}

	status, brNum := breaker.Status(brName)
	if status == cb.CircuitBreakerStatusOpen {
		return nil, cb.ErrCircuitBreak
	}

	if status == cb.CircuitBreakerStatusHalfOpen {
		if !breaker.CASHalfOpenProbeSent(brName, brNum, true) {
			return nil, cb.ErrCircuitBreak
		}
	}

	var triggerFlag int32 = -1
	connId := q.connId
	if err := breaker.AddTimeWheelTask(brName, connId, &triggerFlag); err != nil {
		return nil, err
	}
	// TODO: handle err
	defer breaker.RemoveTimeWheelTask(connId)

	ret, err := q.execute(ctx, sql, stmtNode)

	if triggerFlag == -1 {
		// TODO: handle err
		breaker.Hit(brName, -1, false)
	}

	return ret, err
}

func (q *QueryCtxImpl) execute(ctx context.Context, sql string, stmtNode ast.StmtNode) (*gomysql.Result, error) {
	startTime := time.Now()
	ret, err := q.executeStmt(ctx, sql, stmtNode)
	durationMilliSecond := float64(time.Since(startTime)) / float64(time.Second)
	q.recordQueryMetrics(ctx, stmtNode, err, durationMilliSecond)
	return ret, err
}

// TODO(eastfisher): remove this function when Driver interface is changed
func (*QueryCtxImpl) ExecuteInternal(ctx context.Context, sql string) ([]server.ResultSet, error) {
	return nil, nil
}

func (q *QueryCtxImpl) SetClientCapability(capability uint32) {
	q.sessionVars.SetClientCapability(capability)
}

func (q *QueryCtxImpl) Prepare(ctx context.Context, sql string) (stmtId int, columns, params []*server.ColumnInfo, err error) {
	stmt, err := q.connMgr.StmtPrepare(ctx, q.currentDB, sql)
	if err != nil {
		return -1, nil, nil, err
	}

	columns = createBinaryPrepareColumns(stmt.ColumnNum())
	params = createBinaryPrepareParams(stmt.ParamNum())
	return stmt.ID(), columns, params, nil
}

func (q *QueryCtxImpl) StmtExecuteForward(ctx context.Context, stmtId int, data []byte) (*gomysql.Result, error) {
	return q.connMgr.StmtExecuteForward(ctx, stmtId, data)
}

func (q *QueryCtxImpl) StmtClose(ctx context.Context, stmtId int) error {
	return q.connMgr.StmtClose(ctx, stmtId)
}

func (q *QueryCtxImpl) FieldList(tableName string) ([]*server.ColumnInfo, error) {
	conn, err := q.ns.GetPooledConn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.PutBack()

	if err := conn.UseDB(q.currentDB); err != nil {
		return nil, err
	}

	fields, err := conn.FieldList(tableName, "")
	if err != nil {
		return nil, err
	}

	columns := convertFieldsToColumnInfos(fields)
	return columns, nil
}

func (q *QueryCtxImpl) Close() error {
	if q.ns != nil {
		q.ns.DescConnCount()
	}
	if q.connMgr != nil {
		return q.connMgr.Close()
	}
	return nil
}

func (q *QueryCtxImpl) Auth(user *auth.UserIdentity, pwd []byte, salt []byte) bool {
	ns, ok := q.nsmgr.Auth(user.Username, pwd, salt)
	if !ok {
		return false
	}
	q.ns = ns
	q.initAttachedConnHolder()
	q.ns.IncrConnCount()
	return true
}

// TODO(eastfisher): does weir need to support show processlist?
func (*QueryCtxImpl) ShowProcess() *util.ProcessInfo {
	return nil
}

func (q *QueryCtxImpl) GetSessionVars() *variable.SessionVars {
	return q.sessionVars.sessionVars
}

func (q *QueryCtxImpl) SetCommandValue(command byte) {
	q.sessionVars.SetCommandValue(command)
}

// TODO(eastfisher): remove this function when Driver interface is changed
func (*QueryCtxImpl) SetSessionManager(util.SessionManager) {
	return
}

func (q *QueryCtxImpl) initAttachedConnHolder() {
	connMgr := NewBackendConnManager(getGlobalFSM(), q.ns)
	q.connMgr = connMgr
}

func (q *QueryCtxImpl) isStmtNeedToCheckCircuitBreaking(stmt ast.StmtNode) bool {
	breaker, err := q.ns.GetBreaker()
	if err != nil {
		return false
	}
	if !breaker.IsUseBreaker() {
		return false
	}
	switch stmt.(type) {
	case *ast.SelectStmt:
		return true
	case *ast.InsertStmt:
		return true
	case *ast.UpdateStmt:
		return true
	case *ast.DeleteStmt:
		return true
	default:
		return false
	}
}
