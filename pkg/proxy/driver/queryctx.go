package driver

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	"github.com/pingcap-incubator/weir/pkg/proxy/server"
	cb "github.com/pingcap-incubator/weir/pkg/util/rate_limit_breaker/circuit_breaker"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
	gomysql "github.com/siddontang/go-mysql/mysql"
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

	connMgr            *BackendConnManager
	firstTableName     string
	currentSqlParadigm uint32
	useBreaker         bool
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
	defer q.reset()
	charsetInfo, collation := q.sessionVars.GetCharsetInfo()
	stmt, err := q.parser.ParseOneStmt(sql, charsetInfo, collation)
	if err != nil {
		return nil, err
	}

	if q.isStmtDenied(ctx, sql, stmt) {
		q.recordDeniedQueryMetrics(stmt)
		return nil, mysql.NewErrf(mysql.ErrUnknown, "statement is denied")
	}

	if err = q.preHandleBreaker(ctx, sql, stmt); err != nil {
		return nil, err
	}

	if q.useBreaker {
		return q.executeWithBreakerInterceptor(ctx, stmt, sql, q.connId)
	} else {
		return q.executeStmt(ctx, sql, stmt)
	}
}

func (q *QueryCtxImpl) preHandleBreaker(ctx context.Context, sql string, stmt ast.StmtNode) error {
	if !isStmtNeedToCheckCircuitBreaking(stmt) {
		return nil
	}

	charsetInfo, collation := q.sessionVars.GetCharsetInfo()
	featureStmt, err := q.parser.ParseOneStmt(sql, charsetInfo, collation)
	if err != nil {
		return err
	}

	visitor, err := extractAstVisit(featureStmt)
	if err != nil {
		return err
	}

	q.useBreaker = true
	q.firstTableName = visitor.TableName()
	q.currentSqlParadigm = crc32.ChecksumIEEE([]byte(visitor.SqlFeature()))
	return nil
}

func (q *QueryCtxImpl) executeWithBreakerInterceptor(ctx context.Context, stmtNode ast.StmtNode, sql string, connectionID uint64) (*gomysql.Result, error) {
	startTime := time.Now()

	breaker, err := q.ns.GetBreaker()
	if err != nil {
		return nil, err
	}

	brName, err := q.getBreakerName(ctx, sql, breaker)
	if err != nil {
		return nil, err
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
	if err := breaker.AddTimeWheelTask(brName, connectionID, &triggerFlag); err != nil {
		return nil, err
	}
	ret, err := q.executeStmt(ctx, sql, stmtNode)
	breaker.RemoveTimeWheelTask(connectionID)

	if triggerFlag == -1 {
		breaker.Hit(brName, -1, false)
	}
	durationMilliSecond := float64(time.Since(startTime)) / float64(time.Second)
	q.recordQueryMetrics(stmtNode, err, durationMilliSecond)
	return ret, err
}

func (q *QueryCtxImpl) reset() {
	q.firstTableName = ""
	q.useBreaker = false
	q.currentSqlParadigm = 0
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

func isStmtNeedToCheckCircuitBreaking(stmt ast.StmtNode) bool {
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

type AstVisitor struct {
	table      string
	sqlFeature string
	found      bool
}

func (f *AstVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch nn := n.(type) {
	case *ast.TableName:
		if f.found {
			return n, false
		}
		f.table = nn.Name.String()
		f.found = true
	case *ast.PatternInExpr:
		if len(nn.List) == 0 {
			return nn, false
		}
		if _, ok := nn.List[0].(*driver.ValueExpr); ok {
			nn.List = nn.List[:1]
		}
	case *driver.ValueExpr:
		nn.SetValue("?")
	}
	return n, false
}

func (f *AstVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, !f.found
}

func (f *AstVisitor) TableName() string {
	return f.table
}

func (f *AstVisitor) SqlFeature() string {
	return f.sqlFeature
}

func extractAstVisit(stmt ast.StmtNode) (*AstVisitor, error) {
	visitor := &AstVisitor{}

	stmt.Accept(visitor)

	sb := strings.Builder{}
	if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return nil, err
	}
	visitor.sqlFeature = sb.String()

	return visitor, nil
}

func UInt322Bytes(n uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, n)
	return b
}

func Bytes2Uint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}
