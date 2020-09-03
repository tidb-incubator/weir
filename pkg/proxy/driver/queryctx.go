package driver

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pingcap-incubator/weir/pkg/proxy/server"
	"github.com/pingcap-incubator/weir/pkg/util/passwd"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
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
	currentDB string
	backend   Backend
}

func NewQueryCtxImpl(backend Backend) *QueryCtxImpl {
	return &QueryCtxImpl{backend: backend}
}

func (*QueryCtxImpl) Status() uint16 {
	return ServerStatusAutocommit
}

func (*QueryCtxImpl) LastInsertID() uint64 {
	return 0
}

func (*QueryCtxImpl) LastMessage() string {
	return "test last message"
}

func (*QueryCtxImpl) AffectedRows() uint64 {
	return 1
}

func (*QueryCtxImpl) Value(key fmt.Stringer) interface{} {
	return nil
}

func (*QueryCtxImpl) SetValue(key fmt.Stringer, value interface{}) {
	return
}

func (*QueryCtxImpl) SetProcessInfo(sql string, t time.Time, command byte, maxExecutionTime uint64) {
	return
}

func (*QueryCtxImpl) CommitTxn(ctx context.Context) error {
	return nil
}

func (*QueryCtxImpl) RollbackTxn() {
	return
}

func (*QueryCtxImpl) WarningCount() uint16 {
	return 0
}

func (q *QueryCtxImpl) CurrentDB() string {
	return q.currentDB
}

func (q *QueryCtxImpl) Execute(ctx context.Context, sql string) ([]server.ResultSet, error) {
	return q.doExecute(ctx, sql)
}

func (*QueryCtxImpl) ExecuteInternal(ctx context.Context, sql string) ([]server.ResultSet, error) {
	return nil, nil
}

func (*QueryCtxImpl) SetClientCapability(uint32) {
	return
}

func (*QueryCtxImpl) Prepare(sql string) (statement server.PreparedStatement, columns, params []*server.ColumnInfo, err error) {
	return nil, nil, nil, fmt.Errorf("prepare is unimplemented")
}

func (*QueryCtxImpl) GetStatement(stmtID int) server.PreparedStatement {
	return nil
}

func (*QueryCtxImpl) FieldList(tableName string) (columns []*server.ColumnInfo, err error) {
	return nil, nil
}

func (*QueryCtxImpl) Close() error {
	return nil
}

// TODO: implement multi tenant auth
func (*QueryCtxImpl) Auth(user *auth.UserIdentity, pwd []byte, salt []byte) bool {
	originPwdStr := []byte("world")
	originPwd := passwd.CalculatePassword(salt, originPwdStr)
	return bytes.Equal(originPwd, pwd)
}

func (*QueryCtxImpl) ShowProcess() *util.ProcessInfo {
	return nil
}

func (*QueryCtxImpl) GetSessionVars() *variable.SessionVars {
	return nil
}

func (*QueryCtxImpl) SetCommandValue(command byte) {
	return
}

func (*QueryCtxImpl) SetSessionManager(util.SessionManager) {
	return
}
