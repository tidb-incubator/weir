package proxy

import (
	"context"

	"github.com/pingcap/parser/mysql"
)

func (cc *clientConn) dispatch(ctx context.Context, data []byte) error {
	cmd := data[0]
	data = data[1:]

	cc.setProcessInfoInDispatch(cmd)
	return cc.dispatchRequest(cmd, data)
}

// TODO: inplement this method
func (cc *clientConn) setProcessInfoInDispatch(cmd byte) {
	switch cmd {
	case mysql.ComPing,
		mysql.ComStmtClose,
		mysql.ComStmtSendLongData,
		mysql.ComStmtReset,
		mysql.ComSetOption,
		mysql.ComChangeUser:
	case mysql.ComInitDB:
	}
}

func (cc *clientConn) dispatchRequest(cmd byte, data []byte) error {
	switch cmd {
	case mysql.ComSleep:
	case mysql.ComQuit:
	case mysql.ComQuery:
	case mysql.ComPing:
	case mysql.ComInitDB:
	case mysql.ComFieldList:
	case mysql.ComStmtPrepare:
	case mysql.ComStmtExecute:
	case mysql.ComStmtFetch:
	case mysql.ComStmtClose:
	case mysql.ComStmtSendLongData:
	case mysql.ComStmtReset:
	case mysql.ComSetOption:
	case mysql.ComChangeUser:
	default:
	}

	panic("unimplemented")
}

func (cc *clientConn) useDB(ctx context.Context, db string) (err error) {
	// if input is "use `SELECT`", mysql client just send "SELECT"
	// so we add `` around db.
	_, err = cc.ctx.Execute(ctx, "use `"+db+"`")
	if err != nil {
		return err
	}
	cc.dbname = db
	return
}
