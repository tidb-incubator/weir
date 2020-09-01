package proxy

import (
	"context"
	"io"

	"github.com/pingcap/parser/mysql"
)

func (cc *clientConn) dispatch(ctx context.Context, data []byte) error {
	cmd := data[0]
	data = data[1:]

	cc.setProcessInfoInDispatch(cmd)
	return cc.dispatchRequest(cmd, data)
}

// TODO: implement this function
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

// TODO: implement this function
func (cc *clientConn) dispatchRequest(cmd byte, data []byte) error {
	switch cmd {
	case mysql.ComSleep:
		return nil
	case mysql.ComQuit:
		return io.EOF
	case mysql.ComQuery:
		return cc.writeOK()
	case mysql.ComPing:
		return cc.writeOK()
	case mysql.ComInitDB:
		return cc.writeOK()
	case mysql.ComFieldList:
		return cc.writeOK()
	case mysql.ComStmtPrepare:
		return cc.writeOK()
	case mysql.ComStmtExecute:
		return cc.writeOK()
	case mysql.ComStmtFetch:
		return cc.writeOK()
	case mysql.ComStmtClose:
		return cc.writeOK()
	case mysql.ComStmtSendLongData:
		return cc.writeOK()
	case mysql.ComStmtReset:
		return cc.writeOK()
	case mysql.ComSetOption:
		return cc.writeOK()
	case mysql.ComChangeUser:
		return cc.writeOK()
	default:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	}
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
