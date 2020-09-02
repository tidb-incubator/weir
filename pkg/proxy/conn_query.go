package proxy

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/hack"
)

func (cc *clientConn) dispatch(ctx context.Context, data []byte) error {
	cmd := data[0]
	data = data[1:]

	cc.setProcessInfoInDispatch(cmd)
	return cc.dispatchRequest(ctx, cmd, data)
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
func (cc *clientConn) dispatchRequest(ctx context.Context, cmd byte, data []byte) error {
	switch cmd {
	case mysql.ComSleep:
		return nil
	case mysql.ComQuit:
		return io.EOF
	case mysql.ComQuery:
		// For issue 1989
		// Input payload may end with byte '\0', we didn't find related mysql document about it, but mysql
		// implementation accept that case. So trim the last '\0' here as if the payload an EOF string.
		// See http://dev.mysql.com/doc/internals/en/com-query.html
		if len(data) > 0 && data[len(data)-1] == 0 {
			data = data[:len(data)-1]
		}
		return cc.handleQuery(ctx, string(hack.String(data)))
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

// handleQuery executes the sql query string and writes result set or result ok to the client.
// As the execution time of this function represents the performance of TiDB, we do time log and metrics here.
// There is a special query `load data` that does not return result, which is handled differently.
// Query `load stats` does not return result either.
// TODO: implement this function
func (cc *clientConn) handleQuery(ctx context.Context, sql string) (err error) {
	rss, err := cc.ctx.Execute(ctx, sql)
	if err != nil {
		metrics.ExecuteErrorCounter.WithLabelValues(metrics.ExecuteErrorToLabel(err)).Inc()
		return err
	}

	status := atomic.LoadInt32(&cc.status)
	if rss != nil && (status == connStatusShutdown || status == connStatusWaitShutdown) {
		for _, rs := range rss {
			terror.Call(rs.Close)
		}
		return executor.ErrQueryInterrupted
	}
	if rss != nil {
		if len(rss) == 1 {
			err = cc.writeResultset(ctx, rss[0], false, 0, 0)
		} else {
			err = cc.writeMultiResultset(ctx, rss, false)
		}
	} else {
		err = cc.writeOK()
	}
	return err
}
