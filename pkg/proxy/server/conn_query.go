// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"io"
	"strings"
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
	dataStr := string(hack.String(data))
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
			dataStr = string(hack.String(data))
		}
		return cc.handleQuery(ctx, dataStr)
	case mysql.ComPing:
		return cc.writeOK()
	case mysql.ComInitDB:
		if err := cc.useDB(ctx, dataStr); err != nil {
			return err
		}
		return cc.writeOK()
	case mysql.ComFieldList:
		return cc.handleFieldList(dataStr)
	case mysql.ComStmtPrepare:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	case mysql.ComStmtExecute:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	case mysql.ComStmtFetch:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	case mysql.ComStmtClose:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	case mysql.ComStmtSendLongData:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	case mysql.ComStmtReset:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	case mysql.ComSetOption:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	case mysql.ComChangeUser:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	default:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	}
}

// useDB only save db name in clientConn,
// but run "use `db`" when execute query in backend.
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

// handleFieldList returns the field list for a table.
// The sql string is composed of a table name and a terminating character \x00.
func (cc *clientConn) handleFieldList(sql string) (err error) {
	parts := strings.Split(sql, "\x00")
	columns, err := cc.ctx.FieldList(parts[0])
	if err != nil {
		return err
	}
	data := cc.alloc.AllocWithLen(4, 1024)
	for _, column := range columns {
		// Current we doesn't output defaultValue but reserve defaultValue length byte to make mariadb client happy.
		// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
		// TODO: fill the right DefaultValues.
		column.DefaultValueLength = 0
		column.DefaultValue = []byte{}

		data = data[0:4]
		data = column.Dump(data)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	if err := cc.writeEOF(0); err != nil {
		return err
	}
	return cc.flush()
}
