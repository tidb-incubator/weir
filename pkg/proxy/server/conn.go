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
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap-incubator/weir/pkg/proxy/metrics"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     // Closed by server.
	connStatusWaitShutdown // Notified by server to close.
)

const (
	WRITE_TIMEOUT = "write_timeout"

	DEF_WRITE_TIMEOUT = 5
)

var (
	mysqlComMapping = [...]string{
		mysql.ComSleep:            "Sleep",
		mysql.ComQuit:             "Quit",
		mysql.ComInitDB:           "InitDB",
		mysql.ComQuery:            "Query",
		mysql.ComPing:             "Ping",
		mysql.ComFieldList:        "FieldList",
		mysql.ComStmtPrepare:      "StmtPrepare",
		mysql.ComStmtExecute:      "StmtExecute",
		mysql.ComStmtFetch:        "StmtFetch",
		mysql.ComStmtClose:        "StmtClose",
		mysql.ComStmtSendLongData: "StmtSendLongData",
		mysql.ComStmtReset:        "StmtReset",
		mysql.ComSetOption:        "SetOption",
	}
)

const (
	mysqlComOK    = "OK"
	mysqlComError = "Error"
)

// clientConn represents a connection between server and client, it maintains connection specific state,
// handles client query.
type clientConn struct {
	pkt          *packetIO         // a helper to read and write data in packet format.
	bufReadConn  *bufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	tlsConn      *tls.Conn         // TLS connection, nil if not TLS.
	server       *Server           // a reference of server instance.
	capability   uint32            // client capability affects the way server handles client request.
	connectionID uint32            // atomically allocated by a global variable, unique in process scope.
	user         string            // user of the client.
	dbname       string            // default database name.
	alloc        arena.Allocator   // an memory allocator for reducing memory allocation.
	salt         []byte            // random bytes used for authentication.
	lastPacket   []byte            // latest sql query string, currently used for logging error.
	ctx          QueryCtx          // an interface to execute sql statements.
	attrs        map[string]string // attributes parsed from client handshake response, not used for now.
	peerHost     string            // peer host
	peerPort     string            // peer port
	status       int32             // dispatching/reading/shutdown/waitshutdown
	lastCode     uint16            // last error code
	collation    uint8             // collation used by client, may be different from the collation used by database.
}

// newClientConn creates a *clientConn object.
func newClientConn(s *Server) *clientConn {
	return &clientConn{
		server:       s,
		connectionID: atomic.AddUint32(&s.baseConnID, 1),
		collation:    mysql.DefaultCollationID,
		alloc:        arena.NewAllocator(32 * 1024),
		status:       connStatusDispatching,
	}
}

// Run reads client query and writes query result to client in for loop, if there is a panic during query handling,
// it will be recovered and log the panic error.
// This function returns and the connection is closed if there is an IO error or there is a panic.
func (cc *clientConn) Run(ctx context.Context) {
	const size = 4096
	defer func() {
		r := recover()
		if r != nil {
			buf := make([]byte, size)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("connection running loop panic",
				zap.Stringer("lastSQL", getLastStmtInConn{cc}),
				zap.String("err", fmt.Sprintf("%v", r)),
				zap.String("stack", string(buf)),
			)
			err := cc.writeError(errors.New(fmt.Sprintf("%v", r)))
			terror.Log(err)
			metrics.PanicCounter.WithLabelValues(metrics.LabelSession).Inc()
		}
		cc.server.tw.Remove(cc.connectionID)
		if atomic.LoadInt32(&cc.status) != connStatusShutdown {
			err := cc.Close()
			terror.Log(err)
		}
	}()
	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	for {
		if !atomic.CompareAndSwapInt32(&cc.status, connStatusDispatching, connStatusReading) {
			return
		}

		cc.alloc.Reset()

		// close connection when idle time is more than wait_timeout
		cc.setWaitTimeout(ctx)

		data, err := cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				errStack := errors.ErrorStack(err)
				if !strings.Contains(errStack, "use of closed network connection") {
					logutil.Logger(ctx).Warn("read packet failed, close this connection",
						zap.Error(errors.SuspendStack(err)))
				}
			}
			return
		}

		if !atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusDispatching) {
			return
		}

		startTime := time.Now()
		if err = cc.dispatch(ctx, data); err != nil {
			if terror.ErrorEqual(err, io.EOF) {
				cc.addMetrics(data[0], startTime, nil)
				return
			} else if terror.ErrResultUndetermined.Equal(err) {
				logutil.Logger(ctx).Error("result undetermined, close this connection", zap.Error(err))
				return
			} else if terror.ErrCritical.Equal(err) {
				logutil.Logger(ctx).Fatal("critical error, stop the server", zap.Error(err))
			}
			var txnMode string
			if cc.ctx != nil {
				txnMode = cc.ctx.GetSessionVars().GetReadableTxnMode()
			}
			logutil.Logger(ctx).Error("command dispatched failed",
				zap.String("connInfo", cc.String()),
				zap.String("command", mysql.Command2Str[data[0]]),
				zap.String("status", cc.SessionStatusToString()),
				zap.Stringer("sql", getLastStmtInConn{cc}),
				zap.String("txn_mode", txnMode),
				zap.String("err", errStrForLog(err)),
			)
			err1 := cc.writeError(err)
			terror.Log(err1)
		}
		cc.addMetrics(data[0], startTime, err)
		cc.pkt.sequence = 0
	}
}

func (cc *clientConn) setConn(conn net.Conn) {
	cc.bufReadConn = newBufferedReadConn(conn)
	if cc.pkt == nil {
		cc.pkt = newPacketIO(cc.bufReadConn)
	} else {
		// Preserve current sequence number.
		cc.pkt.setBufferedReadConn(cc.bufReadConn)
	}
}

// getSessionVarsWaitTimeout get session variable wait_timeout
func (cc *clientConn) getSessionVarsWaitTimeout(ctx context.Context) uint64 {
	valStr, exists := cc.ctx.GetSessionVars().GetSystemVar(variable.WaitTimeout)
	if !exists {
		return variable.DefWaitTimeout
	}
	waitTimeout, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		logutil.Logger(ctx).Warn("get sysval wait_timeout failed, use default value", zap.Error(err))
		// if get waitTimeout error, use default value
		return variable.DefWaitTimeout
	}
	return waitTimeout
}

func (cc *clientConn) SessionStatusToString() string {
	status := cc.ctx.Status()
	inTxn, autoCommit := 0, 0
	if status&mysql.ServerStatusInTrans > 0 {
		inTxn = 1
	}
	if status&mysql.ServerStatusAutocommit > 0 {
		autoCommit = 1
	}
	return fmt.Sprintf("inTxn:%d, autocommit:%d",
		inTxn, autoCommit,
	)
}

func (cc *clientConn) killWaitTimeOutConn(ctx context.Context, connectionID uint32, t time.Time) {
	cc.server.KillOneConnections(connectionID)
	elapsed := time.Since(t)
	logutil.Logger(ctx).Info("wait timeout, close this connection",
		zap.Duration("idle", elapsed),
	)
}

func (cc *clientConn) setWaitTimeout(ctx context.Context) {
	var wt time.Duration
	waitTimeout := cc.getSessionVarsWaitTimeout(ctx)
	wt = time.Duration(cc.server.sessionTimeout) * time.Second
	if waitTimeout != 0 {
		wt = time.Duration(waitTimeout) * time.Second
	}
	t := time.Now()
	cc.server.tw.Add(wt, cc.connectionID, func() { cc.killWaitTimeOutConn(ctx, cc.connectionID, t) })
}

func (cc *clientConn) Close() error {
	cc.server.rwlock.Lock()
	delete(cc.server.clients, cc.connectionID)
	connections := len(cc.server.clients)
	cc.server.rwlock.Unlock()
	return closeConn(cc, connections)
}

func closeConn(cc *clientConn, connections int) error {
	metrics.ConnGauge.WithLabelValues().Set(float64(connections))
	err := cc.bufReadConn.Close()
	terror.Log(err)
	if cc.ctx != nil {
		return cc.ctx.Close()
	}
	return nil
}

func (cc *clientConn) closeWithoutLock() error {
	delete(cc.server.clients, cc.connectionID)
	return closeConn(cc, len(cc.server.clients))
}

// ShutdownOrNotify will Shutdown this client connection, or do its best to notify.
func (cc *clientConn) ShutdownOrNotify() bool {
	if (cc.ctx.Status() & mysql.ServerStatusInTrans) > 0 {
		return false
	}
	// If the client connection status is reading, it's safe to shutdown it.
	if atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusShutdown) {
		return true
	}
	// If the client connection status is dispatching, we can't shutdown it immediately,
	// so set the status to WaitShutdown as a notification, the client will detect it
	// and then exit.
	atomic.StoreInt32(&cc.status, connStatusWaitShutdown)
	return false
}

func (cc *clientConn) String() string {
	collationStr := mysql.Collations[cc.collation]
	return fmt.Sprintf("id:%d, addr:%s status:%b, collation:%s, user:%s",
		cc.connectionID, cc.bufReadConn.RemoteAddr(), cc.ctx.Status(), collationStr, cc.user,
	)
}

// TODO(eastfisher): implement this function
func (cc *clientConn) addMetrics(cmd byte, startTime time.Time, err error) {
	getQueryTotalCounter(cmd, err).Inc()
}

func getQueryTotalCounter(cmd byte, err error) prometheus.Counter {
	var comTypeStr string
	if int(cmd) < len(mysqlComMapping) {
		comTypeStr = mysqlComMapping[cmd]
	}
	if comTypeStr == "" {
		comTypeStr = strconv.Itoa(int(cmd))
	}
	var comRespStr string = mysqlComOK
	if err != nil {
		comRespStr = mysqlComError
	}
	return metrics.QueryTotalCounter.WithLabelValues(comTypeStr, comRespStr)
}
