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
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     // Closed by server.
	connStatusWaitShutdown // Notified by server to close.
)

type clientConn struct {
	// server句柄
	server *Server

	// 网络连接与协议编解码
	bufReadConn *bufferedReadConn
	tlsConn     *tls.Conn
	pkt         *packetIO
	capability  uint32 // client capability affects the way server handles client request.
	user        string // user of the client.
	dbname      string // default database name.
	peerHost    string // peer host
	peerPort    string // peer port

	// 基本信息
	connectionID uint32            // atomically allocated by a global variable, unique in process scope.
	salt         []byte            // random bytes used for authentication.
	collation    uint8             // collation used by client, may be different from the collation used by database.
	attrs        map[string]string // attributes parsed from client handshake response, not used for now.

	// SQL查询
	ctx        QueryCtx        // an interface to execute sql statements.
	status     int32           // dispatching/reading/shutdown/waitshutdown
	alloc      arena.Allocator // an memory allocator for reducing memory allocation.
	lastCode   uint16          // last error code
	lastPacket []byte          // latest sql query string, currently used for logging error.
}

func newClientConn(s *Server) *clientConn {
	return &clientConn{
		server:       s,
		connectionID: s.GetNextConnID(),
		collation:    mysql.DefaultCollationID,
		alloc:        arena.NewAllocator(32 * 1024),
		status:       connStatusDispatching,
	}
}

// TODO: implement this function
func (cc *clientConn) Run(ctx context.Context) {
	const size = 4096
	defer func() {
		r := recover()
		if r != nil {
			buf := make([]byte, size)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("connection running loop panic",
				//zap.Stringer("lastSQL", getLastStmtInConn{cc}),
				zap.String("err", fmt.Sprintf("%v", r)),
				zap.String("stack", string(buf)),
			)
			err := cc.writeError(errors.New(fmt.Sprintf("%v", r)))
			terror.Log(err)
			metrics.PanicCounter.WithLabelValues(metrics.LabelSession).Inc()
		}
		if atomic.LoadInt32(&cc.status) != connStatusShutdown {
			err := cc.Close()
			terror.Log(err)
		}
	}()

	// Loop handle incoming data
	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	for {
		// check status, if current status is not dispatching, return
		if !atomic.CompareAndSwapInt32(&cc.status, connStatusDispatching, connStatusReading) {
			return
		}

		cc.alloc.Reset()

		waitTimeout := cc.getSessionVarsWaitTimeout(ctx)
		cc.pkt.setReadTimeout(time.Duration(waitTimeout) * time.Second)

		start := time.Now()
		data, err := cc.readPacket()
		if err != nil {
			logReadPacketErrorInfo(ctx, err, start, waitTimeout)
			return
		}

		// check status, if current status is not reading, return
		if !atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusDispatching) {
			return
		}

		startTime := time.Now()
		if err = cc.dispatch(ctx, data); err != nil {
			cc.logDispatchErrorInfo()
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

// TODO: implemented this function
func (cc *clientConn) getSessionVarsWaitTimeout(ctx context.Context) uint64 {
	return variable.DefWaitTimeout
}

func (cc *clientConn) Close() error {
	err := cc.bufReadConn.Close()
	terror.Log(err)
	if cc.ctx != nil {
		return cc.ctx.Close()
	}
	return nil
}
