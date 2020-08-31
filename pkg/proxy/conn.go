package server

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/arena"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown      // Closed by server.
	connStatusWaitShutdown  // Notified by server to close.
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
	ctx      QueryCtx        // an interface to execute sql statements.
	status   int32           // dispatching/reading/shutdown/waitshutdown
	alloc    arena.Allocator // an memory allocator for reducing memory allocation.
	lastCode uint16          // last error code
}

func newClientConn(s *Server) *clientConn {
	return &clientConn{
		server:       s,
		connectionID: s.GetNextConnID(),
	}
}

// TODO: implement this function
func (cc *clientConn) Run(ctx context.Context) {
	// do something before client conn exit
	defer func() {

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
