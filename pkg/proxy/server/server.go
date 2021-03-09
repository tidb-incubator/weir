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
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/fastrand"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	errUnknownFieldType        = terror.ClassServer.New(errno.ErrUnknownFieldType, errno.MySQLErrName[errno.ErrUnknownFieldType])
	errInvalidSequence         = terror.ClassServer.New(errno.ErrInvalidSequence, errno.MySQLErrName[errno.ErrInvalidSequence])
	errInvalidType             = terror.ClassServer.New(errno.ErrInvalidType, errno.MySQLErrName[errno.ErrInvalidType])
	errNotAllowedCommand       = terror.ClassServer.New(errno.ErrNotAllowedCommand, errno.MySQLErrName[errno.ErrNotAllowedCommand])
	errAccessDenied            = terror.ClassServer.New(errno.ErrAccessDenied, errno.MySQLErrName[errno.ErrAccessDenied])
	errConCount                = terror.ClassServer.New(errno.ErrConCount, errno.MySQLErrName[errno.ErrConCount])
	errSecureTransportRequired = terror.ClassServer.New(errno.ErrSecureTransportRequired, errno.MySQLErrName[errno.ErrSecureTransportRequired])

	timeWheelUnit       = time.Second * 1
	timeWheelBucketsNum = 3600
)

// DefaultCapability is the capability of the server when it is created using the default configuration.
// When server is configured with SSL, the server will have extra capabilities compared to DefaultCapability.
const defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts | mysql.ClientPluginAuth | mysql.ClientInteractive

type Server struct {
	cfg            *config.Proxy
	tlsConfig      unsafe.Pointer // *tls.Config
	driver         IDriver
	listener       net.Listener
	rwlock         sync.RWMutex
	clients        map[uint32]*clientConn
	baseConnID     uint32
	capability     uint32
	sessionTimeout time.Duration
	tw             *TimeWheel
}

// NewServer creates a new Server.
func NewServer(cfg *config.Proxy, driver IDriver) (*Server, error) {
	// Init time wheel
	tw, err := NewTimeWheel(timeWheelUnit, timeWheelBucketsNum)
	if err != nil {
		return nil, err
	}
	// start time wheel
	tw.Start()

	// init Server
	s := &Server{
		cfg:     cfg,
		driver:  driver,
		clients: make(map[uint32]*clientConn),
		tw:      tw,
	}

	st := strconv.Itoa(cfg.ProxyServer.SessionTimeout)
	st = st + "s"
	s.sessionTimeout, err = time.ParseDuration(st)
	if err != nil {
		return nil, err
	}

	// TODO(eastfisher): set tlsConfig

	setSystemTimeZoneVariable()

	s.initCapability()

	if err := s.initListener(); err != nil {
		return nil, err
	}

	// TODO(eastfisher): init status http server

	// Init rand seed for randomBuf()
	rand.Seed(time.Now().UTC().UnixNano())

	return s, nil
}

func (s *Server) initCapability() {
	s.capability = defaultCapability
	if s.tlsConfig != nil {
		s.capability |= mysql.ClientSSL
	}
}

// TODO(eastfisher): support unix socket and proxyprotocol
func (s *Server) initListener() error {
	listener, err := net.Listen("tcp", s.cfg.ProxyServer.Addr)
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

func (s *Server) Run() error {
	metrics.ServerEventCounter.WithLabelValues(metrics.EventStart).Inc()

	// TODO(eastfisher): startStatusHTTP()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Err.Error() == "use of closed network connection" {
					return nil
				}
			}

			// TODO(eastfisher): support PROXY protocol

			logutil.BgLogger().Error("accept failed", zap.Error(err))
			return errors.Trace(err)
		}

		clientConn := s.newConn(conn)
		go s.onConn(clientConn)
	}
}

// ConnectionCount gets current connection count.
func (s *Server) ConnectionCount() int {
	s.rwlock.RLock()
	cnt := len(s.clients)
	s.rwlock.RUnlock()
	return cnt
}

func (s *Server) onConn(conn *clientConn) {
	ctx := logutil.WithConnID(context.Background(), conn.connectionID)
	if err := conn.handshake(ctx); err != nil {
		// Some keep alive services will send request to TiDB and disconnect immediately.
		// So we only record metrics.
		metrics.HandShakeErrorCounter.Inc()
		err = conn.Close()
		terror.Log(errors.Trace(err))
		return
	}

	logutil.Logger(ctx).Info("new connection", zap.String("remoteAddr", conn.bufReadConn.RemoteAddr().String()))

	defer func() {
		logutil.Logger(ctx).Info("connection closed")
	}()

	s.rwlock.Lock()
	s.clients[conn.connectionID] = conn
	connections := len(s.clients)
	s.rwlock.Unlock()
	metrics.ConnGauge.Set(float64(connections))

	s.tw.Add(s.sessionTimeout, conn, func() { s.KillOneConnections(conn.connectionID) })

	conn.Run(ctx)
}

func (s *Server) newConn(conn net.Conn) *clientConn {
	cc := newClientConn(s)
	if s.cfg.Performance.TCPKeepAlive {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				logutil.BgLogger().Error("failed to set tcp keep alive option", zap.Error(err))
			}
		}
	}
	cc.setConn(conn)
	cc.salt = fastrand.Buf(20)
	return cc
}

func (s *Server) checkConnectionCount() error {
	// When the value of MaxConnections is 0, the number of connections is unlimited.
	if int(s.cfg.ProxyServer.MaxConnections) == 0 {
		return nil
	}

	s.rwlock.RLock()
	conns := len(s.clients)
	s.rwlock.RUnlock()

	if conns >= int(s.cfg.ProxyServer.MaxConnections) {
		logutil.BgLogger().Error("too many connections",
			zap.Uint32("max connections", s.cfg.ProxyServer.MaxConnections), zap.Error(errConCount))
		return errConCount
	}
	return nil
}

// TODO(eastfisher): implement this function
func (s *Server) isUnixSocket() bool {
	return false
}

// Close closes the server.
// TODO(eastfisher): implement this function, close unix socket, status server, and gRPC server.
func (s *Server) Close() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	if s.listener != nil {
		err := s.listener.Close()
		terror.Log(errors.Trace(err))
		s.listener = nil
	}
	metrics.ServerEventCounter.WithLabelValues(metrics.EventClose).Inc()
}

func killConn(conn *clientConn) {
	sessVars := conn.ctx.GetSessionVars()
	atomic.StoreUint32(&sessVars.Killed, 1)
}

// KillAllConnections kills all connections when server is not gracefully shutdown.
func (s *Server) KillAllConnections() {
	logutil.BgLogger().Info("[server] kill all connections.")

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	for _, conn := range s.clients {
		atomic.StoreInt32(&conn.status, connStatusShutdown)
		if err := conn.closeWithoutLock(); err != nil {
			terror.Log(err)
		}
		killConn(conn)
	}
}

// KillOneConnections kills one connections when server is not gracefully shutdown.
func (s *Server) KillOneConnections(connectionID uint32) {
	logutil.BgLogger().Info("[server] kill one connections.")

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	if conn, ok := s.clients[connectionID]; ok {
		atomic.StoreInt32(&conn.status, connStatusShutdown)
		if err := conn.closeWithoutLock(); err != nil {
			terror.Log(err)
		}
		killConn(conn)
	}
}

var gracefulCloseConnectionsTimeout = 15 * time.Second

// TryGracefulDown will try to gracefully close all connection first with timeout. if timeout, will close all connection directly.
func (s *Server) TryGracefulDown() {
	ctx, cancel := context.WithTimeout(context.Background(), gracefulCloseConnectionsTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		s.GracefulDown(ctx, done)
	}()
	select {
	case <-ctx.Done():
		s.KillAllConnections()
	case <-done:
		return
	}
}

// GracefulDown waits all clients to close.
func (s *Server) GracefulDown(ctx context.Context, done chan struct{}) {
	logutil.Logger(ctx).Info("[server] graceful shutdown.")
	metrics.ServerEventCounter.WithLabelValues(metrics.EventGracefulDown).Inc()

	count := s.ConnectionCount()
	for i := 0; count > 0; i++ {
		s.kickIdleConnection()

		count = s.ConnectionCount()
		if count == 0 {
			break
		}
		// Print information for every 30s.
		if i%30 == 0 {
			logutil.Logger(ctx).Info("graceful shutdown...", zap.Int("conn count", count))
		}
		ticker := time.After(time.Second)
		select {
		case <-ctx.Done():
			return
		case <-ticker:
		}
	}
	close(done)
}

func (s *Server) kickIdleConnection() {
	var conns []*clientConn
	s.rwlock.RLock()
	for _, cc := range s.clients {
		if cc.ShutdownOrNotify() {
			// Shutdowned conn will be closed by us, and notified conn will exist themselves.
			conns = append(conns, cc)
		}
	}
	s.rwlock.RUnlock()

	for _, cc := range conns {
		err := cc.Close()
		if err != nil {
			logutil.BgLogger().Error("close connection", zap.Error(err))
		}
	}
}
