package proxy

import (
	"context"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/errno"
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
)

// DefaultCapability is the capability of the server when it is created using the default configuration.
// When server is configured with SSL, the server will have extra capabilities compared to DefaultCapability.
const defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts | mysql.ClientPluginAuth | mysql.ClientInteractive

type Server struct {
	listener   net.Listener
	cfg        *config.Config
	baseConnID uint32
	capability uint32
	tlsConfig  unsafe.Pointer // *tls.Config
	driver     IDriver
}

// NewServer creates a new Server.
// TODO: implement this function
func NewServer(cfg *config.Config, driver IDriver) (*Server, error) {
	// TODO: handle the unset fields
	s := &Server{
		cfg:    cfg,
		driver: driver,
		//concurrentLimiter: NewTokenLimiter(cfg.TokenLimit),
		//clients:           make(map[uint32]*clientConn),
	}

	// TODO: set tlsConfig

	setSystemTimeZoneVariable()

	s.initCapability()

	// TODO: implement this function
	if err := s.initListener(); err != nil {
		return nil, err
	}

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

// TODO: implement this function
func (s *Server) initListener() error {
	listener, err := net.Listen("tcp", "0.0.0.0:6000")
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

// TODO: implement this function
func (s *Server) Run() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Err.Error() == "use of closed network connection" {
					return nil
				}
			}
			logutil.BgLogger().Error("accept failed", zap.Error(err))
			return errors.Trace(err)
		}

		clientConn := s.newClientConn(conn)
		go s.onConn(clientConn)
	}
}

func (s *Server) GetNextConnID() uint32 {
	return atomic.AddUint32(&s.baseConnID, 1)
}

// TODO: implement this function
func (s *Server) onConn(clientConn *clientConn) {
	ctx := context.Background()
	if err := clientConn.handshake(ctx); err != nil {
		panic(err)
	}

	// do something before close
	defer func() {

	}()

	clientConn.Run(ctx)
}

func (s *Server) newClientConn(conn net.Conn) *clientConn {
	cc := newClientConn(s)
	cc.setConn(conn)
	cc.salt = fastrand.Buf(20)
	return cc
}

// TODO: implement this function
func (s *Server) checkConnectionCount() error {
	return nil
}

// TODO: implement this function
func (s *Server) isUnixSocket() bool {
	return false
}

// Close closes the server.
// TODO: implement this function
func (s *Server) Close() {
	if s.listener != nil {
		err := s.listener.Close()
		terror.Log(errors.Trace(err))
		s.listener = nil
	}
}
