package server

import (
	"context"
	"io"
	"net"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func logReadPacketErrorInfo(ctx context.Context, err error, start time.Time, waitTimeout uint64) {
	if terror.ErrorNotEqual(err, io.EOF) {
		if netErr, isNetErr := errors.Cause(err).(net.Error); isNetErr && netErr.Timeout() {
			idleTime := time.Since(start)
			logutil.Logger(ctx).Info("read packet timeout, close this connection",
				zap.Duration("idle", idleTime),
				zap.Uint64("waitTimeout", waitTimeout),
				zap.Error(err),
			)
		} else {
			errStack := errors.ErrorStack(err)
			if !strings.Contains(errStack, "use of closed network connection") {
				logutil.Logger(ctx).Warn("read packet failed, close this connection",
					zap.Error(errors.SuspendStack(err)))
			}
		}
	}
}

// TODO: implement this function
func (cc *clientConn) logDispatchErrorInfo() {

}

// TODO: implement this function
func (cc *clientConn) addMetrics(cmd byte, startTime time.Time, err error) {

}
