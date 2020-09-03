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

package proxy

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
