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
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/hack"
)

var _ fmt.Stringer = getLastStmtInConn{}

type getLastStmtInConn struct {
	*clientConn
}

func (cc getLastStmtInConn) String() string {
	if len(cc.lastPacket) == 0 {
		return ""
	}
	cmd, data := cc.lastPacket[0], cc.lastPacket[1:]
	switch cmd {
	case mysql.ComInitDB:
		return "Use " + string(data)
	case mysql.ComFieldList:
		return "ListFields " + string(data)
	case mysql.ComQuery, mysql.ComStmtPrepare:
		sql := string(hack.String(data))
		if cc.ctx.GetSessionVars().EnableLogDesensitization {
			sql, _ = parser.NormalizeDigest(sql)
		}
		return queryStrForLog(sql)
	case mysql.ComStmtExecute, mysql.ComStmtFetch:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return queryStrForLog(cc.preparedStmt2String(stmtID))
	case mysql.ComStmtClose, mysql.ComStmtReset:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return mysql.Command2Str[cmd] + " " + strconv.Itoa(int(stmtID))
	default:
		if cmdStr, ok := mysql.Command2Str[cmd]; ok {
			return cmdStr
		}
		return string(hack.String(data))
	}
}

// PProfLabel return sql label used to tag pprof.
func (cc getLastStmtInConn) PProfLabel() string {
	if len(cc.lastPacket) == 0 {
		return ""
	}
	cmd, data := cc.lastPacket[0], cc.lastPacket[1:]
	switch cmd {
	case mysql.ComInitDB:
		return "UseDB"
	case mysql.ComFieldList:
		return "ListFields"
	case mysql.ComStmtClose:
		return "CloseStmt"
	case mysql.ComStmtReset:
		return "ResetStmt"
	case mysql.ComQuery, mysql.ComStmtPrepare:
		return parser.Normalize(queryStrForLog(string(hack.String(data))))
	case mysql.ComStmtExecute, mysql.ComStmtFetch:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return queryStrForLog(cc.preparedStmt2StringNoArgs(stmtID))
	default:
		return ""
	}
}

func queryStrForLog(query string) string {
	const size = 4096
	if len(query) > size {
		return query[:size] + fmt.Sprintf("(len: %d)", len(query))
	}
	return query
}

func errStrForLog(err error) string {
	if kv.ErrKeyExists.Equal(err) || parser.ErrParse.Equal(err) {
		// Do not log stack for duplicated entry error.
		return err.Error()
	}
	return errors.ErrorStack(err)
}
