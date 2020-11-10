package server

import (
	"context"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
)

// TODO(eastfisher): fix me when prepare is implemented
func (cc *clientConn) preparedStmt2String(stmtID uint32) string {
	return ""
}

// TODO(eastfisher): fix me when prepare is implemented
func (cc *clientConn) preparedStmt2StringNoArgs(stmtID uint32) string {
	return ""
}

func (cc *clientConn) handleStmtPrepare(ctx context.Context, sql string) error {
	stmtId, columns, params, err := cc.ctx.Prepare(ctx, sql)
	if err != nil {
		return err
	}
	data := make([]byte, 4, 128)

	//status ok
	data = append(data, 0)
	//stmt id
	data = dumpUint32(data, uint32(stmtId))
	//number columns
	data = dumpUint16(data, uint16(len(columns)))
	//number params
	data = dumpUint16(data, uint16(len(params)))
	//filter [00]
	data = append(data, 0)
	//warning count
	data = append(data, 0, 0) //TODO support warning count

	if err := cc.writePacket(data); err != nil {
		return err
	}

	if len(params) > 0 {
		for i := 0; i < len(params); i++ {
			data = data[0:4]
			data = params[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if err := cc.writeEOF(0); err != nil {
			return err
		}
	}

	if len(columns) > 0 {
		for i := 0; i < len(columns); i++ {
			data = data[0:4]
			data = columns[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if err := cc.writeEOF(0); err != nil {
			return err
		}

	}
	return cc.flush()
}

func (cc *clientConn) handleStmtExecute(ctx context.Context, data []byte) error {
	if len(data) < 9 {
		return mysql.ErrMalformPacket
	}

	stmtID := binary.LittleEndian.Uint32(data[0:4])
	ret, err := cc.ctx.StmtExecuteForward(ctx, int(stmtID), data)
	if err != nil {
		return err
	}

	if ret != nil {
		err = cc.writeGoMySQLResultset(ctx, ret.Resultset, true, ret.Status, 0)
	} else {
		err = cc.writeOK()
	}
	return err
}

// TODO(eastfisher): implement this function
func (cc *clientConn) handleStmtSendLongData(data []byte) error {
	return errors.New("stmt not implemented")
}

// TODO(eastfisher): implement this function
func (cc *clientConn) handleStmtReset(data []byte) error {
	return errors.New("stmt not implemented")
}

func (cc *clientConn) handleStmtClose(ctx context.Context, data []byte) error {
	if len(data) < 4 {
		return nil
	}

	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))
	if err := cc.ctx.StmtClose(ctx, stmtID); err != nil {
		return err
	}

	return cc.writeOK()
}
