package server

import (
	"context"

	"github.com/pingcap/errors"
)

// TODO(eastfisher): fix me when prepare is implemented
func (cc *clientConn) preparedStmt2String(stmtID uint32) string {
	return ""
}

// TODO(eastfisher): fix me when prepare is implemented
func (cc *clientConn) preparedStmt2StringNoArgs(stmtID uint32) string {
	return ""
}

func (cc *clientConn) handleStmtPrepare(sql string) error {
	stmtId, columns, params, err := cc.ctx.Prepare(sql)
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
	return errors.New("stmt not implemented")
}

func (cc *clientConn) handleStmtSendLongData(data []byte) error {
	return errors.New("stmt not implemented")
}

func (cc *clientConn) handleStmtReset(data []byte) error {
	return errors.New("stmt not implemented")
}

func (cc *clientConn) handleStmtClose(data []byte) error {
	return errors.New("stmt not implemented")
}
