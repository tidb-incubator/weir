package driver

import (
	"sync/atomic"

	"github.com/pingcap/tidb/sessionctx/variable"
)

type SessionVarsWrapper struct {
	sessionVarSet map[string]struct{}
	sessionVars   *variable.SessionVars
	affectedRows  uint64
}

func NewSessionVarsWrapper(sessionVars *variable.SessionVars) *SessionVarsWrapper {
	return &SessionVarsWrapper{sessionVars: sessionVars}
}

func (s *SessionVarsWrapper) SessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (s *SessionVarsWrapper) GetAllSystemVars() map[string]string {
	ret := make(map[string]string)
	for n := range s.sessionVarSet {
		if v, ok := s.sessionVars.GetSystemVar(n); ok {
			ret[n] = v
		}
	}
	return ret
}

func (s *SessionVarsWrapper) GetSystemVar(name string) (string, bool) {
	if _, ok := s.sessionVarSet[name]; !ok {
		return "", false
	}
	return s.sessionVars.GetSystemVar(name)
}

func (s *SessionVarsWrapper) SetSystemVar(name string, val string) error {
	s.sessionVarSet[name] = struct{}{}
	return s.sessionVars.SetSystemVar(name, val)
}

func (s *SessionVarsWrapper) SetSystemVarDefault(name string) {
	delete(s.sessionVarSet, name)
}

func (s *SessionVarsWrapper) Status() uint16 {
	return s.sessionVars.Status
}

func (s *SessionVarsWrapper) GetStatusFlag(flag uint16) bool {
	return s.sessionVars.GetStatusFlag(flag)
}

func (s *SessionVarsWrapper) SetStatusFlag(flag uint16, on bool) {
	s.sessionVars.SetStatusFlag(flag, on)
}

func (s *SessionVarsWrapper) GetCharsetInfo() (charset, collation string) {
	return s.sessionVars.GetCharsetInfo()
}

func (s *SessionVarsWrapper) AffectedRows() uint64 {
	return s.affectedRows
}

func (s *SessionVarsWrapper) SetAffectRows(count uint64) {
	s.affectedRows = count
}

func (s *SessionVarsWrapper) LastInsertID() uint64 {
	return s.sessionVars.StmtCtx.LastInsertID
}

func (s *SessionVarsWrapper) SetLastInsertID(id uint64) {
	s.sessionVars.StmtCtx.LastInsertID = id
}

func (s *SessionVarsWrapper) GetMessage() string {
	return s.sessionVars.StmtCtx.GetMessage()
}

func (s *SessionVarsWrapper) SetMessage(msg string) {
	s.sessionVars.StmtCtx.SetMessage(msg)
}

func (s *SessionVarsWrapper) GetClientCapability() uint32 {
	return s.sessionVars.ClientCapability
}

func (s *SessionVarsWrapper) SetClientCapability(capability uint32) {
	s.sessionVars.ClientCapability = capability
}

func (s *SessionVarsWrapper) SetCommandValue(command byte) {
	atomic.StoreUint32(&s.sessionVars.CommandValue, uint32(command))
}
