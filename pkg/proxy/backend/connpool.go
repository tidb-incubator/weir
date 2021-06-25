package backend

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tidb-incubator/weir/pkg/proxy/backend/client"
	"github.com/tidb-incubator/weir/pkg/proxy/constant"
	"github.com/tidb-incubator/weir/pkg/proxy/driver"
	"github.com/tidb-incubator/weir/pkg/proxy/metrics"
	"github.com/tidb-incubator/weir/pkg/util/pool"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type ConnPoolConfig struct {
	Config
	Capacity    int
	IdleTimeout time.Duration
}

type Config struct {
	Addr     string
	UserName string
	Password string
}

type ConnPool struct {
	ns   string
	cfg  *ConnPoolConfig
	pool *pool.ResourcePool
}

type backendPooledConnWrapper struct {
	*client.Conn
	ns       string
	addr     string
	username string
	pool     *pool.ResourcePool
	sysvars  map[string]*ast.VariableAssignment
}

// this struct is only used for fitting pool.Resource interface
type noErrorCloseConnWrapper struct {
	*backendPooledConnWrapper
}

func NewConnPool(ns string, cfg *ConnPoolConfig) *ConnPool {
	return &ConnPool{
		ns:  ns,
		cfg: cfg,
	}
}

func newConnWrapper(pool *pool.ResourcePool, conn *client.Conn, ns, addr, username string) *backendPooledConnWrapper {
	return &backendPooledConnWrapper{
		Conn:     conn,
		ns:       ns,
		addr:     addr,
		username: username,
		pool:     pool,
		sysvars:  make(map[string]*ast.VariableAssignment),
	}
}

func (c *ConnPool) Init() error {
	connFactory := func(context.Context) (pool.Resource, error) {
		// TODO: add connect timeout
		conn, err := client.Connect(c.cfg.Addr, c.cfg.UserName, c.cfg.Password, "")
		if err != nil {
			return nil, err
		}
		return &noErrorCloseConnWrapper{newConnWrapper(c.pool, conn, c.ns, c.cfg.Addr, c.cfg.UserName)}, nil
	}

	c.pool = pool.NewResourcePool(connFactory, c.cfg.Capacity, c.cfg.Capacity, c.cfg.IdleTimeout, 0, nil)
	return nil
}

func (c *ConnPool) GetConn(ctx context.Context) (driver.PooledBackendConn, error) {
	rs, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	recordCurrentBackendMetrics(c.ns, c.cfg.Addr, c.pool)

	conn := rs.(*noErrorCloseConnWrapper).backendPooledConnWrapper
	if err := conn.syncSessionVariables(ctx); err != nil {
		return nil, errors.WithMessage(err, "sync sysvar error")
	}

	return conn, nil
}

func (c *ConnPool) Close() error {
	c.pool.Close()
	return nil
}

func recordCurrentBackendMetrics(ns, addr string, resourcePool *pool.ResourcePool) {
	metrics.BackendConnInUseGauge.WithLabelValues(ns, addr).Set(float64(resourcePool.InUse()))
}

func (cw *backendPooledConnWrapper) PutBack() {
	w := &noErrorCloseConnWrapper{cw}
	cw.pool.Put(w)
	recordCurrentBackendMetrics(cw.ns, cw.addr, cw.pool)
}

func (cw *backendPooledConnWrapper) ErrorClose() error {
	cw.pool.Put(nil)
	recordCurrentBackendMetrics(cw.ns, cw.addr, cw.pool)
	if err := cw.Conn.Close(); err != nil {
		return errors.WithMessage(err, fmt.Sprintf("close backend conn error, addr: %s, username: %s", cw.addr, cw.username))
	}
	return nil
}

func (cw *backendPooledConnWrapper) Close() error {
	return cw.Conn.Close()
}

func (cw *backendPooledConnWrapper) syncSessionVariables(ctx context.Context) error {
	sysVars := getSysVarsFromCtx(ctx)
	varsToSet, varsToRemove := getDiffVariableList(sysVars, cw.sysvars)
	if len(varsToSet) == 0 && len(varsToRemove) == 0 {
		return nil
	}

	setSQL, err := getSetSysVarsSQL(varsToSet, varsToRemove)
	logutil.BgLogger().Debug("backend conn set sysvar sql", zap.String("sql", setSQL), zap.Error(err))
	if err != nil {
		return errors.WithMessage(err, "get set sysvar sql error")
	}

	_, err = cw.Execute(setSQL)
	if err != nil {
		logutil.BgLogger().Error("execute sysvar sql error", zap.Error(err), zap.String("sql", setSQL))
		return errors.WithMessage(err, "set sysvar error")
	}

	cw.sysvars = sysVars
	return nil
}

func (cw *noErrorCloseConnWrapper) Close() {
	if err := cw.backendPooledConnWrapper.Close(); err != nil {
		// TODO: log namespace info
		logutil.BgLogger().Error("close backend conn error", zap.String("addr", cw.addr),
			zap.String("username", cw.username), zap.Error(err))
	}
}

var noValueSysVars = map[string]*ast.VariableAssignment{}

const RestoreSetVariableFlags = format.RestoreStringSingleQuotes

func getSysVarsFromCtx(ctx context.Context) map[string]*ast.VariableAssignment {
	v := ctx.Value(constant.ContextKeySessionVariable)
	if v == nil {
		return noValueSysVars
	}
	return v.(map[string]*ast.VariableAssignment)
}

func getSetSysVarsSQL(toSet, toRemove []*ast.VariableAssignment) (string, error) {
	stmt := &ast.SetStmt{}
	for _, v := range toSet {
		stmt.Variables = append(stmt.Variables, v)
	}

	for _, v := range toRemove {
		defaultVar := &ast.VariableAssignment{
			Name:     v.Name,
			Value:    &ast.DefaultExpr{},
			IsGlobal: false,
			IsSystem: getIsSystemFlagByName(v.Name),
		}
		stmt.Variables = append(stmt.Variables, defaultVar)
	}

	return getRestoreSQLFromStmt(stmt)
}

func getIsSystemFlagByName(name string) bool {
	if name == ast.SetNames || name == ast.SetCharset {
		return false
	}
	return true
}

func getRestoreSQLFromStmt(stmt *ast.SetStmt) (string, error) {
	sb := &strings.Builder{}
	restoreCtx := format.NewRestoreCtx(RestoreSetVariableFlags, sb)
	if err := stmt.Restore(restoreCtx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// get variables to set and variables to set default
func getDiffVariableList(frontend, current map[string]*ast.VariableAssignment) ([]*ast.VariableAssignment, []*ast.VariableAssignment) {
	var toSet, toRemove []*ast.VariableAssignment
	for k, v := range frontend {
		if currentValue, ok := current[k]; ok && isAstEquals(currentValue, v) {
			continue
		}
		toSet = append(toSet, v)
	}
	for k, v := range current {
		if _, ok := frontend[k]; !ok {
			toRemove = append(toRemove, v)
		}
	}
	return toSet, toRemove
}

func isAstEquals(a, b ast.Node) bool {
	sba := strings.Builder{}
	ctx1 := format.NewRestoreCtx(RestoreSetVariableFlags, &sba)
	a.Restore(ctx1)

	sbb := strings.Builder{}
	ctx2 := format.NewRestoreCtx(RestoreSetVariableFlags, &sbb)
	b.Restore(ctx2)
	return sba.String() == sbb.String()
}
