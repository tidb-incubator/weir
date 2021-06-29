package proxy

import (
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/gin-gonic/gin"
	"github.com/tidb-incubator/weir/pkg/config"
	"github.com/tidb-incubator/weir/pkg/configcenter"
	"github.com/tidb-incubator/weir/pkg/proxy/namespace"
	"github.com/tidb-incubator/weir/pkg/proxy/server"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	ParamNamespace = "namespace"
	ParamBreaker   = "breaker"
)

type HttpApiServer struct {
	cfg         *config.Proxy
	proxyServer *server.Server
	nsmgr       *namespace.NamespaceManager
	cfgCenter   configcenter.ConfigCenter
	listener    net.Listener
	closeCh     chan struct{}

	engine *gin.Engine
}

type NamespaceHttpHandler struct {
	nsmgr     *namespace.NamespaceManager
	cfgCenter configcenter.ConfigCenter
}

type CommonJsonResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func NewNamespaceHttpHandler(nsmgr *namespace.NamespaceManager, cfgCenter configcenter.ConfigCenter) *NamespaceHttpHandler {
	return &NamespaceHttpHandler{
		nsmgr:     nsmgr,
		cfgCenter: cfgCenter,
	}
}

func CreateHttpApiServer(proxyServer *server.Server, nsmgr *namespace.NamespaceManager,
	cfgCenter configcenter.ConfigCenter, cfg *config.Proxy) (*HttpApiServer, error) {

	apiServer := &HttpApiServer{
		cfg:         cfg,
		proxyServer: proxyServer,
		nsmgr:       nsmgr,
		cfgCenter:   cfgCenter,
		closeCh:     make(chan struct{}),
	}

	listener, err := net.Listen("tcp", apiServer.cfg.AdminServer.Addr)
	if err != nil {
		return nil, err
	}
	apiServer.listener = listener

	engine := gin.New()
	engine.Use(gin.Recovery())

	namespaceRouteGroup := engine.Group("/admin/namespace")
	apiServer.wrapBasicAuthGinMiddleware(namespaceRouteGroup)
	namespaceHttpHandler := NewNamespaceHttpHandler(apiServer.nsmgr, apiServer.cfgCenter)
	namespaceHttpHandler.AddHandlersToRouteGroup(namespaceRouteGroup)

	metricsRouteGroup := engine.Group("/metrics")
	metricsRouteGroup.GET("/", gin.WrapF(promhttp.Handler().ServeHTTP))

	pprofRouteGroup := engine.Group("/debug/pprof")
	pprofRouteGroup.Any("/", gin.WrapF(pprof.Index))
	pprofRouteGroup.Any("/cmdline", gin.WrapF(pprof.Cmdline))
	pprofRouteGroup.Any("/profile", gin.WrapF(pprof.Profile))
	pprofRouteGroup.Any("/symbol", gin.WrapF(pprof.Symbol))
	pprofRouteGroup.Any("/trace", gin.WrapF(pprof.Trace))
	pprofRouteGroup.Any("/block", gin.WrapF(pprof.Handler("block").ServeHTTP))
	pprofRouteGroup.Any("/goroutine", gin.WrapF(pprof.Handler("goroutine").ServeHTTP))
	pprofRouteGroup.Any("/heap", gin.WrapF(pprof.Handler("heap").ServeHTTP))
	pprofRouteGroup.Any("/mutex", gin.WrapF(pprof.Handler("mutex").ServeHTTP))
	pprofRouteGroup.Any("/threadcreate", gin.WrapF(pprof.Handler("threadcreate").ServeHTTP))
	pprofRouteGroup.Any("/allocs", gin.WrapF(pprof.Handler("allocs").ServeHTTP))

	apiServer.engine = engine
	return apiServer, nil
}

func (h *HttpApiServer) wrapBasicAuthGinMiddleware(group *gin.RouterGroup) {
	basicAuthUser := h.cfg.AdminServer.User
	basicAuthPassword := h.cfg.AdminServer.Password
	if basicAuthUser != "" && basicAuthPassword != "" {
		group.Use(gin.BasicAuth(gin.Accounts{basicAuthUser: basicAuthPassword}))
	}
}

func (h *HttpApiServer) Run() {
	defer func() {
		if err := h.listener.Close(); err != nil {
			logutil.BgLogger().Warn("close http api server listener error", zap.Error(err))
		}
	}()

	errCh := make(chan error)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/", h.engine)
		errCh <- http.Serve(h.listener, mux)
	}()

	select {
	case <-h.closeCh:
		logutil.BgLogger().Info("closing http api server")
	case err := <-errCh:
		logutil.BgLogger().Fatal("http api server exit on error", zap.Error(err))
	}
}

func (h *HttpApiServer) Close() {
	close(h.closeCh)
}

func (n *NamespaceHttpHandler) AddHandlersToRouteGroup(group *gin.RouterGroup) {
	group.POST("/remove/:namespace", n.HandleRemoveNamespace)
	group.POST("/reload/prepare/:namespace", n.HandlePrepareReload)
	group.POST("/reload/commit/:namespace", n.HandleCommitReload)
}

func (n *NamespaceHttpHandler) HandleRemoveNamespace(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusBadRequest, "bad namespace parameter"))
		return
	}

	n.nsmgr.RemoveNamespace(ns)

	logutil.BgLogger().Info("remove namespace success", zap.String("namespace", ns))
	c.JSON(http.StatusOK, CreateSuccessJsonResp())
}

func (n *NamespaceHttpHandler) HandlePrepareReload(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusBadRequest, "bad namespace parameter"))
		return
	}

	nscfg, err := n.cfgCenter.GetNamespace(ns)
	if err != nil {
		errMsg := "get namespace value from configcenter error"
		logutil.BgLogger().Error(errMsg, zap.Error(err))
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusInternalServerError, errMsg))
		return
	}
	if err := n.nsmgr.PrepareReloadNamespace(ns, nscfg); err != nil {
		errMsg := "prepare reload namespace error"
		logutil.BgLogger().Error(errMsg, zap.Error(err), zap.String("namespace", ns))
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusInternalServerError, errMsg))
		return
	}

	logutil.BgLogger().Info("prepare reload success", zap.String("namespace", ns))
	c.JSON(http.StatusOK, CreateSuccessJsonResp())
}

func (n *NamespaceHttpHandler) HandleCommitReload(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusBadRequest, "bad namespace parameter"))
		return
	}

	if err := n.nsmgr.CommitReloadNamespaces([]string{ns}); err != nil {
		errMsg := "commit reload namespace error"
		logutil.BgLogger().Error(errMsg, zap.Error(err), zap.String("namespace", ns))
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusInternalServerError, errMsg))
		return
	}

	logutil.BgLogger().Info("commit reload success", zap.String("namespace", ns))
	c.JSON(http.StatusOK, CreateSuccessJsonResp())
}

func CreateJsonResp(code int, msg string) CommonJsonResp {
	return CommonJsonResp{
		Code: code,
		Msg:  msg,
	}
}

func CreateSuccessJsonResp() CommonJsonResp {
	return CommonJsonResp{
		Code: http.StatusOK,
		Msg:  "success",
	}
}
