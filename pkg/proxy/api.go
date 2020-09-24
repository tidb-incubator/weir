package proxy

import (
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/configcenter"
	"github.com/pingcap-incubator/weir/pkg/proxy/namespace"
	"github.com/pingcap-incubator/weir/pkg/proxy/server"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	ParamNamespace  = "namespace"
	ParamReloadType = "reloadtype"
)

const (
	ReloadTypeFrontend = 1
	ReloadTypeBackend  = 2
	ReloadTypeAll      = 3
)

type HttpApiServer struct {
	cfg         *config.Proxy
	proxyServer *server.Server
	nsmgr       *namespace.NamespaceManagerImpl
	cfgCenter   configcenter.ConfigCenter
	listener    net.Listener
	closeCh     chan struct{}

	engine *gin.Engine
}

type NamespaceHttpHandler struct {
	nsmgr     *namespace.NamespaceManagerImpl
	cfgCenter configcenter.ConfigCenter
}

type CommonJsonResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func NewNamespaceHttpHandler(nsmgr *namespace.NamespaceManagerImpl, cfgCenter configcenter.ConfigCenter) *NamespaceHttpHandler {
	return &NamespaceHttpHandler{
		nsmgr:     nsmgr,
		cfgCenter: cfgCenter,
	}
}

func CreateHttpApiServer(proxyServer *server.Server, nsmgr *namespace.NamespaceManagerImpl,
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
	group.POST("/create/:namespace", n.HandleCreateNamespace)
	group.POST("/remove/:namespace", n.HandleRemoveNamespace)
	group.POST("/reload/prepare/:namespace", n.HandlePrepareReload)
	group.POST("/reload/commit/:namespace", n.HandleCommitReload)
}

func (n *NamespaceHttpHandler) HandleCreateNamespace(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusBadRequest, CreateJsonResp(http.StatusBadRequest, "bad namespace parameter"))
		return
	}

	nscfg, err := n.cfgCenter.GetNamespace(ns)
	if err != nil {
		errMsg := "get namespace value from configcenter error"
		logutil.BgLogger().Error(errMsg, zap.Error(err))
		c.JSON(http.StatusInternalServerError, CreateJsonResp(http.StatusInternalServerError, errMsg))
		return
	}

	if err := n.nsmgr.CreateNamespace(nscfg); err != nil {
		errMsg := "create namespace error"
		logutil.BgLogger().Error(errMsg, zap.Error(err))
		c.JSON(http.StatusInternalServerError, CreateJsonResp(http.StatusInternalServerError, errMsg))
		return
	}

	c.JSON(http.StatusOK, CreateSuccessJsonResp())
}

func (n *NamespaceHttpHandler) HandleRemoveNamespace(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusBadRequest, "bad namespace parameter"))
		return
	}

	if err := n.nsmgr.RemoveNamespace(ns); err != nil {
		logutil.BgLogger().Error("remove namespace error", zap.Error(err))
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusInternalServerError, err.Error()))
		return
	}

	c.JSON(http.StatusOK, CreateSuccessJsonResp())
}

func (n *NamespaceHttpHandler) HandlePrepareReload(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusBadRequest, CreateJsonResp(http.StatusBadRequest, "bad namespace parameter"))
		return
	}

	reloadType := c.GetInt(ParamReloadType)
	if !isValidReloadType(reloadType) {
		c.JSON(http.StatusBadRequest, "invalid reload type")
		return
	}

	nscfg, err := n.cfgCenter.GetNamespace(ns)
	if err != nil {
		c.JSON(http.StatusInternalServerError, errors.WithMessage(err, "get namespace value from configcenter error"))
		return
	}

	if reloadType == ReloadTypeFrontend {
		if err := n.nsmgr.PrepareReloadFrontend(ns, &nscfg.Frontend); err != nil {
			c.JSON(http.StatusInternalServerError, errors.WithMessage(err, "prepare reload frontend namespace error"))
			return
		}
	} else if reloadType == ReloadTypeBackend {
		if err := n.nsmgr.PrepareReloadBackend(ns, &nscfg.Backend); err != nil {
			c.JSON(http.StatusInternalServerError, errors.WithMessage(err, "prepare reload backend namespace error"))
			return
		}
	} else if reloadType == ReloadTypeAll {
		if err := n.nsmgr.PrepareReloadFrontend(ns, &nscfg.Frontend); err != nil {
			c.JSON(http.StatusInternalServerError, errors.WithMessage(err, "prepare reload frontend namespace error"))
			return
		}
		if err := n.nsmgr.PrepareReloadBackend(ns, &nscfg.Backend); err != nil {
			c.JSON(http.StatusInternalServerError, errors.WithMessage(err, "prepare reload backend namespace error"))
			return
		}
	}

	c.JSON(http.StatusOK, "OK")
}

func (n *NamespaceHttpHandler) HandleCommitReload(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	reloadType := c.GetInt(ParamReloadType)
	if !isValidReloadType(reloadType) {
		c.JSON(http.StatusBadRequest, "invalid reload type")
		return
	}

	if reloadType == ReloadTypeFrontend {
		if err := n.nsmgr.CommitReloadFrontend(ns); err != nil {
			c.JSON(http.StatusInternalServerError, errors.WithMessage(err, "commit reload frontend namespace error"))
			return
		}
	} else if reloadType == ReloadTypeBackend {
		if err := n.nsmgr.CommitReloadBackend(ns); err != nil {
			c.JSON(http.StatusInternalServerError, errors.WithMessage(err, "commit reload backend namespace error"))
			return
		}
	} else if reloadType == ReloadTypeAll {
		if err := n.nsmgr.CommitReloadFrontend(ns); err != nil {
			c.JSON(http.StatusInternalServerError, errors.WithMessage(err, "commit reload frontend namespace error"))
			return
		}
		if err := n.nsmgr.CommitReloadBackend(ns); err != nil {
			c.JSON(http.StatusInternalServerError, errors.WithMessage(err, "commit reload backend namespace error"))
			return
		}
	}

	c.JSON(http.StatusOK, "OK")
}

func isValidReloadType(t int) bool {
	return t == ReloadTypeFrontend || t == ReloadTypeBackend
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
