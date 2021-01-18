# Proxy配置详解

以下给出了Weir Proxy的示例配置.

```
version: "v1"
proxy_server:
  addr: "0.0.0.0:6000"
  max_connections: 10
  token_limit: 100
admin_server:
  addr: "0.0.0.0:6001"
  enable_basic_auth: false
  user: ""
  password: ""
log:
  level: "debug"
  format: "console"
config_center:
  type: "file"
  config_file:
    path: "./conf/namespace"
performance:
  tcp_keep_alive: true
```

- version: 配置schema的版本号, 目前为v1
- proxy_server: Proxy代理服务相关配置
  - addr: Proxy服务端口监听地址
  - max_connections: 最大客户端连接数
  - token_limit: 最大同时SQL请求数
- admin_server: Proxy管理相关配置
  - addr: Proxy管理端口监听地址
  - enable_basic_auth: 是否开启Basic Auth
  - user: Basic Auth User
  - password: Basic Auth Password
- log: 日志配置
  - level: 日志级别 (支持debug, info, warn, error)
  - format: 日志输出方式 (支持console, file)
- config_center: 配置中心
  - type: 配置中心类型 (支持file, etcd)
  - config_file: 在type为file时有效
    - path: Namespace配置文件所在目录
- performance: 性能相关配置
  - tcp_keep_alive: 对客户端连接是否开启TCP Keep Alive
