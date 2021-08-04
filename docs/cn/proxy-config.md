# Proxy配置详解

以下给出了Weir Proxy的示例配置🌰.

```
version: "v1"
proxy_server:
  addr: "0.0.0.0:6000"
  max_connections: 1000
  session_timeout: 600
admin_server:
  addr: "0.0.0.0:6001"
  enable_basic_auth: false
  user: ""
  password: ""
log:
  level: "debug"
  format: "console"
  log_file:
    filename: ""
    max_size: 300
    max_days: 1
    max_backups: 1
registry:
  enable: false
config_center:
  type: "file"
  config_file:
    path: "./conf/namespace"
    strict_parse: false
performance:
  tcp_keep_alive: true
```

| 配置名 | 说明 |
| --- | --- |
| version | 配置 schema 的版本号, 目前为 v1 |
| proxy_server | Proxy 代理服务相关配置 |
| proxy_server.addr | Proxy服务端口监听地址 |
| proxy_server.max_connections | 最大客户端连接数 |
| proxy_server.session_timeout | 客户端空闲链接超时时间 |
| admin_server | Proxy 管理相关配置 |
| admin_server.addr | Proxy admin 口监听地址 |
| admin_server.enable_basic_auth | 是否开启Basic Auth |
| admin_server.user | Basic Auth User |
| admin_server.password | Basic Auth Password |
| log | 日志配置 |
| log.level | 日志级别 (支持 debug, info, warn, error) |
| log.format | 日志输出方式 (支持 console, file) |
| log.log_file | 日志文件相关配置 |
| log.log_file.filename | 日志文件名 |
| log.log_file.max_size | 单个日志文件最大尺寸 |
| log.log_file.max_days | 单个日志文件保存最大天数 |
| config_center | 配置中心 |
| config_center.type | 配置中心类型 (支持 file, etcd) |
| config_center.config_file | 配置文件信息，在 type 为file时有效 |
| config_center.config_file.path | Namespace配置文件所在目录 |
| strict_parse | 对命名空间名称的严格校验，如果禁用strictParse，则在列出所有命名空间时将忽略解析命名空间错误 |
| performance | 性能相关配置 |
| tcp_keep_alive | 对客户端连接是否开启TCP Keep Alive |
