# Namespace配置详解

以下给出了Namespace示例配置.

```
version: "v1"
namespace: "test_namespace"
frontend:
  allowed_dbs:
    - "test_weir_db"
  slow_sql_time: 50
  denied_sqls:
  denied_ips:
  idle_timeout: 3600
  users:
    - username: "hello"
      password: "world"
    - username: "hello1"
      password: "world1"
backend:
  instances:
    - "127.0.0.1:3306"
  username: "root"
  password: "12344321"
  selector_type: "random"
  pool_size: 10
  idle_timeout: 60
```

- version: 配置schema的版本号, 目前为v1
- namespace: Namespace名称, 要求整个Proxy集群内唯一
- frontend: 客户端连接相关配置
  - allowed_dbs: 客户端允许访问的Database列表
  - slow_sql_time: 慢SQL日志阈值时间 (单位: 毫秒)
  - denied_sqls: SQL范式黑名单列表
  - denied_ips: IP黑名单列表
  - idle_timeout: 客户端连接空闲超时关闭时间 (单位: 秒)
  - users: 用户连接信息列表
- backend: 后端连接相关配置
  - instances: TiDB Server实例地址列表
  - username: 连接TiDB Server用户名
  - password: 连接TiDB Server密码
  - selector_type: 负载均衡策略, 目前只支持random
  - pool_size: 连接池最大连接数 (针对每个TiDB Server)
  - idle_timeout: 连接池连接空闲超时关闭时间 (单位: 秒)
