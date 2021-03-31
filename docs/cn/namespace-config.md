# Namespace配置详解

## 配置示例

### 客户端连接配置

```
version: "v1"
namespace: "test_namespace"
frontend:
  allowed_dbs:
    - "test_weir_db"
  idle_timeout: 3600
  users:
    - username: "hello"
      password: "world"
    - username: "hello1"
      password: "world1"
```

字段说明

- namespace: Namespace名称, 要求Proxy集群内唯一
- frontend: 客户端连接相关配置
  - allowed_dbs: 客户端允许访问的Database列表
  - sql_blacklist: SQL黑名单列表
  - sql_whitelist: SQL白名单列表
  - idle_timeout: 客户端连接空闲超时关闭时间 (单位: 秒)
  - users: 用户连接信息列表
    - username: 用户名 (要求Proxy集群内唯一)
    - password: 密码

### 后端连接池配置

```
backend:
  instances:
    - "127.0.0.1:3306"
  username: "root"
  password: "12344321"
  selector_type: "random"
  pool_size: 10
  idle_timeout: 60
```

字段说明

- backend: 后端连接相关配置
  - instances: TiDB Server实例地址列表
  - username: 连接TiDB Server用户名
  - password: 连接TiDB Server密码
  - selector_type: 负载均衡策略, 目前只支持random
  - pool_size: 连接池最大连接数 (针对每个TiDB Server)
  - idle_timeout: 连接池连接空闲超时关闭时间 (单位: 秒)

### 熔断器配置

```
breaker:
  scope: "sql"
  strategies:
    - min_qps: 3
      failure_rate_threshold: 0
      failure_num: 5
      sql_timeout_ms: 2000
      open_status_duration_ms: 5000
      size: 10
      cell_interval_ms: 1000
```

字段说明

- breaker: 熔断器配置
  - scope: 熔断器粒度, 支持参数: namespace, db, table, sql
  - strategies: 熔断策略列表
    - min_qps: 最小启动QPS
    - failure_rate_threshold: 错误率阈值百分数 (0~100)
    - failure_num: 错误数阈值 (与failure_rate_threshold只能使用其一)
    - sql_timeout_ms: SQL超时阈值, (单位: 毫秒)
    - open_status_duration_ms: 熔断器开启状态持续时间 (单位: 毫秒)
    - size: 统计周期内的单元数量
    - cell_interval_ms: 每个单元的时长 (单位: 毫秒), 与size字段共同组成了熔断器时间统计的滑窗

### 限流器配置

```
rate_limiter:
  scope: "db"
  qps: 1000
```

字段说明

- rate_limiter: 限流器配置
  - scope: 限流器粒度, 支持参数: namespace, db, table
  - qps: 限流QPS (超过阈值的请求会直接返回错误)

## 完整配置示例

```
version: "v1"
namespace: "test_namespace"
frontend:
  allowed_dbs:
    - "test_weir_db_0"
    - "test_weir_db_1"
  slow_sql_time: 50
  sql_blacklist:
    - sql: "select * from tbl0"
    - sql: "select * from tbl1"
  sql_whitelist:
    - sql: "select * from tbl2"
    - sql: "select * from tbl3"
  denied_ips:
  idle_timeout: 3600
  users:
    - username: "hello"
      password: "world"
    - username: "hello1"
      password: "world1"
backend:
  instances:
    - "127.0.0.1:4000"
  username: "root"
  password: "passwd"
  selector_type: "random"
  pool_size: 10
  idle_timeout: 60
breaker:
  scope: "sql"
  strategies:
    - min_qps: 3
      failure_rate_threshold: 0
      failure_num: 5
      sql_timeout_ms: 2000
      open_status_duration_ms: 5000
      size: 10
      cell_interval_ms: 1000
rate_limiter:
  scope: "db"
  qps: 1000
```
