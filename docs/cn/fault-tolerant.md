# 熔断限流机制

<img src="docs/cn/assets/rateLimiterAndBreaker.png" style="zoom:60%;" />

## 熔断
```
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
### 熔断过程

从配置文件中我们得知 strategies 是一个数组，那么 namespace 中可以在 scope 下配置多种熔断策略。当请求从客户端进入 weir ，weir 会根据链接账户选择要进入的租户(namesapce)，同时启动对应租户下的熔断管理器，
熔断管理器根据 scope 可以选择当前是哪一类熔断器，再根据类别中的特征，比如库名表名，sql 特征等选择对应的熔断器对象，进行计数统计，如下图:

<img src="docs/cn/assets/breaker_process.png" style="zoom:60%;" />

当熔断时返回错误 **circuit breaker triggered** 。

### 熔断级别

这里要说的是熔断级别的问题，目前 weir 支持4种熔断级别

| 级别 | 说明 |
| --- | --- |
| namespace | 租户级别熔断，熔断触发时所有对此租户进行熔断 |
| db | 库级别熔断, 熔断触发时所有对此库的 sql 进行熔断 |
| table | 表级别熔断, 熔断触发时所有对此表的 sql 进行熔断 |
| sql | sql 级别熔断, 对每一个特征 sql 做监控管理，颗粒度最细，熔断触发时对这一类 sql 进行熔断 |

sql 熔断在内存中的输入 eg :

```
select * from test_table where id = 2;
select * from test_table where in (1,2,3);
```
在熔断器中将被转化成:

```
select * from test_table where id = ?;
select * from test_table where in (?);
```

这里我们在 ast 解析时通过判断 ast 的 node 类型进行了值的替换, 并重写了 sql 进行输出, 这样就可以确定一类 sql 并提取他们的摘要方便我们后续使用, 如下图:
<img src="docs/cn/assets/ast.png" style="zoom:50%;" />

熔断器计数采用的是滑动窗口计数器，滑动窗口有实现简单，能应对周期比较长的统计
<img src="docs/cn/assets/sliding_window.png" style="zoom:80%;" />

## 限流

限流分为阻塞式限流和拒绝式限流，目前当前版本完成的是拒绝式限流
拒绝式限流统计数据同样是采用滑动窗口计数器，在周期内会统计 qps 数据，qps 一旦大于阈值将执行限流，期间返回错误 **rate limited**
