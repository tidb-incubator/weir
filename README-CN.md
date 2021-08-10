# Weir

Weir是伴鱼公司研发的开源数据库代理平台, 主要为TiDB分布式数据库提供数据库治理功能.

## 功能特性

> 1. Weir 为 MySQL 协议提供应用层代理，兼容 TiDB 4.0。[L7层负载](docs/cn/proxy.md)
> 2. Weir 使用连接池进行后端连接管理，并支持负载均衡。[链接管理](docs/cn/connection-management.md)
> 3. Weir 支持多租户管理，所有命名空间都可以在运行时动态重新加载。[多租户软隔离](docs/cn/multi-tenant.md)
> 4. Weir 支持 qps 限流和熔断机制来保护客户端和 TiDB 服务器。[熔断限流机制](docs/cn/fault-tolerant.md)

## 使用手册

- [快速上手](docs/cn/quickstart.md)
- [Proxy配置详解](docs/cn/proxy-config.md)
- [Namespace配置详解](docs/cn/namespace-config.md)
- [集群部署](docs/cn/cluster_deployment.md)
- [配置热加载](docs/cn/config-dynamic-reload.md)
- [监控与告警](docs/cn/monitoring.md)

## FAQ
