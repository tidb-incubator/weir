# 应用层代理

Weir作为TiDB分布式数据库治理平台, 其代理组件weir-proxy在实现时复用了TiDB 4.0的协议层和SQL解析器, 因此weir-proxy在协议和语法层面, 对MySQL的兼容性与TiDB 4.0相同.

使用原生MySQL客户端即可连接weir-proxy, 通过weir-proxy将客户端的SQL请求转发到后端的TiDB Server集群.
