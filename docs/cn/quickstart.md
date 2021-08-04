# 快速上手

本文介绍如何快速上手体验Weir平台.

## 前提

使用 weir-proxy 的前提首先要部署一套TiDB集群。对 weir-proxy 来说, 后端数据库也可使用 MySQL 代替 TiDB 进行测试.

### 安装 TiDB/MySQL

安装 TiDB 可参考 [TiDB数据库快速上手指南](https://docs.pingcap.com/zh/tidb/stable/quick-start-with-tidb) 进行安装。

### 构造数据

安装完成后, 需要连接TiDB / MySQL 执行以下SQL语句进行建库和建表操作, 方便测试weir-proxy.

#### 建库
```
DROP DATABASE IF EXISTS `test_weir_db`;
CREATE DATABASE `test_weir_db`;
USE `test_weir_db`;
```

#### 建表
```
CREATE TABLE `test_weir_user` (
  `id` bigint(22) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `uniq_name` (`name`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `test_weir_admin` (
  `id` bigint(22) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  `status` varchar(128) NOT NULL DEFAULT 'normal',
  PRIMARY KEY (`id`),
  UNIQUE `uniq_name` (`name`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

#### 写入测试数据
```
INSERT INTO `test_weir_user` (name) VALUES ('Bob');
INSERT INTO `test_weir_user` (name) VALUES ('Alice');

INSERT INTO `test_weir_admin` (name) VALUES ('Ed');
INSERT INTO `test_weir_admin` (name) VALUES ('Huang');
```

目前Weir平台的代理层中间件weir-proxy已开源, 中控组件weir-controller和控制台weir-dashboard还未开源.

## 安装启动 weir-proxy

### 从源码编译安装

首先, 从github克隆代码仓库到本地.

```
$ git clone https://github.com/tidb-incubator/weir
```

构建weir-proxy.

```
$ make weirproxy
```

生成的weirproxy二进制文件位于bin/目录下, 文件名为bin/weirproxy.

启动weir-proxy.

```
$ ./bin/weirproxy &
```

weir-proxy会默认读取示例配置文件conf/weirproxy.yml进行启动, 示例配置使用本地文件作为namespace配置中心, 配置文件位于conf/namespace/目录下.

使用MySQL客户端通过weir-proxy访问TiDB集群.

```
$ mysql -h127.0.0.1 -P6000 -uhello -pworld test_weir_db

mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1
Server version: 5.7.25-TiDB-None MySQL Community Server (GPL)

Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

如果看到连接成功, 说明weir-proxy已经启动并可以使用了. 恭喜你!
