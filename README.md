# gorge-db-api

Go 数据库管理 API 服务，为 Phorge 提供 Phorge 兼容的数据库基础设施 HTTP API。实现了与 Phorge PHP 端相同的数据库集群管理能力，包括健康探测、Schema 差异检查、安装检查和迁移状态查询。

## 特性

- 数据库集群健康探测，支持主从复制状态检查和延迟监控
- Schema 差异检查，从 INFORMATION_SCHEMA 构建层级化 Schema 树
- 数据库安装检查，验证 MySQL 版本、引擎、配置参数和时钟同步
- 迁移状态查询，读取 patch_status 表比对已应用和缺失的 patch
- 主从路由与读写分离，连接级只读保护
- 嵌套事务支持，通过 MySQL savepoint 实现
- 连接重试与故障转移，master 不可用时自动 fallback 到 replica
- 双重配置模式：Phorge 原生 JSON 配置和独立环境变量
- 统一 Phorge Conduit 兼容的 JSON 响应格式
- 静态编译，Docker 镜像极轻量
- 内置健康检查端点，适配容器编排

## 快速开始

### 本地运行

```bash
go build -o gorge-db-api ./cmd/server
./gorge-db-api
```

服务默认监听 `:8080`。

### Docker 运行

```bash
docker build -t gorge-db-api .
docker run -p 8080:8080 gorge-db-api
```

### 带配置运行

```bash
export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASS=your_password
export SERVICE_TOKEN=your_service_token
export STORAGE_NAMESPACE=phorge
./gorge-db-api

# 或使用 Phorge 配置文件
export PHORGE_CONFIG=/path/to/local.json
./gorge-db-api
```

## 配置

支持两种配置方式：环境变量（默认）和 Phorge 风格 JSON 配置文件。

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | 服务监听地址 |
| `PHORGE_CONFIG` | (空) | Phorge 风格 JSON 配置文件路径，设置后从文件加载配置 |
| `MYSQL_HOST` | `127.0.0.1` | MySQL 主机地址 |
| `MYSQL_PORT` | `3306` | MySQL 端口 |
| `MYSQL_USER` | `root` | MySQL 用户名 |
| `MYSQL_PASS` | (空) | MySQL 密码 |
| `STORAGE_NAMESPACE` | `phorge` | 存储命名空间，用作数据库名前缀 |
| `SERVICE_TOKEN` | (空) | API 认证 Token，通过 `X-Service-Token` 请求头传递 |

### JSON 配置文件

当设置 `PHORGE_CONFIG` 时，从指定的 JSON 文件加载配置：

```json
{
  "mysql.host": "127.0.0.1",
  "mysql.port": 3306,
  "mysql.user": "root",
  "mysql.pass": "password",
  "storage.default-namespace": "phorge",
  "cluster.databases": [
    {
      "host": "master1",
      "port": 3306,
      "role": "master",
      "partition": ["default"]
    },
    {
      "host": "replica1",
      "port": 3306,
      "role": "replica"
    }
  ]
}
```

## API

所有 `/api/db/*` 端点在配置 `SERVICE_TOKEN` 时需要认证。认证方式：

- 请求头：`X-Service-Token: <token>`
- 查询参数：`?token=<token>`

### GET /api/db/servers

列出所有数据库节点及健康状态。

**响应** (200)：

```json
{
  "data": [
    {
      "host": "127.0.0.1",
      "port": 3306,
      "is_master": true,
      "connection_status": "okay",
      "connection_latency_sec": 0.003,
      "replica_status": "okay"
    }
  ]
}
```

### GET /api/db/servers/:ref/health

指定节点健康详情。`:ref` 为节点标识，格式为 `host:port`。

### GET /api/db/schema-diff

从 INFORMATION_SCHEMA 获取 Schema 树结构。

### GET /api/db/schema-issues

扁平化的 Schema 问题列表。

### GET /api/db/setup-issues

数据库安装配置检查结果。

### GET /api/db/charset-info

各节点字符集信息。

### GET /api/db/migrations/status

迁移 patch 应用状态。

### GET /healthz

健康检查端点，不需要认证。

**响应** (200)：

```json
{"status": "ok"}
```

### 错误响应

所有错误响应使用统一的 JSON 格式：

```json
{
  "error": {
    "code": "ERR_QUERY",
    "message": "connect failed after 3 attempts: ..."
  }
}
```

| 错误码 | HTTP 状态码 | 含义 |
|---|---|---|
| `ERR_UNAUTHORIZED` | 401 | Service Token 缺失 |
| `ERR_READONLY_WRITE` | 409 | 对只读连接发起写操作 |
| `ERR_MASTER_UNREACHABLE` | 503 | Master 节点不可达 |
| `ERR_ALL_UNREACHABLE` | 503 | 所有节点不可达 |
| `ERR_ACCESS_DENIED` | 403 | 数据库访问被拒绝 |
| `ERR_CONNECTION_LOST` | 500 | 数据库连接丢失 |
| `ERR_QUERY` | 500 | 查询执行失败 |

## 项目结构

```
gorge-db-api/
├── cmd/server/main.go              # 服务入口
├── internal/
│   ├── cluster/
│   │   ├── config.go               # 集群配置加载（JSON 文件 / 环境变量）
│   │   ├── ref.go                  # DatabaseRef 节点模型与状态常量
│   │   ├── router.go               # 主从路由与连接缓存
│   │   └── health.go               # 健康探测与复制状态检查
│   ├── dbcore/
│   │   ├── conn.go                 # 数据库连接池与读写保护
│   │   ├── query.go                # 只读查询正则判定
│   │   ├── retry.go                # 连接与查询重试策略
│   │   └── tx.go                   # 嵌套事务管理（savepoint）
│   ├── schema/
│   │   ├── schema.go               # Schema 差异与字符集检查
│   │   ├── setup.go                # 安装配置检查
│   │   └── migration.go            # 迁移状态查询
│   ├── compat/
│   │   ├── response.go             # 统一 API 响应结构
│   │   └── errors.go               # 错误码体系与 MySQL 错误映射
│   └── httpapi/
│       └── handlers.go             # HTTP 路由注册、认证中间件与处理器
├── scripts/
│   └── validate.sh                 # 集成验证脚本
├── Dockerfile                      # 多阶段 Docker 构建
├── go.mod
└── go.sum
```

## 开发

```bash
# 运行全部测试
go test ./...

# 运行测试（带详细输出）
go test -v ./...

# 构建二进制
go build -o gorge-db-api ./cmd/server

# 集成验证（需要运行中的服务和 MySQL）
./scripts/validate.sh http://localhost:8080 your_service_token
```

## 技术栈

- **语言**：Go 1.26
- **HTTP 框架**：[Echo](https://echo.labstack.com/) v4.15.1
- **数据库驱动**：[go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) v1.9.3
- **许可证**：Apache License 2.0
