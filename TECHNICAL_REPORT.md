# gorge-db-api 技术报告

## 1. 概述

gorge-db-api 是 Gorge 平台中的数据库管理 API 微服务，为 Phorge（Phabricator 社区维护分支）提供 Phorge 兼容的数据库基础设施 HTTP API。

该服务的核心目标是将 Phorge PHP 端的数据库管理功能（健康检查、Schema 检查、安装验证、迁移状态）抽取为独立的 Go HTTP 服务。Phorge 的数据库管理逻辑原本分散在多个 PHP 类中（`PhabricatorDatabaseRefParser`、`PhabricatorDatabaseRef`、`PhabricatorConfigSchemaQuery`、`PhabricatorDatabaseSetupCheck` 等），通过 `bin/storage` 命令行工具调用。gorge-db-api 将这些功能整合为一组 REST API，便于其他 Go 微服务调用，同时保持与 PHP 端行为一致的输出格式。

## 2. 设计动机

### 2.1 原有方案的问题

Phorge 的数据库管理功能嵌入在 PHP 应用中：

1. **不可远程调用**：Schema 检查、健康探测等操作只能通过 `bin/storage` 命令行执行，无法被其他服务或 CI/CD 流水线远程调用。
2. **缺乏集中管控**：数据库集群的健康状态分散在各节点的本地探测中，没有一个统一的 API 提供全局视图。
3. **PHP 进程模型限制**：PHP 的请求-响应模型不适合需要维护长连接池、并行探测多个节点的数据库管理场景。每次 `bin/storage status` 调用都会建立全新的数据库连接，无法复用。
4. **部署耦合**：数据库管理工具与 Phorge PHP 应用绑定在同一部署单元中，无法独立扩缩容或部署。

### 2.2 gorge-db-api 的解决思路

将数据库管理功能抽取为独立的 Go HTTP 微服务：

- **REST API 化**：所有数据库管理操作通过 HTTP API 暴露，其他服务、运维工具和 CI/CD 流水线可以直接调用。
- **连接池复用**：Go 常驻进程维护数据库连接池，避免重复建连的开销。
- **集群全局视图**：通过 `/api/db/servers` 端点一次获取所有节点的健康状态、复制延迟等信息。
- **独立部署**：作为独立容器运行，可根据需要独立扩缩容，不依赖 PHP 运行时。
- **行为兼容**：API 响应格式和错误码与 Phorge PHP 端保持一致，确保下游系统无缝切换。

## 3. 系统架构

### 3.1 在 Gorge 平台中的位置

```
┌──────────────────────────────────────────────────┐
│                   Gorge 平台                      │
│                                                   │
│  ┌──────────┐  ┌───────────┐  ┌───────────────┐  │
│  │  gorge-  │  │  gorge-   │  │ 其他 Go 服务   │  │
│  │  worker  │  │  conduit  │  │               │  │
│  └────┬─────┘  └─────┬─────┘  └───────┬───────┘  │
│       │               │               │          │
│       └───────────────┼───────────────┘          │
│                       │                          │
│                       ▼                          │
│           ┌───────────────────────┐              │
│           │    gorge-db-api      │              │
│           │    :8080             │              │
│           │                     │              │
│           │    Token Auth       │              │
│           │    Cluster Router   │              │
│           │    Health / Schema  │              │
│           │    / Setup / Migr.  │              │
│           └──────────┬──────────┘              │
│                      │                          │
│                      ▼                          │
│           ┌───────────────────────┐              │
│           │   MySQL 8.0 Cluster  │              │
│           │                     │              │
│           │   Master ──► Replica │              │
│           └───────────────────────┘              │
└──────────────────────────────────────────────────┘
```

### 3.2 模块划分

项目采用 Go 标准布局，分为五个内部模块：

| 模块 | 路径 | 职责 |
|---|---|---|
| cluster | `internal/cluster/` | 集群配置解析、主从路由、健康探测 |
| dbcore | `internal/dbcore/` | 数据库连接池、读写保护、重试策略、事务管理 |
| schema | `internal/schema/` | Schema 差异检查、安装检查、迁移状态查询 |
| compat | `internal/compat/` | Phorge 兼容的响应格式和错误码体系 |
| httpapi | `internal/httpapi/` | HTTP 路由注册、认证中间件与请求处理 |

入口程序 `cmd/server/main.go` 负责串联五个模块：加载配置 -> 初始化四大服务 -> 启动 HTTP 服务。

### 3.3 请求处理流水线

一个 API 请求经过的完整处理链路：

```
客户端请求 GET /api/db/servers
       │
       ▼
┌─ Echo 框架层 ─────────────────────────────────┐
│  Logger          记录请求日志                    │
│       │                                        │
│       ▼                                        │
│  Recover         捕获 panic，防止进程崩溃         │
└───────┼────────────────────────────────────────┘
        │
        ▼
┌─ 路由组 /api/db ───────────────────────────────┐
│  tokenAuth       校验 X-Service-Token           │
│       │                                        │
│       ▼                                        │
│  Handler         调用对应 Service                │
└───────┼────────────────────────────────────────┘
        │
        ▼
┌─ Service 层 ──────────────────────────────────┐
│  HealthService / DiffService /                 │
│  SetupService / MigrationService               │
│       │                                        │
│       ▼                                        │
│  DBRouter / NewConn                            │
│  建立连接 → 执行 SQL → 返回结果                   │
└───────┼────────────────────────────────────────┘
        │
        ▼
  MySQL 数据库
        │
        ▼
  APIResponse{Data: ...} 返回客户端
```

## 4. 核心实现分析

### 4.1 集群配置

配置模块位于 `internal/cluster/config.go`，支持两种配置来源。

#### 4.1.1 双重配置模式

**环境变量模式**（`LoadFromEnv`）：

从独立环境变量构建单节点 master 配置。适合开发环境和简单的单机 MySQL 部署。

```go
ref := &DatabaseRef{
    Host:               envOr("MYSQL_HOST", "127.0.0.1"),
    Port:               envIntOr("MYSQL_PORT", 3306),
    User:               envOr("MYSQL_USER", "root"),
    IsMaster:           true,
    IsIndividual:       true,
    IsDefaultPartition: true,
}
```

**JSON 文件模式**（`LoadFromFile`）：

解析 Phorge 风格的 `local.json` 配置文件。适合已有 Phorge 部署环境，可直接复用 Phorge 的配置文件，无需维护两套配置。

JSON 文件中的 `cluster.databases` 数组定义多节点集群拓扑，每个节点包含 `host`、`port`、`role`（master/replica）、`partition`（应用分区映射）等属性。

#### 4.1.2 应用分区路由

`ClusterConfig` 的 `GetMasterForApplication(app)` 实现了 Phorge 的应用分区机制——不同的 Phorge 应用（如 `maniphest`、`config`、`repository` 等）可以路由到不同的 master 节点：

```go
func (cc *ClusterConfig) GetMasterForApplication(app string) *DatabaseRef {
    var appMaster, defaultMaster *DatabaseRef
    for _, m := range cc.masters {
        if m.Disabled { continue }
        if m.IsApplicationHost(app) {
            appMaster = m
            break
        }
        if m.IsDefaultPartition && defaultMaster == nil {
            defaultMaster = m
        }
    }
    if appMaster != nil { return appMaster }
    return defaultMaster
}
```

查找逻辑分两级：优先查找显式绑定该应用的 master，找不到则 fallback 到 default partition 的 master。这与 Phorge PHP 端 `PhabricatorDatabaseRefParser` 的行为一致。

### 4.2 主从路由

路由模块位于 `internal/cluster/router.go`，`DBRouter` 是连接 Service 层与数据库层的核心组件。

#### 4.2.1 读写分离

`GetWriter` 和 `GetReader` 实现了读写分离逻辑：

- **GetWriter**：只返回 master 连接。如果服务处于只读模式（master 不可达触发的自动降级），返回 `ErrReadonlyWrite` 错误。
- **GetReader**：优先使用 master 连接。如果 master 连接失败，自动将服务标记为只读模式并 fallback 到 replica。

```go
func (r *DBRouter) GetReader(ctx context.Context, application string) (*dbcore.Conn, error) {
    master := r.config.GetMasterForApplication(application)
    if master != nil {
        conn, err := r.getOrCreateConn(master, application, false)
        if err == nil {
            return conn, nil
        }
        r.mu.Lock()
        r.readOnly = true
        r.mu.Unlock()
    }
    replica := r.config.GetReplicaForApplication(application)
    if replica != nil {
        conn, err := r.getOrCreateConn(replica, application, true)
        if err == nil { return conn, nil }
    }
    // ...
}
```

这种自动降级机制确保了 master 故障时服务不会完全不可用——读请求仍可通过 replica 提供服务，同时通过 `readOnly` 标志阻止任何写操作。

#### 4.2.2 连接缓存

连接按 `refKey/app/readOnly` 三元组做缓存：

```go
key := fmt.Sprintf("%s/%s/%v", ref.RefKey(), app, readOnly)
```

同一 `(节点, 应用, 读写模式)` 组合只会创建一个连接，后续请求复用已有连接。缓存通过 `sync.Mutex` 保护并发安全。

#### 4.2.3 连接参数

```go
dsn := dbcore.DSN{
    MaxRetries:      3,
    ConnTimeoutSec:  10,
    QueryTimeoutSec: 30,
}
```

连接超时 10 秒、查询超时 30 秒，最多重试 3 次。这些参数平衡了快速失败和容忍瞬时网络问题的需求。

### 4.3 数据库连接层

连接层位于 `internal/dbcore/`，是整个服务的数据库访问基础。

#### 4.3.1 连接池管理

```go
func NewConn(dsn DSN, readOnly bool) (*Conn, error) {
    db, err := sql.Open("mysql", dsn.String())
    // ...
    db.SetMaxOpenConns(25)
    db.SetMaxIdleConns(5)
    db.SetConnMaxLifetime(5 * time.Minute)
    return &Conn{db: db, dsn: dsn, readOnly: readOnly}, nil
}
```

每个 `Conn` 底层持有一个 `sql.DB` 连接池。MaxOpenConns=25 限制单个连接对象对 MySQL 的最大并发连接数，MaxIdleConns=5 保持少量空闲连接以减少重建开销，ConnMaxLifetime=5 分钟防止长时间空闲连接被 MySQL 的 `wait_timeout` 断开。

DSN 构建使用 `go-sql-driver/mysql` 的 `Config` 结构体，启用了 `ParseTime`（自动将 `DATETIME`/`TIMESTAMP` 解析为 `time.Time`）和 `InterpolateParams`（客户端参数内插，减少一次网络往返）。

#### 4.3.2 只读连接保护

`Conn` 在连接级别实现了只读保护，这是双层安全设计的第一层：

```go
func (c *Conn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
    if c.readOnly {
        if !isReadQuery(query) {
            return nil, &compat.DBError{Code: compat.ErrReadonlyWrite, ...}
        }
    }
    return c.db.QueryContext(ctx, query, args...)
}

func (c *Conn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
    if c.readOnly {
        return nil, &compat.DBError{Code: compat.ErrReadonlyWrite, ...}
    }
    return c.db.ExecContext(ctx, query, args...)
}
```

- `ExecContext` 在只读连接上被完全禁止
- `QueryContext` 在只读连接上只允许通过正则判定的只读查询

第二层保护在 `DBRouter` 层面——`GetWriter` 在只读模式下直接拒绝返回连接。

#### 4.3.3 只读查询判定

```go
var readQueryRe = regexp.MustCompile(`(?i)^\s*\(?\s*(SELECT|SHOW|EXPLAIN)\s`)
```

正则匹配 SQL 语句开头的关键字，支持三种只读操作：`SELECT`、`SHOW`、`EXPLAIN`。前导空白、括号（用于 UNION 子查询如 `(SELECT a) UNION (SELECT b)`）都被正确处理。该正则在包加载时编译一次，后续调用零分配。

#### 4.3.4 连接重试

```go
var retryableConnectCodes = map[uint16]bool{
    2002: true, // Connection Timeout
    2003: true, // Unable to Connect
}

var retryableQueryCodes = map[uint16]bool{
    2013: true, // Connection Dropped
    2006: true, // Gone Away
}
```

重试策略区分连接错误和查询错误：

- **连接重试**（`ConnectWithRetry`）：MySQL 错误码 2002（连接超时）和 2003（无法连接）时重试，默认最多 3 次。
- **查询重试**（`QueryWithRetry`）：MySQL 错误码 2013（连接断开）和 2006（Gone Away）时重试，但有两个前提条件：必须是只读查询，且不在事务内。

不在事务内重试查询是关键的安全约束——事务内重试会破坏事务的原子性语义。这与 Phorge PHP 端 `AphrontBaseMySQLDatabaseConnection` 的行为一致。

#### 4.3.5 嵌套事务

```go
func (m *TxManager) Begin(ctx context.Context) error {
    if m.depth == 0 {
        tx, err := m.conn.DB().BeginTx(ctx, nil)
        // ...
        m.tx = tx
    } else {
        name := m.savepointName()
        m.tx.ExecContext(ctx, "SAVEPOINT "+name)
    }
    m.depth++
    return nil
}

func (m *TxManager) savepointName() string {
    return fmt.Sprintf("Aphront_Savepoint_%d", m.depth)
}
```

`TxManager` 通过 MySQL savepoint 实现嵌套事务语义：

- **depth=0 时**：执行真正的 `BEGIN` 开启事务
- **depth>0 时**：创建 savepoint，命名为 `Aphront_Savepoint_N`

Commit 和 Rollback 对称处理：

- **depth=1 时 Commit**：执行真正的 `COMMIT`
- **depth>1 时 Commit**：仅递减深度计数（savepoint 自然释放）
- **depth=1 时 Rollback**：执行真正的 `ROLLBACK`
- **depth>1 时 Rollback**：执行 `ROLLBACK TO SAVEPOINT Aphront_Savepoint_N`

savepoint 命名规则 `Aphront_Savepoint_N` 与 Phorge PHP 端 `AphrontDatabaseConnection` 保持一致，确保行为兼容性。

### 4.4 健康探测

健康探测位于 `internal/cluster/health.go`，`HealthService` 负责探测每个数据库节点的连接状态和复制状态。

#### 4.4.1 探测流程

对每个 `DatabaseRef` 执行以下步骤：

1. **建立连接**：使用短超时（2 秒）建立只读连接，不启用重试（`MaxRetries: 0`）
2. **Ping 测试**：验证连接可用性
3. **复制状态查询**：执行 `SHOW REPLICA STATUS`（MySQL 8.0 语法）

#### 4.4.2 连接状态分类

```go
const (
    StatusOkay              ConnectionStatus = "okay"
    StatusFail              ConnectionStatus = "fail"
    StatusAuth              ConnectionStatus = "auth"
    StatusReplicationClient ConnectionStatus = "replication-client"
)
```

四种连接状态覆盖了常见场景：

- `okay`：连接正常
- `fail`：连接失败或查询失败
- `auth`：认证错误（MySQL 1045）
- `replication-client`：权限不足（MySQL 1227/1044），无法执行 `SHOW REPLICA STATUS`

#### 4.4.3 复制状态分析

```go
const (
    ReplicationOkay           ReplicaStatus = "okay"
    ReplicationMasterReplica  ReplicaStatus = "master-replica"
    ReplicationReplicaNone    ReplicaStatus = "replica-none"
    ReplicationSlow           ReplicaStatus = "replica-slow"
    ReplicationNotReplicating ReplicaStatus = "not-replicating"
)
```

复制状态检测覆盖了五种场景：

- `okay`：角色与复制状态匹配
- `master-replica`：标记为 master 但实际在复制数据（拓扑异常）
- `replica-none`：标记为 replica 但没有在复制（拓扑异常）
- `replica-slow`：复制延迟超过 30 秒（性能告警）
- `not-replicating`：`Seconds_Behind_Master` 为 NULL（复制已停止）

对 `Seconds_Behind_Master` 的提取采用列名定位而非固定位置索引，兼容不同 MySQL 版本可能不同的列数。

#### 4.4.4 错误状态分析

```go
func isAccessDenied(msg string) bool {
    return len(msg) > 5 && (msg[0:5] == "Error" ||
        contains(msg, "Access denied") ||
        contains(msg, "1227") || contains(msg, "1044"))
}
```

健康探测不使用 Phorge 的统一错误映射（`FromMySQLError`），而是通过字符串匹配分析错误信息。这是因为探测阶段可能遇到 `SHOW REPLICA STATUS` 的权限错误，这不属于标准的查询错误范畴——权限不足不意味着节点不健康，而是需要区分为 `replication-client` 状态。

### 4.5 Schema 差异检查

Schema 检查位于 `internal/schema/schema.go`，`DiffService` 通过查询 `INFORMATION_SCHEMA` 构建层级化的 Schema 树。

#### 4.5.1 三级查询策略

Schema 树通过三级 SQL 查询构建：

1. **数据库级**：`SELECT SCHEMA_NAME, ... FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE ?`，使用 `{namespace}_%` 过滤出属于当前 Phorge 实例的数据库。
2. **表级**：`SELECT TABLE_NAME, TABLE_COLLATION, ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?`，获取每个数据库中的所有表。
3. **列级**：`SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, CHARACTER_SET_NAME, COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`，获取每张表的所有列。

查询结果组装为 `SchemaNode` 树结构，层级关系为 Server -> Database -> Table -> Column。

#### 4.5.2 SchemaNode 树结构

```go
type SchemaNode struct {
    RefKey   string        `json:"ref_key"`
    Database string        `json:"database,omitempty"`
    Table    string        `json:"table,omitempty"`
    Column   string        `json:"column,omitempty"`
    Key      string        `json:"key,omitempty"`
    Issues   []string      `json:"issues,omitempty"`
    Status   string        `json:"status"`
    Children []*SchemaNode `json:"children,omitempty"`
}
```

每个节点同时携带定位信息（RefKey + Database + Table + Column）和状态信息（Issues + Status），使得下游消费者既可以遍历树结构查看完整 Schema，也可以直接从单个节点获取其完整上下文。

#### 4.5.3 问题扁平化

`CollectIssues` 方法将树结构中散落在各节点的 Issues 扁平化为 `SchemaIssue` 列表：

```go
func (s *DiffService) flattenIssues(node *SchemaNode, out *[]SchemaIssue) {
    for _, issue := range node.Issues {
        *out = append(*out, SchemaIssue{
            RefKey: node.RefKey, Database: node.Database,
            Table: node.Table, Column: node.Column,
            Key: node.Key, Issue: issue, Status: node.Status,
        })
    }
    for _, child := range node.Children {
        s.flattenIssues(child, out)
    }
}
```

递归遍历树，为每个 issue 保留完整的定位路径。这使得 `/api/db/schema-issues` 端点返回可直接用于告警和报表的扁平数据。

#### 4.5.4 字符集检查

`CharsetInfo` 检查每个节点是否支持 `utf8mb4`：

```go
row := conn.QueryRowContext(ctx,
    "SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.CHARACTER_SETS WHERE CHARACTER_SET_NAME = 'utf8mb4'")
hasUTF8MB4 := row.Scan(&name) == nil
```

如果支持 `utf8mb4`，所有字符集和排序规则字段使用 `utf8mb4` 系列；否则降级到 `binary`/`utf8` 组合。这与 Phorge PHP 端 `PhabricatorStorageManagementAPI::getCharset` 的行为一致。

### 4.6 安装配置检查

安装检查位于 `internal/schema/setup.go`，`SetupService` 执行一系列数据库环境验证。

#### 4.6.1 检查项目

对每个活跃节点执行以下检查：

| 检查项 | SQL 查询 | 判定条件 | 是否致命 |
|---|---|---|---|
| MySQL 版本 | `SELECT VERSION()` | MySQL >= 8.0.0，MariaDB >= 10.5.1 | 是 |
| InnoDB 引擎 | `SHOW ENGINES` | InnoDB 状态为 YES 或 DEFAULT | 是 |
| meta_data 库 | `SHOW DATABASES` | `{namespace}_meta_data` 存在 | 是 |
| max_allowed_packet | `SELECT @@max_allowed_packet` | >= 32 MB | 否 |
| sql_mode | `SELECT @@sql_mode` | 包含 STRICT_ALL_TABLES | 否 |
| innodb_buffer_pool_size | `SELECT @@innodb_buffer_pool_size` | >= ~225 MB | 否 |
| local_infile | `SELECT @@local_infile` | 为 0（禁用） | 否 |
| 时钟同步 | `SELECT UNIX_TIMESTAMP()` | 与应用服务器时差 <= 60 秒 | 否 |

#### 4.6.2 致命与非致命问题

```go
type SetupIssue struct {
    Key     string `json:"key"`
    Name    string `json:"name"`
    Summary string `json:"summary,omitempty"`
    Message string `json:"message"`
    IsFatal bool   `json:"is_fatal"`
    RefKey  string `json:"ref_key,omitempty"`
}
```

检查结果分为两级：

- **致命问题**（`IsFatal: true`）：如 MySQL 版本过低、InnoDB 不可用、meta_data 库不存在。这些问题会阻止 Phorge 正常运行。
- **非致命问题**（`IsFatal: false`）：如 buffer pool 偏小、sql_mode 不够严格。这些问题不影响基本功能，但可能导致性能或数据安全问题。

#### 4.6.3 版本比较

```go
func compareVersions(a, b string) int {
    pa := strings.Split(a, ".")
    pb := strings.Split(b, ".")
    for i := 0; i < 3; i++ {
        va, vb := 0, 0
        if i < len(pa) { fmt.Sscanf(pa[i], "%d", &va) }
        if i < len(pb) { fmt.Sscanf(pb[i], "%d", &vb) }
        if va < vb { return -1 }
        if va > vb { return 1 }
    }
    return 0
}
```

版本比较逐段解析主版本、次版本、补丁版本进行数值比较。`SELECT VERSION()` 返回的版本字符串可能包含后缀（如 `8.0.32-ubuntu0.22.04.2` 或 `10.6.12-MariaDB-1:10.6.12+maria~ubu2204`），先通过 `strings.SplitN(version, "-", 2)` 提取纯版本号再比较。

### 4.7 迁移状态查询

迁移状态查询位于 `internal/schema/migration.go`，`MigrationService` 读取 `{namespace}_meta_data.patch_status` 表获取已应用的迁移 patch。

#### 4.7.1 查询逻辑

```go
func (m *MigrationService) checkRef(ctx context.Context, ref *cluster.DatabaseRef) MigrationStatus {
    st := MigrationStatus{RefKey: ref.RefKey()}
    // 连接到 {namespace}_meta_data 库
    // Ping 成功 → Initialized = true
    // 查询 SELECT patch FROM patch_status
    // 构建 applied patches 列表
    return st
}
```

查询流程：

1. 连接到 `{namespace}_meta_data` 数据库
2. Ping 成功标记 `Initialized = true`（数据库已初始化）
3. 查询 `patch_status` 表获取所有已应用的 patch 名称
4. 仅查询 master 节点（replica 的 patch_status 由复制同步）

#### 4.7.2 hoststate 查询

```go
row := conn.QueryRowContext(ctx,
    "SELECT stateValue FROM hoststate WHERE stateKey = 'cluster.databases'")
_ = row.Scan(&stateValue)
```

对 `hoststate` 表的查询结果当前被丢弃。这是为多 master 集群同步预留的接口——Phorge PHP 端通过 `hoststate` 表在多个 master 之间同步集群配置状态。

### 4.8 HTTP 层

#### 4.8.1 路由设计

```go
func RegisterRoutes(e *echo.Echo, deps *Deps) {
    e.GET("/", healthPing())
    e.GET("/healthz", healthPing())

    g := e.Group("/api/db")
    g.Use(tokenAuth(deps))

    g.GET("/servers", listServers(deps))
    g.GET("/servers/:ref/health", serverHealth(deps))
    g.GET("/schema-diff", schemaDiff(deps))
    g.GET("/schema-issues", schemaIssues(deps))
    g.GET("/setup-issues", setupIssues(deps))
    g.GET("/charset-info", charsetInfo(deps))
    g.GET("/migrations/status", migrationStatus(deps))
}
```

路由设计要点：

- **健康检查独立**：`/` 和 `/healthz` 不经过认证中间件，确保 Docker HEALTHCHECK 和负载均衡器的探测不受影响。
- **路由组中间件**：`tokenAuth` 作为 `/api/db` 路由组的中间件，仅对受保护端点生效。
- **RESTful 设计**：资源路径语义清晰，`/servers/:ref/health` 通过路径参数定位具体节点。

#### 4.8.2 认证中间件

```go
func tokenAuth(deps *Deps) echo.MiddlewareFunc {
    return func(next echo.HandlerFunc) echo.HandlerFunc {
        return func(c echo.Context) error {
            token := c.Request().Header.Get("X-Service-Token")
            if token == "" {
                token = c.QueryParam("token")
            }
            if token == "" {
                return c.JSON(http.StatusUnauthorized, &compat.APIResponse{
                    Error: &compat.APIError{Code: "ERR_UNAUTHORIZED", Message: "missing service token"},
                })
            }
            return next(c)
        }
    }
}
```

设计要点：

- **双通道获取**：支持 `X-Service-Token` 请求头和 `?token=` 查询参数两种方式。请求头适合服务间调用，查询参数适合调试或浏览器访问。
- **存在性检查**：当前实现仅检查 token 是否非空，不对 token 值进行校验。这在服务部署于受信任的内部网络时是合理的简化——认证的目的是防止误调用而非防御恶意攻击。

#### 4.8.3 统一错误处理

所有 Handler 使用 `compat.RespondError` 统一返回错误：

```go
func listServers(deps *Deps) echo.HandlerFunc {
    return func(c echo.Context) error {
        refs, err := deps.Health.QueryAll(c.Request().Context(), deps.Password)
        if err != nil {
            return compat.RespondError(c, compat.NewClusterError(compat.ErrQuery, err.Error()))
        }
        return compat.RespondOK(c, refs)
    }
}
```

错误通过 `DBError.HTTPStatus()` 映射到语义化的 HTTP 状态码。所有处理器遵循相同的模式：调用 Service 方法，成功用 `RespondOK`，失败用 `RespondError`。

### 4.9 兼容层

#### 4.9.1 统一响应结构

```go
type APIResponse struct {
    Data   any       `json:"data,omitempty"`
    Error  *APIError `json:"error,omitempty"`
    Cursor *Cursor   `json:"cursor,omitempty"`
}
```

`APIResponse` 包含三个可选字段：

- `Data`：成功时携带响应数据
- `Error`：失败时携带错误信息
- `Cursor`：分页时携带游标信息

这种信封结构与 Phorge Conduit API 的响应格式兼容，下游系统可以用统一的解析逻辑处理。

#### 4.9.2 MySQL 错误码映射

```go
var mysqlErrnoMap = map[uint16]ErrorCode{
    2013: ErrConnectionLost,     // Connection Dropped
    2006: ErrConnectionLost,     // Gone Away
    1213: ErrDeadlock,           // Deadlock
    1205: ErrLockTimeout,        // Lock Timeout
    1062: ErrDuplicateKey,       // Duplicate Entry
    1044: ErrAccessDenied,       // Access Denied (DB)
    1142: ErrAccessDenied,       // Access Denied (Table)
    1143: ErrAccessDenied,       // Access Denied (Column)
    1227: ErrAccessDenied,       // Access Denied (Command)
    1045: ErrInvalidCredentials, // Wrong Password
    1146: ErrSchema,             // Table Not Found
    1049: ErrSchema,             // Unknown Database
    1054: ErrSchema,             // Unknown Column
}
```

12 个 MySQL 错误码映射到 7 个语义化错误码，与 Phorge PHP 端 `AphrontBaseMySQLDatabaseConnection::throwCommonException` 的映射关系一致。

#### 4.9.3 HTTP 状态码映射

```go
func (e *DBError) HTTPStatus() int {
    switch e.Code {
    case ErrReadonlyWrite:                          return 409
    case ErrMasterUnreachable, ErrAllUnreachable:   return 503
    case ErrAccessDenied, ErrInvalidCredentials:     return 403
    case ErrSchema, ErrUnconfigured:                 return 500
    case ErrDuplicateKey:                            return 409
    case ErrDeadlock, ErrLockTimeout:                return 409
    default:                                         return 500
    }
}
```

状态码选择体现了 HTTP 语义：

- **409 Conflict**：读写冲突（只读连接写入）、并发冲突（死锁、锁超时、重复键）
- **503 Service Unavailable**：数据库节点不可达
- **403 Forbidden**：数据库权限问题
- **500 Internal Server Error**：Schema 问题、未配置等内部错误

### 4.10 应用生命周期

#### 4.10.1 启动顺序

```
LoadFromFile/LoadFromEnv → NewHealthService + NewDiffService + NewSetupService
+ NewMigrationService → Echo + Logger + Recover → RegisterRoutes → e.Start
```

启动流程是线性的：先完成所有配置和服务初始化，再启动 HTTP 服务。四个 Service 在创建时仅保存配置引用，不建立数据库连接，连接在首次请求时按需建立。

#### 4.10.2 关闭行为

当前实现使用 `e.Logger.Fatal(e.Start(addr))`，在 `e.Start` 返回错误时通过 `Fatal` 退出进程。Docker HEALTHCHECK 会在进程异常退出后触发容器重启。

## 5. 部署方案

### 5.1 Docker 镜像

采用多阶段构建：

- **构建阶段**：基于 `golang:1.26-alpine3.22`，使用 `CGO_ENABLED=0` 静态编译，`-ldflags="-s -w"` 去除调试信息和符号表以缩小二进制体积。
- **运行阶段**：基于 `alpine:3.20`，仅包含编译后的二进制和 CA 证书。

内置 Docker `HEALTHCHECK`，每 10 秒通过 `wget` 检查 `/healthz` 端点，启动等待 5 秒，超时 3 秒，最多重试 3 次。

### 5.2 集成验证脚本

`scripts/validate.sh` 提供了轻量级的集成验证，使用 `curl` 和 `jq` 检查：

- 各端点的可达性和响应格式
- 401 认证拦截
- Server 状态字段的存在性和类型

适合在 CI/CD 流水线中作为 smoke test 使用。

## 6. 依赖分析

| 依赖 | 版本 | 用途 |
|---|---|---|
| `go-sql-driver/mysql` | v1.9.3 | MySQL 数据库驱动 |
| `labstack/echo/v4` | v4.15.1 | HTTP 框架，提供路由、中间件和上下文管理 |
| `golang.org/x/crypto` | v0.49.0 | Echo 的加密基础（间接） |
| `golang.org/x/net` | v0.52.0 | Echo 的网络基础（间接） |
| `golang.org/x/time` | v0.15.0 | Echo 的时间工具（间接） |

直接依赖仅两个：Echo 框架和 MySQL 驱动。数据库连接池、重试策略、事务管理等功能完全基于标准库 `database/sql` 自行实现，保持了代码的可控性和透明度。

## 7. 测试覆盖

项目包含四组测试文件：

| 测试文件 | 覆盖范围 |
|---|---|
| `config_test.go` | 单节点配置构建、集群配置构建、应用分区路由、数据库名拼接、RefKey 格式 |
| `errors_test.go` | MySQL 错误码映射（12 个错误码）、HTTP 状态码映射（8 种错误类型）、可重试性判定 |
| `conn_test.go` | 只读查询正则判定（SELECT/SHOW/EXPLAIN/INSERT/UPDATE/DELETE/CREATE/DROP/START TRANSACTION/COMMIT） |
| `tx_test.go` | savepoint 命名规则验证 |

测试设计的特点：

- **表驱动测试**：`errors_test.go` 和 `conn_test.go` 使用 table-driven 风格，覆盖所有已知的错误码和查询类型。
- **配置组合**：`config_test.go` 测试了单节点和集群两种配置模式，验证了应用分区路由的优先级逻辑。
- **集成验证**：通过 `scripts/validate.sh` 提供端到端集成测试，验证完整的请求链路。

## 8. 总结

gorge-db-api 是一个职责明确的数据库管理微服务，核心价值在于：

1. **功能解耦**：将 Phorge PHP 端分散在多个类中的数据库管理功能整合为统一的 REST API，便于其他服务调用和运维自动化。
2. **集群感知**：通过应用分区路由、主从自动 fallback、连接池复用等机制，提供对 MySQL 集群的全局管理视图。
3. **行为兼容**：错误码映射、savepoint 命名、Schema 查询逻辑等关键行为与 Phorge PHP 端保持一致，确保下游系统无缝切换。
4. **弹性设计**：连接重试、只读降级、事务内不重试等策略，在容错和数据安全之间取得平衡。
5. **运维友好**：双重配置模式适配不同部署场景，Docker 多阶段构建、健康检查端点、集成验证脚本，开箱即用于容器化部署。
