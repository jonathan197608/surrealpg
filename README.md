---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: '51771223-cd8e-406f-b23a-96ee38e70c10'
  PropagateID: '51771223-cd8e-406f-b23a-96ee38e70c10'
  ReservedCode1: '86d7430e-57c7-4bc5-8751-a0787ab891c8'
  ReservedCode2: '86d7430e-57c7-4bc5-8751-a0787ab891c8'
---

# surreal-pg

**SurrealDB PostgreSQL 存储适配器** — 以 PostgreSQL 为底层存储引擎运行完整的 SurrealDB 服务器。

基于 SurrealDB 官方可插拔存储架构，通过委托模式包装 `CommunityComposer`，拦截 `postgresql://` 连接路径，将 SurrealDB 的 KV 操作映射到 PostgreSQL 的 B-tree + MVCC 之上。开箱即用 SurrealDB 全部能力：SurrealQL、ISO GQL（实验性）、GraphQL、HTTP、WebSocket、多模型（文档/图/向量/关系/时序）、权限系统。

## 目录

- [特色与优势](#特色与优势)
- [架构设计](#架构设计)
- [应用场景](#应用场景)
- [快速开始](#快速开始)
- [配置参数](#配置参数)
- [生产部署](#生产部署)
- [监控指标](#监控指标)
- [测试](TESTING.md)

---

## 特色与优势

### PostgreSQL 原生事务，完整 ACID

SurrealDB 的事务语义 1:1 映射到 PostgreSQL 的 `BEGIN` / `COMMIT` / `ROLLBACK` / `SAVEPOINT`，无需额外模拟。PG 的 MVCC 机制天然提供行级事务隔离和并发控制，适合对数据一致性要求严格的业务场景。

### 多后端自由切换

同一个 `surreal-pg` 二进制同时支持 PostgreSQL 和所有官方后端，通过连接字符串切换：

| 连接字符串 | 后端 |
|-----------|------|
| `postgresql://user:pass@host:5432/db` | PostgreSQL（本项目） |
| `rocksdb://data/surreal` | RocksDB |
| `tikv://host:2379` | TiKV |
| `memory://` | 内存 |

生产环境用 PostgreSQL 保证持久性和一致性，开发环境切到 `memory://` 零配置启动。

### 开箱即用的全栈能力

拿到的是完整的 SurrealDB 服务器——SurrealQL 查询引擎、GraphQL 自动生成（需 `DEFINE CONFIG GRAPHQL AUTO`）、ISO GQL 图模式查询（实验性，需 `--allow-experimental gql`）、HTTP/WebSocket API、多模型支持（文档/图/向量/关系/时序）、权限系统、事件触发器——只是底层存储换成了 PostgreSQL。

### Pooler 自动适配

连接池初始化时自动探测服务器是否在 pgbouncer / Supavisor 之后，动态切换 prepared statement 策略：直连 PG 使用 named prepared statement 获得最佳性能；检测到 transaction-mode pooler 时自动降级为 unnamed statement 保证兼容性。也可通过环境变量手动覆盖。探测策略采用双连接 named prepared statement 冲突检测——两个连接分别创建同名 prepared statement，若发生 `42P05`（duplicate_prepared_statement）则判定为 pooler。小连接池（≤ 2）跳过探测默认关闭，避免池耗尽。

### 5 层 29 参数精细化调优

内置分层调优系统，全部参数有合理默认值（零配置可用），也可通过 `PG_TUNED_*` 环境变量按需调整：

- **连接池层**：连接数、超时、生命周期
- **TCP keepalive 层**：空闲探测、探测间隔、失败计数
- **表存储层**：fillfactor、TOAST 策略、UNLOGGED 模式
- **Autovacuum 层**：死元组触发阈值、VACUUM 限流
- **查询运行时层**：超时防护、锁等待、统计精度
- **PG 服务器层**：work_mem、random_page_cost 等（session SET + postgresql.conf 建议）

### Rust 单二进制，零运行时依赖

纯 Rust 实现，编译为单一可执行文件，无 GC、无 JVM、无 Node.js 运行时。依赖 crates.io 发布的 `surrealdb-server` 和 `surrealdb-core`，升级路径清晰。

---

## 架构设计

### 委托模式

`PostgresComposer` 包装 `CommunityComposer`，仅拦截 `postgresql://` 和 `postgres://` 连接路径，将其路由到 `PgStore`。其他所有后端（`memory://`、`rocksdb://`、`tikv://` 等）透传给 community composer，行为与官方 SurrealDB 完全一致。

```
surreal-pg start postgresql://host:5432/db
    │
    ▼
PostgresComposer::new_transaction_builder(path)
    │
    ├── postgresql:// → PgStore::new() → TransactionBuilder
    │
    └── 其他 scheme → CommunityComposer (透传)
```

### 三层结构

| 层 | 结构体 | 文件 | 职责 |
|----|--------|------|------|
| 工厂层 | `PgStore` | `store.rs` | 持有连接池，创建事务，管理生命周期指标 |
| 事务层 | `PgTransaction` | `transaction.rs` | 底层 PG 事务，实现全部 KV 操作的 SQL 映射 |
| 适配层 | `PgTx` | `pg_tx.rs` | `Transactable` trait wrapper，用 `Mutex` 提供内部可变性 |

`PgStore` 返回 `Arc<Self>`，是 `Clone` 的（所有字段用 `Arc` 包装），`PgTx` 通过 `Mutex<Option<PgTransaction>>` 实现 `&self` 上的可变操作——满足 SurrealDB 的 `Transactable` trait 约束。

### KV → SQL 映射

SurrealDB 的 KV 层将所有数据视为字节对 `(Vec<u8>, Vec<u8>)`，存储在单张 PG 表中：

```sql
CREATE TABLE kv (key BYTEA PRIMARY KEY, val BYTEA NOT NULL);
```

| KV 操作 | SQL | 说明 |
|---------|-----|------|
| `set(k, v)` | `INSERT … ON CONFLICT DO UPDATE` | Upsert |
| `setm(pairs)` | `INSERT … SELECT * FROM UNNEST(…)` | 批量 upsert，自动分块（≤ 32,000 对/批） |
| `put(k, v)` | `INSERT … ON CONFLICT DO NOTHING` | Insert-if-absent，0 行受影响时返回 `KeyAlreadyExists` |
| `get(k)` | `SELECT val WHERE key = $1` | 单键查询 |
| `getm(keys)` | `SELECT key, val WHERE key = ANY($1)` | 批量查询，小结果集用线性扫描，大结果集用 HashMap |
| `del(k)` | `DELETE WHERE key = $1` | 单键删除 |
| `delr(range)` | `DELETE WHERE key >= $1 AND key < $2` | 范围删除 |
| `putc(k, v, chk)` | `UPDATE SET val = $2 WHERE key = $1 AND val = $3` | Compare-and-swap |
| `delc(k, chk)` | `DELETE WHERE key = $1 AND val = $2` | Compare-and-delete |
| `keys/scan(range)` | `SELECT key[/,val] WHERE key >= $1 AND key < $2 ORDER BY key [ASC\|DESC] LIMIT $3 OFFSET $4` | 范围扫描，支持正向/反向 |
| `count(range)` | `SELECT count(*) WHERE key >= $1 AND key < $2` | 精确计数 |
| `count_approx()` | `SELECT reltuples FROM pg_class WHERE relname = $1` | O(1) 近似计数（基于 ANALYZE 统计） |
| `exists(k)` | `SELECT 1 WHERE key = $1` | 存在性检查 |
| savepoint | `SAVEPOINT sp_N` / `ROLLBACK TO SAVEPOINT sp_N` / `RELEASE SAVEPOINT sp_N` | PG 原生 savepoint，名字栈式管理 |

所有 SQL 在 `PgStore::new()` 时预构建为 `Arc<Sql>`，每个事务通过 `Arc::clone` 共享（1 次原子引用计数），操作时零 `format!()` 堆分配。热路径利用 Rust 的"不同字段可同时借用"规则：`&self.sql.field`（不可变）与 `conn.deref_mut()`（可变）并存于同一作用域。

### 错误映射

PostgreSQL SQLSTATE 语义错误自动映射到 SurrealDB 的 `kvs::Error`：

| SQLSTATE | 含义 | 映射到 |
|----------|------|--------|
| `23505` | unique_violation | `TransactionKeyAlreadyExists` |
| `40P01` | deadlock_detected | `TransactionConflict` |
| `40001` | serialization_failure | `TransactionConflict` |
| `08xxx` | connection_exception | `ConnectionFailed` |
| `25P01` | no_active_sql_transaction | 保留上下文，映射为 `Transaction` |
| 其他 | — | `Transaction` |

### 连接泄漏恢复

从池中获取的连接可能残留前一个泄漏的事务状态。`begin()` 采用乐观策略——直接执行 `BEGIN`，仅在遇到 `25P02`（in_failed_sql_transaction）时才执行 `ROLLBACK` 清理并重试。这避免了正常路径的额外网络往返，同时安全处理异常情况。

---

## 应用场景

### 已有 PostgreSQL 基础设施团队的数据库选型

团队已有 PostgreSQL 运维经验、监控体系、备份策略，但需要 SurrealDB 的多模型能力（文档/图/向量）。用 `surreal-pg` 可以直接复用现有 PG 集群，无需引入新的存储引擎和运维负担。

### 需要强一致事务的多模型应用

SurrealDB 自带的 RocksDB 后端是嵌入式存储，无法跨节点共享。PostgreSQL 提供网络化的共享存储，多个应用实例可以同时连接同一个数据库，天然支持多实例部署下的数据一致性。

### Supabase / 云数据库上的 SurrealDB

直接连接 Supabase Pooler 或其他托管 PostgreSQL 服务，自动适配 transaction-mode pooler。无需自建数据库服务器，几分钟内获得 SurrealDB + PostgreSQL 的组合能力。

### 从 SurrealDB 原生后端平滑迁移

开发阶段用 `memory://` 快速验证，生产部署切到 `postgresql://` 获得持久化。同一个二进制、同一套 SurrealQL 语句，只改连接字符串即可完成后端切换。

### 图数据 + 关系数据混合查询

SurrealDB 的图关联查询能力（`RELATE` 语句）配合 PostgreSQL 的 ACID 事务保障，适合社交网络、知识图谱、推荐系统等需要图遍历同时又要求严格一致性的场景。

---

## 快速开始

### 构建

```bash
cargo build --release
```

### 启动

```bash
# 以 PostgreSQL 后端启动 SurrealDB 服务器
./target/release/surreal-pg start \
    --user root --pass secret \
    postgresql://user:pass@localhost:5432/surrealdb

# 连接到 Supabase Pooler（transaction mode 自动检测）
./target/release/surreal-pg start \
    --user root --pass secret \
    postgresql://user:pass@host.pooler.supabase.com:6543/postgres?sslmode=require
```

### 使用 SurrealQL

```bash
./target/release/surreal-pg sql -u root -p secret --namespace myapp --database myapp

> DEFINE TABLE person SCHEMAFULL;
> DEFINE FIELD name ON TABLE person TYPE string;
> DEFINE FIELD age ON TABLE person TYPE int;
> CREATE person SET name = 'Alice', age = 30;
> SELECT * FROM person WHERE name = 'Alice';
```

### 使用 GraphQL

GraphQL 已内置，无需 experimental flag。通过 `DEFINE CONFIG GRAPHQL AUTO` 启用后即可使用：

```bash
# 在 SurrealQL 中定义 schema 并开启 GraphQL
./target/release/surreal-pg sql -u root -p secret --namespace myapp --database myapp

> DEFINE TABLE person SCHEMAFULL;
> DEFINE FIELD name ON TABLE person TYPE string;
> DEFINE FIELD age ON TABLE person TYPE int;
> DEFINE CONFIG GRAPHQL AUTO;

# 新增记录（mutation）
curl -X POST -u "root:secret" \
  -H "Surreal-NS: myapp" -H "Surreal-DB: myapp" \
  -H "Content-Type: application/json" \
  -d '{"query": "mutation { createPerson(data: { name: \"Bob\", age: 10 }) { id name age } }"}' \
  http://localhost:8000/graphql

# 查询全部记录（复数形式）
curl -X POST -u "root:secret" \
  -H "Surreal-NS: myapp" -H "Surreal-DB: myapp" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ persons { id name age } }"}' \
  http://localhost:8000/graphql

# 根据 id 查询单条记录
curl -X POST -u "root:secret" \
  -H "Surreal-NS: myapp" -H "Surreal-DB: myapp" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ person(id: \"8ifuxq45p74ngnx27qny\") { id name age } }"}' \
  http://localhost:8000/graphql
```

### 使用 ISO GQL（实验性）

ISO GQL 是图模式查询语言（`MATCH … RETURN`），需要 `--allow-experimental gql` 启用：

```bash
# 启动时启用 ISO GQL 实验
./target/release/surreal-pg start --allow-experimental gql \
    --user root --pass secret \
    postgresql://user:pass@localhost:5432/surrealdb

# 通过 GQL 查询
curl -X POST -u "root:secret" \
  -H "Surreal-NS: myapp" -H "Surreal-DB: myapp" \
  -H "Content-Type: text/plain" \
  -d 'MATCH (n:person) RETURN n.name AS name ORDER BY name' \
  http://localhost:8000/gql
```

---

## 配置参数

项目提供两套配置系统：基础配置（`PgConfig`）和调优配置（`PgTuneConfig`）。

### 基础配置（URL 参数）

通过连接字符串的 query 参数传递，值支持 percent-decode（`%XX` 序列和 `+` → 空格）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_connections` | 20 | 连接池最大连接数（0 会被忽略） |
| `min_connections` | 5 | 连接池最小保持连接数（若 > max_connections 则被忽略） |
| `connect_timeout` | 10（秒） | 获取连接超时 |
| `idle_timeout` | 600（秒） | 空闲连接回收时间 |
| `max_lifetime` | 1800（秒） | 连接最大生存时间 |
| `slow_acquire_threshold_secs` | 2（秒） | 获取连接耗时超过此值时输出 WARN 日志（sqlx 默认 2s） |
| `slow_statements_threshold_secs` | 1（秒） | 单条 SQL 执行耗时超过此值时输出 WARN 日志（sqlx 默认 1s） |
| `auto_create_table` | true | 启动时自动建表 + 表调优 |
| `table_name` | `kv` | 表名（需符合 PG 标识符规则，拒绝 SQL 保留字） |
| `isolation_level` | `read_committed` | 事务隔离级别（`read_committed` / `repeatable_read` / `serializable`） |
| `persistent_statements` | `auto` | prepared statement 策略（`auto` / `true` / `false` / `on` / `off` / `yes` / `no` / `1` / `0`） |

示例：`postgresql://user:pass@host:5432/db?max_connections=30&isolation_level=serializable`

### 环境变量

| 环境变量 | 说明 |
|---------|------|
| `PG_PERSISTENT_STATEMENTS` | 覆盖 persistent statements 策略（`auto`/`true`/`false`/`on`/`off` 等） |

优先级规则：
- **persistent_statements**：`PG_PERSISTENT_STATEMENTS` 环境变量 > URL 参数 > 默认值 `auto`
- **pool 参数**（max/min_connections、timeouts）：URL 参数 > `PG_TUNED_*` 调优默认值
- **其他 URL 参数**：URL 参数 > 默认值；不识别的值打印 `warn` 并用默认值

### 调优配置（29 个参数，6 层）

所有调优参数通过 `PG_TUNED_*` 环境变量配置，全部有合理默认值，开箱即用。

#### PG 服务器级（8 个参数）

通过 `after_connect` 的 session `SET` 语句生效（部分仅在 `postgresql.conf` 中可设）。

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `PG_TUNED_SERVER_SHARED_BUFFERS` | `256MB` | 共享内存缓冲池（需 postgresql.conf） |
| `PG_TUNED_SERVER_WORK_MEM` | `64MB` | 单查询排序/哈希内存 |
| `PG_TUNED_SERVER_MAINTENANCE_WORK_MEM` | `256MB` | VACUUM/CREATE INDEX 内存 |
| `PG_TUNED_SERVER_WAL_BUFFERS` | `16MB` | WAL 写缓冲区（需 postgresql.conf） |
| `PG_TUNED_SERVER_MAX_CONNECTIONS` | `100` | PG 最大连接数（需 postgresql.conf） |
| `PG_TUNED_SERVER_EFFECTIVE_CACHE_SIZE` | `1GB` | 规划器可用系统缓存大小 |
| `PG_TUNED_SERVER_RANDOM_PAGE_COST` | `1.1` | 随机读代价（SSD 建议 1.1） |
| `PG_TUNED_SERVER_CHECKPOINT_TARGET` | `0.9` | 检查点完成目标（需 postgresql.conf） |

#### 连接池级（5 个参数）

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `PG_TUNED_POOL_MAX_CONNECTIONS` | `20` | 连接池最大连接数 |
| `PG_TUNED_POOL_MIN_CONNECTIONS` | `5` | 连接池最小保持连接数 |
| `PG_TUNED_POOL_ACQUIRE_TIMEOUT` | `10s` | 获取连接超时 |
| `PG_TUNED_POOL_IDLE_TIMEOUT` | `600s` | 空闲连接回收时间 |
| `PG_TUNED_POOL_MAX_LIFETIME` | `1800s` | 连接最大生存时间 |

#### TCP keepalive 级（3 个参数）

通过 `after_connect` 的 session `SET` 语句生效。TCP keepalive 让操作系统在连接空闲时主动发送探测包，及时发现被 Pooler/服务器静默断开的连接，避免"僵尸连接"导致池耗尽。

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `PG_TUNED_KEEPALIVE_IDLE` | `60s` | 空闲多久后开始探测（Supabase Pooler 通常 ~60s 回收空闲连接） |
| `PG_TUNED_KEEPALIVE_INTERVAL` | `10s` | 探测间隔 |
| `PG_TUNED_KEEPALIVE_COUNT` | `5` | 连续探测失败次数，达到后判定连接死亡。总检测时间 = idle + interval × count |

默认配置下，死亡连接在 60 + 10×5 = 110s 内被检测到。托管 PG（如 Supabase）可能忽略这些设置，但设置它们是安全的，对直连 PG 场景特别有效。

#### KV 表存储级（4 个参数）

通过 DDL `ALTER TABLE` 在建表时执行。

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `PG_TUNED_TABLE_FILLFACTOR` | `90` | 页填充率，留 10% 给 HOT update |
| `PG_TUNED_TABLE_TOAST_STORAGE` | `external` | 大值存储策略（external/extended/main/plain） |
| `PG_TUNED_TABLE_TOAST_THRESHOLD` | `2032` | TOAST 触发阈值（字节） |
| `PG_TUNED_TABLE_UNLOGGED` | `false` | 是否使用 UNLOGGED 表（跳过 WAL，崩溃丢数据） |

#### Autovacuum 级（5 个参数）

通过 DDL `ALTER TABLE SET (autovacuum_*)` 在建表时执行。

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `PG_TUNED_AUTOVAC_VACUUM_SCALE` | `0.05` | 死元组占比触发 VACUUM |
| `PG_TUNED_AUTOVAC_VACUUM_THRESHOLD` | `50` | 死元组绝对数量阈值 |
| `PG_TUNED_AUTOVAC_ANALYZE_SCALE` | `0.02` | 变更占比触发 ANALYZE |
| `PG_TUNED_AUTOVAC_VACUUM_COST_LIMIT` | `2000` | VACUUM IO 代价上限 |
| `PG_TUNED_AUTOVAC_VACUUM_COST_DELAY` | `1` | VACUUM IO 代价超限后睡眠（ms） |

#### 查询运行时级（4 个参数）

通过 `after_connect` 的 session `SET` 语句生效。

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `PG_TUNED_QUERY_STATEMENT_TIMEOUT` | `30s` | 单条 SQL 执行超时 |
| `PG_TUNED_QUERY_IDLE_TXN_TIMEOUT` | `60s` | 事务内空闲超时（防泄漏） |
| `PG_TUNED_QUERY_LOCK_TIMEOUT` | `10s` | 锁等待超时 |
| `PG_TUNED_QUERY_STATS_TARGET` | `500` | ANALYZE 采样精度 |

Duration 格式支持：`500ms`、`10s`、`5m`、`2h` 或裸数字（秒）。

### 场景化配置示例

```bash
# 开发环境（最小资源）
PG_TUNED_POOL_MAX_CONNECTIONS=5 \
PG_TUNED_POOL_MIN_CONNECTIONS=1 \
PG_TUNED_TABLE_UNLOGGED=true \
./target/release/surreal-pg start --user root --pass secret \
    postgresql://localhost:5432/surrealdb

# 生产环境（高吞吐读写混合）
PG_TUNED_POOL_MAX_CONNECTIONS=30 \
PG_TUNED_POOL_MIN_CONNECTIONS=10 \
PG_TUNED_TABLE_FILLFACTOR=85 \
PG_TUNED_AUTOVAC_VACUUM_SCALE=0.03 \
PG_TUNED_QUERY_STATEMENT_TIMEOUT=15s \
./target/release/surreal-pg start --user root --pass secret \
    postgresql://prod-pg:5432/surrealdb

# Supabase / 云 Pooler（跨 region 部署）
PG_TUNED_POOL_ACQUIRE_TIMEOUT=30s \
PG_TUNED_POOL_MIN_CONNECTIONS=5 \
./target/release/surreal-pg start --user root --pass secret \
    --query-timeout 5m \
    postgresql://user:pass@host.pooler.supabase.com:6543/postgres?sslmode=require&min_connections=5&connect_timeout=30&slow_acquire_threshold_secs=10&slow_statements_threshold_secs=5
```

### Supabase / 云 Pooler 场景配置指南

Supabase Pooler（基于 PgBouncer/Supavisor）在 transaction 模式下，连接不是 1:1 映射到后端 PG 进程，而是通过池复用。跨 region 部署时（例如应用在亚洲、Supabase 在美东），新建连接的延迟可达 2-3 秒。这会导致以下连锁告警：

```
① query exceeded the timeout: 1m
   → ② Timed out updating node registration after 60s
      → ③ connection pool timeout
```

**根因**：连接池 `acquire_timeout`（默认 10s）在高并发下不够用，任务排队等不到连接，进而触发上层超时。节点注册的 60s 超时是 SurrealDB 硬编码的，不可配置，但如果连接池不超时，节点注册通常在毫秒级完成，远不会到 60s。

#### 常见告警与应对

**告警 1：`acquired connection, but time to acquire exceeded slow threshold`**

```
sqlx::pool::acquire: aquired_after_secs=6.4 slow_acquire_threshold_secs=2.0
```

含义：连接池获取连接耗时超过告警阈值（默认 2s），但最终获取成功。说明池子繁忙，新连接需要排队或冷启动建连。

- 若偶尔出现：正常，跨 region 建连本身就慢
- 若频繁出现：池容量不足，考虑加大 `max_connections` 或提高 `min_connections`
- 若想调高告警阈值减少日志噪音：`slow_acquire_threshold_secs=10`（URL 参数）

**告警 2：`slow statement: execution time exceeded alert threshold`**

```
sqlx::query: elapsed=1.36s slow_threshold=1s
```

含义：单条 SQL 执行耗时超过告警阈值（默认 1s）。对于简单的 KV 查询（如 `SELECT val FROM kv WHERE key = $1`），正常执行在毫秒级，1.36s 说明**不是 SQL 本身慢，而是等连接**——连接获取的耗时被计入 SQL 执行计时。

- 根治：加大 `min_connections` 保持热连接，减少冷启动
- 调高告警阈值减少日志噪音：`slow_statements_threshold_secs=5`（URL 参数）

**告警 3：`connection pool timeout`**

含义：连接池 `acquire_timeout` 内无法获取连接，事务直接失败。这是最严重的告警。

- 必须加大 `acquire_timeout`（推荐 30s）
- 同时加大 `min_connections` 减少冷启动建连频率

**告警 4：`ZOMBIE POOL detected`**

```
pool_size=20 pool_idle=0 pool_max=20 tx_active=0 error=connection pool timeout
```

含义：连接池中有 N 个连接（`pool_size=N`），但 0 个空闲（`pool_idle=0`），同时代码中 0 个活跃事务（`tx_active=0`）。这是一个矛盾——如果没有代码持有连接，连接应该回到池中成为 idle。说明所有连接被 sqlx 内部持有（正在重建/健康检查中），不是代码泄漏。

- **根因**：Supabase Pooler 在服务器端静默回收了空闲连接，sqlx 不知道连接已死，等到要用时才发现，然后所有连接同时进入重建状态
- **防御 1**：TCP keepalive（已内置，默认 idle=60s/interval=10s/count=5），让 OS 主动检测断开
- **防御 2**：`before_acquire` 条件式 ping（已内置），空闲超过 60s 的连接获取前先 ping
- **应急**：若池完全卡死，重启进程；加大 `min_connections=5` 保持更多热连接

#### 必须调整的参数

| 参数 | 推荐值 | 原因 |
|------|--------|------|
| `PG_TUNED_POOL_ACQUIRE_TIMEOUT` | `30s` | 跨 region 连接建立慢（2-3s/连接），高并发时 10s 不够排队 |
| `min_connections`（URL 或 `PG_TUNED_POOL_MIN_CONNECTIONS`） | `5` | 保持更多热连接，减少冷启动建连频率 |

#### 建议调整的参数

| 参数 | 推荐值 | 原因 |
|------|--------|------|
| `--query-timeout`（SurrealDB CLI） | `5m` | 节点注册和内部操作可能因连接池排队耗时较长，1m 可能不够 |
| `connect_timeout`（URL 参数） | `30` | 与 acquire_timeout 对齐，避免 URL 层的连接超时先于池超时 |
| `slow_acquire_threshold_secs`（URL 参数） | `10` | 跨 region 正常建连 2-3s，默认 2s 阈值会频繁误报 |
| `slow_statements_threshold_secs`（URL 参数） | `5` | 含连接获取耗时的 SQL 执行时间不可控，默认 1s 阈值对跨 region 场景过低 |
| `PG_TUNED_POOL_IDLE_TIMEOUT` | `300s` | 减少空闲回收时间，加速僵尸连接的淘汰（默认 600s 偏保守） |

#### 完整启动命令

```bash
PG_TUNED_POOL_ACQUIRE_TIMEOUT=30s \
PG_TUNED_POOL_MIN_CONNECTIONS=5 \
PG_TUNED_POOL_IDLE_TIMEOUT=300s \
./target/release/surreal-pg start \
    --user root --pass secret \
    --query-timeout 5m \
    postgresql://user:pass@host.pooler.supabase.com:6543/postgres?sslmode=require&min_connections=5&connect_timeout=30&slow_acquire_threshold_secs=10&slow_statements_threshold_secs=5
```

#### 超时层次对照表

理解超时层次关系有助于排查问题：外层超时应 >= 内层超时之和，否则内层还未完成就被外层取消。

| 层次 | 超时 | 默认值 | 可调？ | 说明 |
|------|------|--------|--------|------|
| 0. TCP keepalive | 探测空闲 | 60s | `PG_TUNED_KEEPALIVE_IDLE` | OS 层面检测死连接 |
| 0. TCP keepalive | 探测间隔 | 10s | `PG_TUNED_KEEPALIVE_INTERVAL` | 两次探测的时间间隔 |
| 0. TCP keepalive | 失败判定 | 5 次 | `PG_TUNED_KEEPALIVE_COUNT` | 连续失败次数，总检测时间 = idle + interval × count |
| 1. PG 服务器 | `statement_timeout` | 30s | `PG_TUNED_QUERY_STATEMENT_TIMEOUT` | 单条 SQL 执行上限 |
| 2. PG 服务器 | `lock_timeout` | 10s | `PG_TUNED_QUERY_LOCK_TIMEOUT` | 等锁超时 |
| 3. 连接池 | `acquire_timeout` | 10s | `PG_TUNED_POOL_ACQUIRE_TIMEOUT` | **推荐 30s** |
| 4. SurrealDB | `query-timeout` | 无限制 | `--query-timeout` / `SURREAL_QUERY_TIMEOUT` | 查询总执行上限 |
| 5. SurrealDB | 节点注册超时 | 60s | **不可配置** | 硬编码常量 |
| 6. SurrealDB | 事务超时 | 无限制 | `--transaction-timeout` / `SURREAL_TRANSACTION_TIMEOUT` | 事务总执行上限 |

推荐关系：`lock_timeout ≤ statement_timeout < acquire_timeout ≤ query-timeout`

#### 告警阈值对照表

告警阈值不影响功能，只控制日志输出。阈值过高会漏报真正的性能问题，过低则产生大量误报警报噪音。跨 region 场景下正常延迟基线更高，需适当调高。

| 告警 | 阈值参数 | 默认值 | 跨 region 推荐值 | 说明 |
|------|---------|--------|-----------------|------|
| 连接获取慢 | `slow_acquire_threshold_secs` | 2s | 10s | 跨 region 建连 2-3s 属正常 |
| SQL 执行慢 | `slow_statements_threshold_secs` | 1s | 5s | 含连接获取耗时，1s 阈值对跨 region过低 |

---

## 生产部署

### PostgreSQL 配置建议

在 `postgresql.conf` 中设置（需重启）：

```ini
shared_buffers = 4GB              # 物理内存的 25%
work_mem = 64MB
maintenance_work_mem = 256MB
wal_buffers = 16MB
max_connections = 100
random_page_cost = 1.1            # SSD
checkpoint_completion_target = 0.9
```

### 连接数计算

```
推荐 pool max = min(CPU核心数 * 2 + 磁盘数, PG max_connections - 10)
```

示例：8 核 CPU + 1 SSD → min(17, 90) → 取 20。

### PostgreSQL 内部监控

| 指标 | 查询方式 | 健康阈值 |
|------|---------|---------|
| 缓存命中率 | `pg_stat_database.blks_hit / (blks_hit + blks_read)` | > 99% |
| 死元组比例 | `n_dead_tup / n_live_tup` | < 5% |
| HOT 更新比例 | `n_tup_hot_upd / n_tup_upd` | > 90% |
| 连接池使用率 | `active / max_connections` | 60%~80% |

---

## 监控指标

`PgStore` 通过 SurrealDB 的 `Metrics` 接口暴露以下内置指标，可通过 `register_metrics()` / `collect_u64_metric()` 查询：

| 指标名 | 说明 |
|--------|------|
| `pg_pool_size` | 当前连接池总连接数（含空闲） |
| `pg_pool_idle` | 空闲连接数 |
| `pg_pool_max` | 连接池最大连接数 |
| `pg_tx_started` | 累计启动的事务数 |
| `pg_tx_committed` | 累计提交的事务数 |
| `pg_tx_rolled_back` | 累计回滚/取消的事务数 |
| `pg_tx_active` | 当前活跃事务数（连接被占用的数量） |

指标恒等式：`tx_started = tx_committed + tx_rolled_back`（commit/cancel 失败也计入 rolled_back）。

`pg_tx_active` 与 `pg_pool_size - pg_pool_idle` 的差值可以反映连接池状态：
- **正常**：`tx_active ≈ pool_size - pool_idle`（所有非空闲连接都在事务中）
- **僵尸池**：`tx_active = 0` 但 `pool_idle = 0`（所有连接被 sqlx 内部持有，非代码泄漏）

连接池利用率超过 80% 时自动输出一次性 `warn` 日志，避免日志刷屏。

### 定期维护

```bash
# 通过 PgStore API 执行 VACUUM（需在事务外调用）
# 或通过 PG cron 定期执行：
VACUUM ANALYZE kv;
```

---

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| `surrealdb-server` | ^3.2 | SurrealDB 服务器引擎（init / CLI / HTTP / WS） |
| `surrealdb-core` | ^3.2 | 核心库（CommunityComposer / kvs traits） |
| `sqlx` | 0.8 | PostgreSQL 异步驱动 + 连接池 |
| `tokio` | 1 | 异步运行时 |
| `tokio-util` | 0.7 | CancellationToken（优雅关停） |
| `thiserror` | 2 | 错误类型派生 |
| `tracing` | 0.1 | 结构化日志 |
| `axum` | 0.8 | HTTP 框架（RouterFactory 返回类型） |

**Edition**: Rust 2024。依赖 `surrealdb-server` / `surrealdb-core` 版本范围为 `^3.2`，升级时需运行完整测试套件验证兼容性。

---

## 许可证

Apache-2.0，详见 [LICENSE](LICENSE)。