---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: '52f0e174-8741-4289-811d-40962fc77b5f'
  PropagateID: '52f0e174-8741-4289-811d-40962fc77b5f'
  ReservedCode1: 'ebfdbea3-d0fa-4893-adb6-ecac15754a8d'
  ReservedCode2: 'ebfdbea3-d0fa-4893-adb6-ecac15754a8d'
---

# 测试文档

## 测试分层

| 层级 | 文件 | 测试数 | 说明 |
|------|------|--------|------|
| 单元测试 | `src/tune.rs` | 5 | PgTuneConfig 的参数解析、SQL 生成、Duration 解析 |
| 集成测试 | `tests/integration_test.rs` | 10 | 直接测试 KV 层（PgStore / PgTransaction），不经过 SurrealDB 引擎 |
| SurrealQL 测试 | `tests/surreal_kv_suite.rs` | 2 | 通过 SurrealDB Datastore API 端到端验证 SurrealQL 语句 |

**合计 17 个测试**，单元测试无需数据库即可运行，集成测试和 SurrealQL 测试需要 `PG_TEST_URL` 环境变量。

---

## 单元测试（src/tune.rs）

无需数据库连接，验证调优配置的正确性。

| 测试名 | 验证内容 |
|--------|---------|
| `test_defaults` | 所有 26 个参数的默认值是否正确 |
| `test_parse_duration` | Duration 解析支持 `500ms`、`10s`、`5m`、`2h` 及裸数字（秒） |
| `test_session_sql_contains_all_params` | session SQL 包含所有运行时级参数的 SET 语句 |
| `test_create_table_sql` | 建表 SQL 包含 fillfactor、TOAST、UNLOGGED 等表存储参数 |
| `test_tune_table_sql` | ALTER TABLE SQL 包含 autovacuum 参数 |

运行：

```bash
cargo test --lib tune
```

---

## 集成测试（tests/integration_test.rs）

直接调用 `PgStore` / `PgTransaction` 的原生 API，覆盖 KV 层全部操作。需要 `PG_TEST_URL` 环境变量，未设置时自动跳过。

| 测试名 | 验证内容 |
|--------|---------|
| `test_basic_crud` | set / get / del 基本读写删除 |
| `test_put` | put（insert-if-absent），已存在 key 应失败 |
| `test_range_scan_and_delete` | scan 范围查询 + count + delr 范围删除 |
| `test_savepoint_rollback` | savepoint 创建 + rollback_to_save_point 回滚 |
| `test_putc` | putc（compare-and-swap），匹配/不匹配 check 值 |
| `test_namespace_isolation` | 带命名空间前缀的 key 隔离与范围扫描 |
| `test_exists_and_getm` | exists 存在性检查 + getm 批量获取 |
| `test_delc` | delc（compare-and-delete），匹配/不匹配 check 值 |
| `test_keys_direction` | keys 正序 / keysr 反序扫描 |
| `test_read_only_rejects_writes` | 只读事务拒绝写操作 |

---

## SurrealQL 测试（tests/surreal_kv_suite.rs）

通过 SurrealDB 的 `Datastore` API 构建完整的 SurrealDB 引擎，验证 SurrealQL 语句端到端正确性。需要 `PG_TEST_URL` 环境变量，未设置时自动跳过。

| 测试名 | 验证内容 |
|--------|---------|
| `surreal_kv_basic_crud` | DEFINE NAMESPACE/DATABASE/TABLE → CREATE → SELECT → UPDATE → DELETE 全流程 |
| `surreal_kv_transaction_rollback` | `BEGIN; CREATE; CANCEL;` 事务回滚后数据不可见 |

---

## 运行测试

### 前置条件

设置 `PG_TEST_URL` 环境变量指向测试数据库。测试自动追加 `table_name=kv_test` 参数，使用 `kv_test` 表隔离测试数据，不会影响生产表 `kv`。

```bash
# 本地 PostgreSQL（直连，端口 5432）
export PG_TEST_URL='postgresql://user:pass@localhost:5432/postgres'

# Supabase Pooler（transaction mode，端口 6543）
export PG_TEST_URL='postgresql://user:pass@host.pooler.supabase.com:6543/postgres?sslmode=require&min_connections=0'
```

> **注意**：Supabase Pooler 连接池有超时限制，URL 中务必附加 `min_connections=0` 参数，否则空闲连接会被 Pooler 断开导致 `PoolTimeout` 错误。

### 运行命令

```bash
# 全部测试（串行执行，避免 Supabase Pooler 连接池超时）
cargo test -- --test-threads=1

# 仅单元测试（无需数据库）
cargo test --lib

# 仅集成测试
cargo test --test integration_test -- --test-threads=1

# 仅 SurrealQL 测试
cargo test --test surreal_kv_suite -- --test-threads=1
```

> **串行执行**：连接 Supabase Pooler（Supavisor, transaction pool mode）时，并发测试会触发连接池超时。务必使用 `-- --test-threads=1` 串行执行。

### Persistent Statements 自动检测

集成测试启动时会通过 `PgStore::probe_persistent()` 自动探测连接环境：

- **直连 PG（端口 5432）** → `true`（使用 named prepared statement，最佳性能）
- **Supabase Pooler（端口 6543）** → `false`（使用 unnamed statement，兼容 transaction-mode pooler）

如需查看检测结果，使用 `--nocapture` 放开 stdout 捕获：

```bash
cargo test --test integration_test -- --nocapture --test-threads=1
```

也可通过环境变量 `PG_PERSISTENT_STATEMENTS` 手动覆盖（`auto`/`true`/`false`）。

---

## macOS 构建注意事项

macOS 上 Homebrew 安装的 LLVM clang 配置会硬编码 `-isysroot` 指向 Command Line Tools（CLT）的 SDK，而该 SDK 的 `usr/include/` 为空目录，导致 bindgen（被 `rquickjs-sys` 等 crate 使用）无法找到 `stdio.h` 等 C 标准头文件，编译失败。

项目已在 `.cargo/config.toml` 中配置修复，将 `-isysroot` 指向完整版 Xcode SDK：

```toml
[env]
BINDGEN_EXTRA_CLANG_ARGS = "-isysroot /Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"
```

**前提条件**：需安装完整版 Xcode（而非仅 Command Line Tools）。如果 Xcode 安装路径不同，请相应修改 SDK 路径。

---

## 代码质量检查

```bash
# clippy 零告警（每次修改代码后必须执行）
cargo clippy --all-targets
```