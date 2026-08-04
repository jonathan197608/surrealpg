---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: 'efe0b39d-883f-4b8c-821e-875e945d8e90'
  PropagateID: 'efe0b39d-883f-4b8c-821e-875e945d8e90'
  ReservedCode1: '2ae2478f-16d9-4398-b90c-ae8b5f390cfc'
  ReservedCode2: '2ae2478f-16d9-4398-b90c-ae8b5f390cfc'
---

# 测试文档

## 测试分层

| 层级 | 文件 | 测试数 | 说明 |
|------|------|--------|------|
| 单元测试 | `src/config.rs` | 13 | URL 参数解析、percent-decode、标识符校验、SQL 保留字检测 |
| 单元测试 | `src/tune.rs` | 9 | PgTuneConfig 参数解析、SQL 生成、Duration 解析、checkpoint_target clamp |
| 集成测试 | `tests/integration_test.rs` | 19 | 直接测试 KV 层（PgStore / PgTransaction），不经过 SurrealDB 引擎 |
| SurrealQL 测试 | `tests/surreal_kv_suite.rs` | 2 | 通过 SurrealDB Datastore API 端到端验证 SurrealQL 语句 |

**合计 43 个测试**，22 个单元测试无需数据库即可运行，集成测试和 SurrealQL 测试需要 `PG_TEST_URL` 环境变量。

---

## 单元测试

无需数据库连接，验证配置解析与调优参数的正确性。

### `src/config.rs`（13 个）

| 测试名 | 验证内容 |
|--------|---------|
| `test_validate_identifier_valid` | 合法标识符通过（`kv`、`kv_test`、`_kv`、`Mixed_Case123`） |
| `test_validate_identifier_empty` | 空字符串被拒绝 |
| `test_validate_identifier_leading_digit` | 首字符为数字被拒绝（PG 标识符规则） |
| `test_validate_identifier_invalid_chars` | 含 `-`、`.`、空格、`;`、`'` 等非法字符被拒绝 |
| `test_validate_identifier_reserved` | SQL 保留字被拒绝（含 SAVEPOINT 回归测试） |
| `test_is_sql_reserved_case_insensitive` | 保留字检测支持大写/小写/混合大小写 |
| `test_percent_decode_normal` | `%20` → 空格、`%2F` → `/` 等标准解码 |
| `test_percent_decode_consecutive` | 连续 `%XX` 序列正确解码 |
| `test_percent_decode_invalid` | 无效 hex 序列原样保留 |
| `test_percent_decode_trailing_percent` | 尾部 `%` 或 `%2` 不完整序列原样保留 |
| `test_percent_decode_empty` | 空字符串输入返回空字符串 |
| `test_percent_decode_plus` | `+` → 空格（form-urlencoded 兼容） |
| `test_hex_digit` | 十六进制字符转数值（0-9, a-f, A-F） |

### `src/tune.rs`（9 个）

| 测试名 | 验证内容 |
|--------|---------|
| `test_defaults` | 所有 26 个参数的默认值是否正确 |
| `test_parse_duration` | Duration 解析支持 `500ms`、`10s`、`5m`、`2h` 及裸数字（秒） |
| `test_session_sql_contains_all_params` | session SQL 包含所有运行时级参数的 SET 语句 |
| `test_create_table_sql` | 建表 SQL 包含 fillfactor、TOAST、UNLOGGED 等表存储参数 |
| `test_tune_table_sql` | ALTER TABLE SQL 包含 autovacuum 参数 |
| `test_validate_pg_memory_size` | PG 内存大小校验（`64MB`、`1GB` 等，拒绝 SQL 注入） |
| `test_validate_toast_storage` | TOAST 存储策略校验（external/extended/main/plain） |
| `test_env_bool` | 环境变量布尔值解析（true/false/1/0/yes/no/on/off，含大小写） |
| `test_checkpoint_target_clamping` | checkpoint_completion_target 超范围 clamp 到 [0.0, 1.0] |

运行：

```bash
# 全部单元测试
cargo test --lib

# 仅 config 模块
cargo test --lib config

# 仅 tune 模块
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
| `test_count_approx` | count_approx 近似计数（pg_class.reltuples） |
| `test_health_check` | health_check（SELECT 1）健康检查 |
| `test_pool_size` | pool_size / pool_max 连接池信息读取 |
| `test_setm` | setm 批量写入 + upsert 更新 + 空输入 no-op |
| `test_vacuum` | VACUUM ANALYZE 执行 |
| `test_try_resize_pool` | try_resize_pool 参数校验（max < min 拒绝） |
| `test_empty_range` | 空范围（start >= end）的 keys/count/scan/delr 返回空 |
| `test_nested_savepoint` | 两层嵌套 savepoint 回滚，验证栈式管理 |
| `test_setm_delc_combo` | setm 批量写入 + delc 条件删除组合操作 |

集成测试运行结束后会写入一条 `test:marker` 记录，包含通过/失败数和时间戳，可用于验证数据确实到达 PG。

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
# clippy 零告警（每次修改代码后必须执行，-D warnings 将告警视为错误）
cargo clippy --all-targets -- -D warnings
```

## Rust 2024 注意事项

项目使用 Rust edition 2024。在该 edition 下，`std::env::set_var` 和 `std::env::remove_var` 是 unsafe 操作（因为它们涉及全局可变状态），测试中需用 `unsafe { }` 块包裹：

```rust
// 正确写法（edition 2024）
unsafe { std::env::set_var("PG_TUNED_SERVER_CHECKPOINT_TARGET", "1.5") };
unsafe { std::env::remove_var("PG_TUNED_SERVER_CHECKPOINT_TARGET") };
```

`src/tune.rs` 的 `test_env_bool` 和 `test_checkpoint_target_clamping` 测试涉及环境变量操作，均已按此规则处理。