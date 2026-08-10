---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: '6f19e4e3-09c1-4a0b-aadf-a628c5b90f72'
  PropagateID: '6f19e4e3-09c1-4a0b-aadf-a628c5b90f72'
  ReservedCode1: 'b1413b04-1945-47c5-ae64-1337e5407f32'
  ReservedCode2: 'b1413b04-1945-47c5-ae64-1337e5407f32'
---

# 测试文档

## 测试分层

| 层级 | 文件 | 测试数 | 说明 |
|------|------|--------|------|
| 单元测试 | `src/config.rs` | 23 | URL 参数解析、percent-decode、标识符校验、SQL 保留字检测、persistent_statements 解析、参数同义词、超时/阈值零值拒绝 |
| 单元测试 | `src/tune.rs` | 41 | PgTuneConfig 参数解析、SQL 生成（DDL/ALTER/session）、Duration 解析、Hash 分区、分区表处理、各种边界值校验 |
| 单元测试 | `src/store.rs` | 15 | URL 参数剥离、分区验证逻辑 |
| 单元测试 | `src/composer.rs` | 12 | URL 脱敏（IPv4/IPv6/带查询参数/无用户信息等） |
| 单元测试 | `src/transaction.rs` | 9 | setm 去重逻辑、getm HashMap 路径 |
| 单元测试 | `src/error.rs` | 3 | transient 错误检测、Tls/Protocol 映射 |
| 集成测试 | `tests/integration_test.rs` | 19 | 直接测试 KV 层（PgStore / PgTransaction），不经过 SurrealDB 引擎 |
| SurrealQL 测试 | `tests/surreal_kv_suite.rs` | 2 | 通过 SurrealDB Datastore API 端到端验证 SurrealQL 语句 |

**合计 124 个测试**（103 个单元测试 + 19 个集成测试 + 2 个 SurrealQL 测试）。103 个单元测试无需数据库即可运行，集成测试和 SurrealQL 测试需要 `PG_TEST_URL` 环境变量。

---

## 单元测试

无需数据库连接，验证配置解析与调优参数的正确性。

### `src/config.rs`（23 个）

| 测试名 | 验证内容 |
|--------|---------|
| `test_validate_identifier_valid` | 合法标识符通过（`kv`、`kv_test`、`_kv`、`Mixed_Case123`） |
| `test_validate_identifier_empty` | 空字符串被拒绝 |
| `test_validate_identifier_leading_digit` | 首字符为数字被拒绝（PG 标识符规则） |
| `test_validate_identifier_invalid_chars` | 含 `-`、`.`、空格、`;`、`'` 等非法字符被拒绝 |
| `test_validate_identifier_reserved` | SQL 保留字被拒绝（含 SAVEPOINT 回归测试） |
| `test_validate_identifier_too_long` | 超过 PG 63 字节标识符限制被拒绝 |
| `test_is_sql_reserved_case_insensitive` | 保留字检测支持大写/小写/混合大小写 |
| `test_percent_decode_normal` | `%20` → 空格、`%2F` → `/` 等标准解码 |
| `test_percent_decode_consecutive` | 连续 `%XX` 序列正确解码 |
| `test_percent_decode_invalid` | 无效 hex 序列原样保留 |
| `test_percent_decode_trailing_percent` | 尾部 `%` 或 `%2` 不完整序列原样保留 |
| `test_percent_decode_empty` | 空字符串输入返回空字符串 |
| `test_percent_decode_plus` | `+` → 空格（form-urlencoded 兼容） |
| `test_percent_decode_multibyte_utf8` | 多字节 UTF-8 percent-decode（如中文） |
| `test_hex_digit` | 十六进制字符转数值（0-9, a-f, A-F） |
| `test_persistent_statements_parse_synonyms` | `persistent_statements` URL 参数同义词（`on`/`off`/`1`/`0`/`true`/`false`） |
| `test_merge_url_params_fragment` | URL 参数合并保留 fragment |
| `test_merge_url_params_pooler_synonyms` | `pooler` 参数同义词合并 |
| `test_min_connections_before_max_cross_validation` | `min_connections > max_connections` 交叉校验 |
| `test_parse_bool_param_synonyms` | 布尔参数同义词解析（`true`/`yes`/`on`/`1`/`false`/`no`/`off`/`0`） |
| `test_timeout_zero_rejected` | `connect_timeout=0` 被拒绝（零值无效） |
| `test_slow_threshold_zero_rejected` | `slow_acquire_threshold_secs=0` 等阈值零值被拒绝 |
| `test_hash_partitions_url_param` | URL 参数 `hash_partitions` 解析 |

### `src/tune.rs`（41 个）

| 测试名 | 验证内容 |
|--------|---------|
| `test_defaults` | 所有参数的默认值是否正确 |
| `test_parse_duration` | Duration 解析支持 `500ms`、`10s`、`5m`、`2h` 及裸数字（秒） |
| `test_session_sql_contains_all_params` | session SQL 包含所有运行时级参数的 SET 语句 |
| `test_session_sql_subsecond_duration` | sub-second Duration 正确格式化为毫秒（如 500ms → `500ms`，非截断为 0s） |
| `test_session_sql_default_uses_seconds` | 默认 Duration（整秒）格式化为秒单位 |
| `test_create_table_sql` | 建表 SQL 包含 fillfactor、TOAST、UNLOGGED 等表存储参数 |
| `test_create_table_sql_no_partition` | 非分区模式建表 SQL 不含 PARTITION BY |
| `test_create_table_sql_with_partitions` | Hash 分区建表 SQL 包含 PARTITION BY HASH + 子分区定义，语句间有分号分隔 |
| `test_create_table_sql_unlogged_with_partitions` | UNLOGGED + Hash 分区组合 |
| `test_create_table_sql_partition_name_at_limit` | 分区子表名刚好在 PG 63 字节限制内 |
| `test_create_table_sql_partition_name_too_long` | 分区子表名超 PG 63 字节限制被拒绝 |
| `test_tune_table_sql` | ALTER TABLE SQL 包含 autovacuum 参数 |
| `test_tune_table_sql_on_partitioned_table` | 分区父表 ALTER 只含 SET STORAGE（PG 13+ 不允许分区表 SET storage parameters） |
| `test_tune_table_sql_rejects_bad_fillfactor` | fillfactor 越界被拒绝 |
| `test_tune_table_sql_rejects_bad_toast` | 无效 TOAST 策略被拒绝 |
| `test_tune_table_sql_rejects_nan_vacuum_scale` | vacuum_scale_factor 为 NaN 被拒绝 |
| `test_tune_table_sql_rejects_nan_analyze_scale` | analyze_scale_factor 为 NaN 被拒绝 |
| `test_tune_table_sql_rejects_negative_cost_limit` | autovacuum_cost_limit 为负被拒绝 |
| `test_tune_table_sql_rejects_negative_cost_delay` | autovacuum_cost_delay 为负被拒绝 |
| `test_tune_table_sql_rejects_negative_vacuum_threshold` | autovacuum_vacuum_threshold 为负被拒绝 |
| `test_tune_table_sql_rejects_oversized_toast_threshold` | toast_tuple_target 超上界被拒绝 |
| `test_validate_pg_memory_size` | PG 内存大小校验（`64MB`、`1GB` 等，拒绝 SQL 注入） |
| `test_validate_toast_storage` | TOAST 存储策略校验（external/extended/main/plain） |
| `test_env_bool` | 环境变量布尔值解析（true/false/1/0/yes/no/on/off，含大小写） |
| `test_checkpoint_target_clamping` | checkpoint_completion_target 超范围 clamp 到 [0.0, 1.0] |
| `test_pool_max_zero_fallback` | pool_max_connections=0 回退到默认值 |
| `test_pool_min_exceeds_max_clamped` | pool_min > pool_max 时 clamp 到 max |
| `test_fillfactor_out_of_range` | fillfactor 越界 [1, 100] 被拒绝 |
| `test_f64_nan_infinity_fallback` | NaN/Infinity f64 值回退到默认 |
| `test_toast_threshold_min_value` | toast_tuple_target 最小值校验 |
| `test_toast_threshold_upper_bound` | toast_tuple_target 上界校验 [128, 8160] |
| `test_autovac_nonneg_validation` | autovacuum 参数非负校验 |
| `test_stats_target_range_validation` | default_statistics_target 范围校验 [-1, 10000] |
| `test_autovac_vacuum_threshold_nonneg` | autovacuum_vacuum_threshold 非负校验 |
| `test_autovac_scale_range_validation` | autovacuum_scale_factor 范围校验 [0.0, 1.0] |
| `test_keepalive_count_upper_bound` | keepalive_count 上界校验 |
| `test_duration_zero_rejected` | Duration 零值被拒绝 |
| `test_server_max_connections_range` | max_connections 范围校验 |
| `test_hash_partitions_default` | hash_partitions 默认值为 0（非分区） |
| `test_hash_partitions_env` | 环境变量设置 hash_partitions |
| `test_partition_count_sql` | 分区计数 SQL 生成 |

### `src/store.rs`（15 个）

**test_strip 模块**（9 个）— URL 参数剥离逻辑：

| 测试名 | 验证内容 |
|--------|----------|
| `no_query` | 无查询参数的 URL 不变 |
| `preserve_sqlx_params` | 保留 sqlx 内部参数（如 `sslmode`） |
| `strip_all_custom` | 剥离所有自定义参数 |
| `strip_hash_partitions` | 剥离 `hash_partitions` 参数 |
| `mixed_params` | 混合参数部分剥离部分保留 |
| `with_fragment` | 带 fragment 的 URL 正确处理 |
| `strip_bare_param_no_equals` | 裸参数（无 `=`）被剥离 |
| `strip_empty_value_param` | 空值参数被剥离 |
| `strip_only_bare_custom_params` | 仅裸自定义参数的 URL |

**test_partition 模块**（6 个）— 分区验证逻辑：

| 测试名 | 验证内容 |
|--------|---------|
| `test_verify_no_partition_expected_none` | 非分区表 + 期望非分区 → 通过 |
| `test_verify_no_partition_expected_none_borderline` | 边界情况：期望非分区 |
| `test_verify_partitioned_match` | 分区表 + 期望分区 → 通过 |
| `test_verify_partitioned_mismatch` | 分区表 + 期望非分区 → 失败 |
| `test_verify_mismatch_expected_none_actual_partitioned` | 非分区表 + 期望分区 → 失败 |
| `test_verify_partitioned_but_table_not_partitioned` | 期望分区但表实际非分区 → 失败 |

### `src/composer.rs`（12 个）

| 测试名 | 验证内容 |
|--------|----------|
| `test_redact_url_basic` | 基本 IPv4 URL 脱敏（user:pass → user:***） |
| `test_redact_url_no_userinfo` | 无用户信息的 URL 不变 |
| `test_redact_url_with_query` | 含查询参数的 URL 脱敏 |
| `test_redact_url_no_scheme` | 无 scheme 的 URL 脱敏 |
| `test_redact_url_ipv6_with_userinfo` | IPv6 + 用户信息脱敏 |
| `test_redact_url_ipv6_no_userinfo` | IPv6 无用户信息不变 |
| `test_redact_url_ipv6_with_query` | IPv6 + 查询参数脱敏 |
| `test_redact_url_password_with_at_sign` | 密码含 `@` 的 rfind 行为 |
| `test_redact_url_password_with_multiple_at_signs` | 密码含多个 `@` 的 rfind 行为 |
| `test_redact_url_no_path` | 无路径的 URL 脱敏 |
| `test_redact_url_no_port` | 无端口的 URL 脱敏 |
| `test_redact_url_userinfo_host_only` | 仅含用户信息+主机的 URL 脱敏 |

### `src/transaction.rs`（9 个）

| 测试名 | 验证内容 |
|--------|----------|
| `test_dedup_pairs_empty` | 空输入 |
| `test_dedup_pairs_single` | 单个键值对 |
| `test_dedup_pairs_no_duplicates` | 无重复键 |
| `test_dedup_pairs_last_wins` | 重复键最后写入胜出 |
| `test_dedup_pairs_large` | 大集合去重 |
| `test_dedup_pairs_boundary_32` | 32 对边界（线性路径） |
| `test_dedup_pairs_boundary_33` | 33 对边界（HashMap 路径） |
| `test_getm_hashmap_path_duplicate_keys` | getm HashMap 路径重复键处理 |
| `test_getm_hashmap_path_large_result_set` | getm HashMap 路径大结果集 |

### `src/error.rs`（3 个）

| 测试名 | 验证内容 |
|--------|----------|
| `test_is_transient` | transient 错误码检测（`08xxx` 连接异常、`57P01` 等） |
| `test_from_sqlx_tls_is_transient` | Tls 错误映射为 Io(transient) |
| `test_from_sqlx_protocol_is_transient` | Protocol 错误映射为 Io(transient) |

运行：

```bash
# 全部单元测试
cargo test --lib

# 按 module 过滤
cargo test --lib config
cargo test --lib tune
cargo test --lib store
cargo test --lib composer
cargo test --lib transaction
cargo test --lib error
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

`persistent_statements` 参数决定是否使用 named prepared statement，自动根据连接模式解析：

- **连接池模式**（默认）→ `true`（使用 named prepared statement，最佳性能）
- **直连模式**（`pooler=true`）→ `false`（使用 unnamed statement，兼容 transaction-mode pooler）

可通过以下方式手动覆盖：

- URL 参数：`?persistent_statements=false`
- 环境变量：`PG_PERSISTENT_STATEMENTS=true`（支持 `auto`/`true`/`false` 及同义词 `on`/`off`/`1`/`0`）

如需查看检测结果，使用 `--nocapture` 放开 stdout 捕获：

```bash
cargo test --test integration_test -- --nocapture --test-threads=1
```

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