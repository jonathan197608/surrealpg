# surreal-pg 代码审计报告

> 审计日期: 2026-07-31
> 审计范围: `/Volumes/DELL/rust/surrealpg/src/` 全部源码 + 测试 + 配置
> 代码量: ~1,800 行 Rust (不含 target/)

---

## 总览

| 严重性 | 数量 | 类型 |
|--------|------|------|
| **Critical** | 2 | SQL 注入 |
| **High** | 3 | 连接泄漏 / 事务安全 / 死代码 |
| **Medium** | 7 | 设计缺陷 / 健壮性 |
| **Low** | 5 | 代码质量 / 效率 |

---

## Critical

### S1. `table_name` SQL 注入

**文件**: `src/config.rs:192`, `src/transaction.rs` (多处), `src/store.rs:235`

`table_name` 通过 URL 查询参数直接设置，且在所有 SQL 中通过 `format!()` 直接拼接，未使用参数化查询或标识符转义。

**注入路径**:
```
URL: postgresql://host/db?table_name=kv; DROP TABLE users; --
```

**受影响的 SQL 构造点** (共 14 处):
- `transaction.rs:207` — `SELECT 1 AS exists_flag FROM {table} WHERE key = $1`
- `transaction.rs:214` — `SELECT val FROM {table} WHERE key = $1`
- `transaction.rs:227` — `SELECT key, val FROM {table} WHERE key = ANY($1)`
- `transaction.rs:251` — `INSERT INTO {table} (key, val) VALUES ...`
- `transaction.rs:266` — `INSERT INTO {table} ... ON CONFLICT ...`
- `transaction.rs:287` — `UPDATE {table} SET val = ...`
- `transaction.rs:312` — `DELETE FROM {table} WHERE key = $1`
- `transaction.rs:332` — `DELETE FROM {table} WHERE key = $1 AND val = $2`
- `transaction.rs:346` — `DELETE FROM {table} WHERE key >= $1 AND key < $2`
- `transaction.rs:371` — `SELECT {select} FROM {table} WHERE ...`
- `transaction.rs:440` — `SELECT count(*) ... FROM {table} WHERE ...`
- `store.rs:103` — `CREATE TABLE IF NOT EXISTS {table} ...`
- `store.rs:110` — `ALTER TABLE {table} SET ...` (多处)
- `store.rs:235` — `VACUUM ANALYZE {table}`

**修复建议**: 使用 `sqlx::quote_identifier()` 或手动校验 `table_name` 仅包含 `[a-zA-Z0-9_]`，拒绝其他字符。最佳方案是在 `PgConfig::merge_url_params` 中添加验证:

```rust
fn validate_identifier(name: &str) -> Result<String, String> {
    if name.chars().all(|c| c.is_alphanumeric() || c == '_') && !name.is_empty() {
        Ok(name.to_string())
    } else {
        Err(format!("invalid table name: {name}"))
    }
}
```

---

### S2. `PG_TUNED_*` 环境变量 SQL 注入

**文件**: `src/tune.rs:162-223`

`session_sql()` 和 `tune_table_sql()` 将多个环境变量值直接拼接进 SQL 字符串，未做转义。

**注入路径**:
```bash
PG_TUNED_SERVER_WORK_MEM="64MB'; DROP TABLE kv; --"
```
生成的 SQL:
```sql
SET work_mem = '64MB'; DROP TABLE kv; --';
```

**受影响的参数**:
| 环境变量 | 拼接位置 | 方式 |
|---------|---------|------|
| `PG_TUNED_TABLE_TOAST_STORAGE` | `SET STORAGE {toast}` | 裸拼接 (无引号) |
| `PG_TUNED_SERVER_WORK_MEM` | `SET work_mem = '{wm}'` | 单引号内 |
| `PG_TUNED_SERVER_MAINTENANCE_WORK_MEM` | `SET maintenance_work_mem = '{mwm}'` | 单引号内 |
| `PG_TUNED_SERVER_EFFECTIVE_CACHE_SIZE` | `SET effective_cache_size = '{ecs}'` | 单引号内 |

虽然环境变量通常由运维人员控制，但仍应遵循纵深防御原则。

**修复建议**: 对所有从环境变量获取的 SQL 值进行白名单校验。例如 `toast_storage` 限定为 `external|extended|main|plain`，内存大小值校验为 `^[0-9]+(MB|GB|kB)$`。

---

## High

### H1. 只读事务 `commit()` 泄漏连接

**文件**: `src/pg_tx.rs:100-116`

```rust
fn commit(&self) -> BoxFut<'_, kvs::Result<()>> {
    Box::pin(async move {
        if self.done.swap(true, Ordering::AcqRel) {  // ← 先设 done=true
            return Err(kvs::Error::TransactionFinished);
        }
        if !self.write {
            return Err(kvs::Error::TransactionReadonly);  // ← 返回错误，但 done 已为 true
        }
        // ... 连接未清理 ...
    })
}
```

**问题**: 当只读事务调用 `commit()` 时:
1. `done` 被设为 `true` (不可逆)
2. 返回 `TransactionReadonly` 错误
3. 内部 `PgTransaction` 未被 `cancel()` 或 `close()`
4. 连接未归还连接池
5. 后续 `cancel()` 调用会因 `done == true` 返回 `TransactionFinished`，无法清理

连接会一直占用，直到 `PgTx` 被 Drop (此时 `PgTransaction::Drop` 只打日志不 ROLLBACK)。

**额外问题**: PostgreSQL 完全支持对 `BEGIN READ ONLY` 事务执行 `COMMIT`，返回错误本身就不合理。SurrealDB 引擎在某些路径下可能对只读事务调用 `commit()` 而非 `cancel()`。

**修复建议**:
```rust
fn commit(&self) -> BoxFut<'_, kvs::Result<()>> {
    Box::pin(async move {
        if self.done.swap(true, Ordering::AcqRel) {
            return Err(kvs::Error::TransactionFinished);
        }
        // 只读事务也可��� COMMIT (PG 原生支持)
        let mut guard = self.inner.lock().await;
        if let Some(tx) = guard.as_mut() {
            tx.commit().await.map_err(kvs::Error::from)?;
        }
        *guard = None;
        Ok(())
    })
}
```

---

### H2. `PgTransaction::Drop` 不执行 ROLLBACK

**文件**: `src/transaction.rs:497-505`

```rust
impl Drop for PgTransaction {
    fn drop(&mut self) {
        if !self.closed {
            tracing::warn!(
                "PgTransaction dropped without explicit commit/cancel; \
                 PG will auto-rollback"
            );
        }
    }
}
```

**问题**: `Drop` 只打日志，不执行 `ROLLBACK`。由于 `Drop` 不能是 async，无法直接执行 SQL。代码注释声称 "PG will auto-rollback"，但这依赖 sqlx 连接池在回收连接时的隐式行为。

实际行为取决于 sqlx 版本:
- sqlx 0.8 的 `PoolConnection::Drop` 会将连接归还连接池
- 如果连接上有未提交的事务，sqlx **不会**自动发送 ROLLBACK
- 下一个获取该连接的用户会继承这个未提交的事务
- 后续 `BEGIN` 会变成嵌套事务 (savepoint)，导致语义错误

**影响**: 在异常路径 (panic, 提前 return) 下，连接可能被污染。

**修复建议**: 在 `PgTx` (而非 `PgTransaction`) 上实现更安全的 Drop 逻辑，或在 `PgTransaction::close()` 中确保所有路径都显式 ROLLBACK。可考虑使用 `tokio::runtime::Handle::current().block_on()` 在 Drop 中同步执行 ROLLBACK (有局限性，但比什么都不做强)。

---

### H3. 调优环境变量 `PG_TUNED_POOL_IDLE_TIMEOUT` / `PG_TUNED_POOL_MAX_LIFETIME` 是死代码

**文件**: `src/store.rs:65-66`

```rust
let idle_timeout = config.idle_timeout.or(Some(tune.pool_idle_timeout));
let max_lifetime = config.max_lifetime.or(Some(tune.pool_max_lifetime));
```

**问题**: `config.idle_timeout` 的默认值是 `Some(Duration::from_secs(600))`，`config.max_lifetime` 的默认值是 `Some(Duration::from_secs(1800))`。由于 `Option::or` 只在 `None` 时使用备选值，这两个 `.or()` 永远不会使用 `tune` 的值。

用户通过 `PG_TUNED_POOL_IDLE_TIMEOUT` 和 `PG_TUNED_POOL_MAX_LIFETIME` 设置的值会被完全忽略。

**修复建议**: 将 `PgConfig` 中 `idle_timeout` 和 `max_lifetime` 的默认值改为 `None`，让 `PgTuneConfig` 的值作为真正的默认:

```rust
// config.rs Default
idle_timeout: None,   // 改为 None
max_lifetime: None,   // 改为 None
```

---

## Medium

### M1. 配置优先级用哨兵值判断

**文件**: `src/store.rs:50-64`

```rust
let pool_max = if config.max_connections != 20 {
    config.max_connections  // URL 设置了非默认值
} else {
    tune.pool_max            // 用调优值
};
```

**问题**: 使用魔法数字 `20`、`5`、`10s` 作为哨兵值来判断 URL 参数是否被覆盖。如果用户在 URL 中显式设置 `max_connections=20` (与默认值相同)，代码无法区分 "使用默认值" 和 "显式设置为 20"，会错误地使用 `tune.pool_max`。

**修复建议**: 在 `PgConfig` 中添加 `Option<u32>` 字段或单独的 "was set" 标记，避免哨兵值判断。

---

### M2. `PgConfig::statement_timeout` 是死代码

**文件**: `src/config.rs:19`, `src/config.rs:177-179`

`statement_timeout` 字段可以通过 URL 参数设置，但 **从未被使用**。实际的 statement timeout 来自 `PgTuneConfig::statement_timeout`，通过 `session_sql()` 设置。

用户设置 `?statement_timeout=60` 会被静默忽略。

---

### M3. 配置值无校验

**文件**: `src/config.rs:167-176`

```rust
"max_connections" => {
    if let Ok(v) = value.parse() {
        self.max_connections = v;  // 不校验 v > 0, v >= min_connections
    }
}
```

**问题**: 无任何边界校验。`max_connections=0` 会导致连接池无法获取任何连接，所有操作永久阻塞。`min_connections > max_connections` 行为未定义。

**修复建议**: 添加校验逻辑，拒绝不合理的值并记录警告。

---

### M4. `probe_persistent` 不清理探测连接

**文件**: `src/store.rs:279-340`

探测函数在两个连接上创建了 persistent prepared statements (`SELECT $1::int4`)，但探测完成后未显式 DEALLOCATE 这些语句。

- 直连 PG: 连接归还连接池后，prepared statement 仍在后端 session 中。后续 sqlx 可能尝试重用这个 statement name，但如果后端 session 被重新分配 (pgbouncer)，会导致 `prepared statement "sqlx_s_1" does not exist` 错误。
- pgbouncer tx mode: 探测检测到冲突返回 false，但 conn1 上的 prepared statement 可能残留在共享后端 session 中，影响其他客户端。

**修复建议**: 探测完成后，在两个连接上执行 `DEALLOCATE ALL` 清理所有 prepared statements。

---

### M5. `canceller` 和 `config` 参数被忽略

**文件**: `src/composer.rs:90-113`

```rust
async fn new_transaction_builder(
    &self,
    path: &str,
    canceller: CancellationToken,  // ← PG 路径忽略
    config: ConfigMap,             // ← PG 路径忽略
) -> anyhow::Result<...> {
    if Self::is_pg_path(path) {
        let store = PgStore::new(path).await?;  // canceller/config 未传入
        ...
    }
}
```

`CancellationToken` 可用于在服务器关闭时取消长时间运行的 PG 操作，但被完全忽略。`ConfigMap` 可能包含影响存储行为的配置，也被忽略。

---

### M6. 只读事务未设置隔离级别

**文件**: `src/store.rs:176-185`

```rust
let begin_sql = if write {
    format!("BEGIN ISOLATION LEVEL {}", self.config.isolation_level.as_sql())
} else if self.config.read_only_optimization {
    "BEGIN READ ONLY".to_string()  // ← 未设置隔离级别
} else {
    "BEGIN".to_string()
};
```

如果用户配置了 `Serializable` 隔离级别，只读事务不会使用它，而是回退到服务器默认 (通常 READ COMMITTED)。对于需要一致性快照读的场景，这可能导致不可预期的行为。

---

### M7. 多个配置项不可通过 URL 设置

**文件**: `src/config.rs:161-212`

`merge_url_params` 不支持以下配置项:
- `connect_timeout` — 只有默认值 10s
- `idle_timeout` — 只有默认值 600s
- `read_only_optimization` — 只有默认值 true
- `min_connections` — 虽然支持但无法与 `max_connections` 做交叉校验

文档中 `read_only_optimization` 和 `connect_timeout` 未提及，但代码中存在这些字段，用户可能期望能配置。

---

## Low

### L1. `is_pg_path` 冗余检查

**文件**: `src/composer.rs:53-58`

```rust
fn is_pg_path(path: &str) -> bool {
    path.starts_with("postgres://")      // 已被第 4 行覆盖
        || path.starts_with("postgresql://")  // 已被第 5 行覆盖
        || path.starts_with("postgres:")
        || path.starts_with("postgresql:")
}
```

`postgres://` 必然匹配 `postgres:`，`postgresql://` 必然匹配 `postgresql:`。前两个检查是冗余的。

---

### L2. OFFSET 分页对大偏移量低效

**文件**: `src/transaction.rs:371-374`

```sql
SELECT ... FROM kv WHERE key >= $1 AND key < $2
ORDER BY key ASC LIMIT $3 OFFSET $4
```

PostgreSQL 需要扫描并跳过 `OFFSET` 行。对于深度分页 (如 `skip=100000`)，性能会显著下降。SurrealDB 通常使用小 offset，但如果未来出现大范围扫描场景，应考虑 keyset pagination。

---

### L3. 无死锁/序列化失败自动重试

**文件**: `src/error.rs:77-80`, `src/pg_tx.rs`

`Deadlock` 和 `SerializationFailure` 错误被映射为 `TransactionConflict`，但 PG 后端本身不执行任何重试。SurrealDB 引擎可能在更高层处理重试，但如果不处理，这些错误会直接暴露给用户。对于 `SERIALIZABLE` 隔离级别，序列化失败是常态而非异常，自动重试几乎是必需的。

---

### L4. 测试 `clean_all` 未覆盖全部 key 空间

**文件**: `tests/integration_test.rs:103-107`

```rust
async fn clean_all(store: &Arc<PgStore>) {
    let mut tx = store.begin(true).await.unwrap();
    tx.delr(vec![]..vec![0xFF]).await.ok();  // ← 只删到 [0xFF]
    tx.commit().await.ok();
}
```

范围 `[]..[0xFF]` 只删除 key 值小于 `[0xFF]` 的记录。key 值 `[0xFF]` 本身及 `[0xFF, 0x00]`、`[0xFF, 0xFF]` 等不会被删除。SurrealDB 编码的 key 通常以 `*` (0x2A) 或 `/` (0x2F) 开头，所以实际上可能覆盖了所有测试 key，但这个假设是隐式的。

**修复**: 使用 `tx.delr(vec![]..vec![0xFF; 16])` 或直接 `DELETE FROM kv_test` (无 WHERE)。

---

### L5. 14 处 `.expect("guarded txn")` 依赖 lock() 顺序保证

**文件**: `src/pg_tx.rs` (14 处)

每个 `Transactable` 方法都调用 `self.lock().await?` 然后立即 `.expect("guarded txn")`。`lock()` 方法检查 `done` 标志和 inner 是否为 None，理论上保证了 `.expect()` 不会 panic。但这依赖于:
1. `done` 的 AcqRel 语义在所有平台上正确传播
2. `lock()` 的两个检查之间不存在 TOCTOU 窗口
3. 没有其他代码路径能在 `lock()` 返回 Ok 后修改 inner

虽然当前分析认为不会 panic，但使用 `.expect()` 在生产代码中不是最佳实践。建议改为 match 并返回错误。

---

## 附录: 配置优先级关系图

```
优先级 (高 → 低):

  PG_PERSISTENT_STATEMENTS (env)     → persistent_statements
  URL ?persistent_statements=        ↗
  PgConfig::default()                ↘
                                     (Auto → probe 检测)

  URL ?max_connections=              → pool_max (通过哨兵值判断)
  PG_TUNED_POOL_MAX_CONNECTIONS      ↗
  PgTuneConfig::default()            ↘

  PG_TUNED_POOL_IDLE_TIMEOUT         → ❌ 死代码 (H3)
  PG_TUNED_POOL_MAX_LIFETIME         → ❌ 死代码 (H3)

  URL ?statement_timeout=            → ❌ 死代码 (M2)
  PG_TUNED_QUERY_STATEMENT_TIMEOUT   → 实际生效 (通过 session_sql)
```

---

## 建议修复优先级

1. **立即修复**: S1, S2 (SQL 注入)
2. **尽快修复**: H1 (连接泄漏), H3 (死代码)
3. **计划修复**: H2 (Drop 不 ROLLBACK), M1-M7
4. **择机改进**: L1-L5
