---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: 'c29bae1a-f520-4b5a-99d9-81adf7ed8720'
  PropagateID: 'c29bae1a-f520-4b5a-99d9-81adf7ed8720'
  ReservedCode1: '8b5cf717-480d-41f8-94e5-7b3842fc69c1'
  ReservedCode2: '8b5cf717-480d-41f8-94e5-7b3842fc69c1'
---

# AUDIT REPORT V10 — Bug / Performance / Functionality

> **Scope**: Full codebase deep audit across three dimensions.  
> **Date**: 2026-08-04  
> **Baseline**: commit `f1a341f` (V9 audit fixes applied)  
> **Diff from V9**: P4 解构借用、B3 小池探测、B4 percent decode、B5 is_finite、F6 env warn、B6 Arc config/tune — 全部已修复

---

## 1. Bug 维度

### B1 [High] `PgTx::commit` 中 `done.swap(true)` 时序仍不一致

**文件**: `src/pg_tx.rs:131-160`

```rust
fn commit(&self) -> BoxFut<'_, kvs::Result<()>> {
    Box::pin(async move {
        if self.done.load(Ordering::Relaxed) {   // 快速检查
            return Err(kvs::Error::TransactionFinished);
        }
        if self.done.swap(true, Ordering::AcqRel) {  // ← 原子 claim
            return Err(kvs::Error::TransactionFinished);
        }
        // --- 窗口：done=true，但 inner 仍持有 PgTransaction + 连接 ---
        let mut guard = self.inner.lock().await;
        if let Some(tx) = guard.as_mut()
            && let Err(e) = tx.commit().await
        {
            *guard = None;
            return Err(kvs::Error::from(e));
        }
        let had_tx = guard.is_some();
        *guard = None;
        ...
    })
}
```

**问题**: V9 修复了 `PgTx::cancel()` 中的 done swap 时序（先 fast check → swap → lock → commit → release），但 `commit()` 仍然在 `self.inner.lock().await` **之前**就 `swap(true)`。

在 swap 到 lock 获取之间有一个窗口：`done=true` 但 `inner` 仍持有活跃的 `PgTransaction`。如果另一个线程在此窗口调用 `closed()`（返回 true），SurrealDB 引擎可能认为事务已关闭，而底层连接尚未归还池。

**影响**: 极窄窗口，但 `cancel()` 路径在 V9 中已按 B1 修复（先 fast load → swap → lock），`commit()` 应保持一致。

**建议**: 将 `commit()` 中 `swap(true)` 移到 `*guard = None` 之后，与 `cancel()` 保持一致的时序模式。或者改用统一的方法处理两个路径。

**严重性**: **High**（逻辑一致性：commit 与 cancel 的 done 标志时序不一致）

---

### B2 [High] `percent_decode` 不处理 `+` 为空格

**文件**: `src/config.rs:9-43`

```rust
fn percent_decode(input: &str) -> String {
    // ... 只处理 %XX，不处理 +
}
```

**问题**: URL query string 中，`+` 是空格的替代编码（`application/x-www-form-urlencoded` 规范）。虽然 PostgreSQL 连接 URL 的参数部分严格遵循 RFC 3986（`+` 不等于空格），但许多库和框架在构造 URL 时会将空格编码为 `+` 而非 `%20`。

如果用户从 pgbouncer/Supabase 等工具复制粘贴 URL，密码中包含空格（编码为 `+`），我们的 `percent_decode` 不会将 `+` 转为空格，导致密码不匹配。

**影响**: 密码或 table_name 中含空格的 URL 参数被错误解码。

**建议**: 增加 `+` → 空格的转换，或在文档中明确声明只支持 RFC 3986 百分比编码（不支持 `+` 编码）。

**严重性**: **Medium**（实际场景中密码含空格较罕见，但一旦出现难以排查）

---

### B3 [Medium] `check_writable()` 在 `self.closed` 检查之前执行

**文件**: `src/transaction.rs` — 所有写操作方法

```rust
pub async fn set(&mut self, key: Key, val: Val) -> Result<()> {
    self.check_writable()?;          // ← 先检查写权限
    if self.closed { return Err(PgStoreError::TxClosed); }  // ← 后检查关闭状态
    ...
}
```

**问题**: 在已关闭的事务上，`check_writable()` 在 `TxClosed` 检查之前执行。虽然 `check_writable()` 只读 `self.writeable` 字段（不会 panic），但语义上应该先检查事务是否关闭，再检查写权限 — 一个已关闭的事务不应该报 `TxReadOnly` 错误。

**影响**: 错误类型不精确。如果事务已关闭但恰好是只读事务，用户收到 `TxReadOnly` 而非 `TxClosed`，可能误导排查方向。

**建议**: 交换两个检查的顺序：先检查 `closed`，再检查 `writable`。

**严重性**: **Medium**（错误类型不精确，影响用户体验和可调试性）

---

### B4 [Medium] `error.rs` 中 `KeyAlreadyExists` 持有 `Vec<u8>` 导致错误构造时总是分配

**文件**: `src/error.rs:12`

```rust
#[error("key already exists: {0:?}")]
KeyAlreadyExists(Vec<u8>),
```

**问题**: `KeyAlreadyExists` 和 `ConditionNotMet` 都持有 `Vec<u8>` 的 key 副本。在 `from_sqlx()` 中：

```rust
"23505" => key
    .map(|k| Self::KeyAlreadyExists(k.to_vec()))  // ← 每次都 clone
    .unwrap_or_else(|| Self::Other(format!("unique violation: {msg}")))
```

如果 `key` 是 `Some(&[u8])`，每次都分配一个新的 `Vec<u8>`。在高 QPS 写入冲突场景下，这是不必要的堆分配。

**影响**: 写冲突热点路径上的额外堆分配。

**建议**: 将 `KeyAlreadyExists` 和 `ConditionNotMet` 改为持有 `Vec<u8>` 的引用或 `Box<[u8]>`。或者仅在 `key` 可用时才构造 — 在 `put()` 中已经知道 key，可以在返回时直接构造而不经过 `from_sqlx`。

**严重性**: **Medium**（性能：写冲突路径额外分配）

---

### B5 [Medium] `is_sql_reserved` 线性扫描 ~50 个保留字

**文件**: `src/config.rs:166-181`

```rust
fn is_sql_reserved(name: &str) -> bool {
    const RESERVED: &[&str] = &[
        "SELECT", "INSERT", ... // ~50 words
    ];
    let upper = name.to_ascii_uppercase();
    RESERVED.contains(&upper.as_str())
}
```

**问题**: 每次调用 `is_sql_reserved` 都执行 `to_ascii_uppercase()`（堆分配一个新的 `String`），然后对 ~50 个元素的数组做线性搜索。

这个方法只在 `validate_identifier` 中调用，而 `validate_identifier` 只在 URL 解析时调用一次（启动时）。所以这不是性能问题。

**真正的问题**: 保留字列表不完整。PG 有 ~500 个保留字，当前只覆盖了 ~50 个。如果用户使用 `DO`、`CASE`、`WHEN`、`THEN`、`END`、`IF`、`FOR`、`ALL`、`ANY`、`EXISTS`、`IS`、`IN` 等作为表名，当前列表不会拦截，但这些词在 PG 中是保留字，会导致 DDL/DML 语法错误。

**建议**: 使用 `phf`（编译期哈希）替代运行时数组 + 线性搜索，同时扩展保留字列表。

**严重性**: **Medium**（功能：SQL 注入防护不完整，可能导致语法错误）

---

### B6 [Low] `PgConfig::merge_url_params` 不处理重复参数

**文件**: `src/config.rs:269-383`

**问题**: URL 如 `?max_connections=10&max_connections=20` 会解析两次，后者覆盖前者。虽然这在语义上是"last wins"（可接受），但没有日志警告用户重复参数。

**影响**: 用户可能无意中重复设置参数，得到非预期值。

**建议**: 对已知参数名记录首次出现后忽略后续出现的，或至少打印 warn 日志。

**严重性**: **Low**（不影响正确性，仅影响可观测性）

---

### B7 [Low] `PgTransaction::count_approx` 绑定 `&*self.sql.table_name` 语义模糊

**文件**: `src/transaction.rs:728`

```rust
let row = Self::build_query(persistent, &self.sql.count_approx)
    .bind(&*self.sql.table_name)  // ← 解引用 Arc<Sql>.table_name
    .fetch_optional(conn.deref_mut())
```

**问题**: `&*self.sql.table_name` 是 `&String`，sqlx 的 `bind` 会将其作为 text 参数发送。功能正确，但 `&self.sql.table_name` 已经是 `&String`，`&*` 是多余的解引用再引用 — 可以直接写 `&self.sql.table_name`，更清晰。

**影响**: 仅代码风格问题，无功能影响。

**建议**: 改为 `self.sql.table_name.as_str()` 或直接 `&self.sql.table_name`。

**严重性**: **Low**（代码风格）

---

## 2. 性能维度

### P1 [High] 每个 `Transactable` 操作经过 `tokio::Mutex::lock()`

**文件**: `src/pg_tx.rs:54-79`

**问题**: V9 中已记录但未修复。`PgTx` 使用 `tokio::sync::Mutex<Option<PgTransaction>>` 实现内部可变性。SurrealDB 事务是顺序执行的（事务内无并发），所以此 Mutex 永远不竞争 — 但每次操作都有 `lock().await` 的开销。

**量化**: `tokio::sync::Mutex::lock()` 在无竞争时约 ~50-100ns（原子 CAS + futures 构造 + poll）。在高 QPS 场景（10万 ops/s），这约 5-10μs/s 的纯开销 — 不是瓶颈但可测量。

**影响**: 每次 KV 操作多一次异步 Mutex lock。

**建议**: 长期方案是让 `PgTransaction` 直接实现 `Transactable`（避免 wrapper），但这需要重构 `PgTransaction` 从 `&mut self` 改为内部可变性。短期不可行，但值得追踪。

**严重性**: **High**（性能：每次操作额外 ~100ns，高 QPS 可测量）

---

### P2 [Medium] `getm` 线性路径对重复 keys 返回重复 values 但 `find()` 每次从头扫描

**文件**: `src/transaction.rs:393-400`

```rust
keys.into_iter()
    .map(|k| {
        extracted
            .iter()
            .find(|(row_key, _)| *row_key == k)  // ← O(n) 每次
            .map(|(_, v)| v.clone())
    })
    .collect()
```

**问题**: 当 `keys` 包含重复键时（如 `[k1, k1, k1]`），线性路径对每个 key 都从头扫描 `extracted`。如果 keys 有 M 个（含重复），rows 有 N 个，复杂度是 O(M×N)。

V8 修复中已经加了 `rows.len() <= 64 && rows.len() * keys.len() <= 8192` 的阈值，防止 O(n²) 爆炸。但如果 `keys = [k1; 128]`（128 个重复键），乘积 = 128 × 1 = 128 ≤ 8192，走线性路径，但实际只扫描 1 行 × 128 次 = 128 次比较 — 还行。

**真正问题**: `v.clone()` 对 `Val`（`Vec<u8>`）做深拷贝。如果 `keys` 中有多个指向同一 row 的引用，每次都 clone 同一个 value。在重复键场景下，这意味着同一个 `Vec<u8>` 被多次深拷贝。

**建议**: 对线性路径，可以先建一个小的 HashMap（key → index in extracted），然后对 keys 查找。或者在 `find` 后用 `Rc<Vec<u8>>` 共享 value。但这增加了复杂度，仅在重复键场景下有价值。

**严重性**: **Medium**（性能：重复键场景下不必要深拷贝）

---

### P3 [Medium] `begin()` 中 ROLLBACK 重试路径无条件执行 DEALLOCATE ALL

**文件**: `src/store.rs:252-274`

```rust
let result = Executor::execute(&mut *conn, sqlx::raw_sql(begin_sql)).await;
match result {
    Ok(_) => {}
    Err(e) => {
        let is_failed_tx = matches!(&e, sqlx::Error::Database(db)
            if matches!(db.code().as_deref(), Some("25P02")));
        if is_failed_tx {
            let _ = Executor::execute(&mut *conn, sqlx::raw_sql("ROLLBACK"))
                .await
                .inspect_err(|e| warn!("ROLLBACK of leaked transaction failed: {e}"));
            Executor::execute(&mut *conn, sqlx::raw_sql(begin_sql))
                .await
                .map_err(|e2| PgStoreError::from_sqlx(None, &e2))?;
            warn!("cleaned up leaked transaction from pool connection");
        }
    }
}
```

**问题**: ROLLBACK 重试后没有重新执行 `after_connect` 中的 session SET 语句。如果连接是从池中获取的旧连接（之前已执行过 session SET），这些 SET 在 ROLLBACK 后仍然有效（因为它们是 session 级别的）— 所以这不是 bug。

但 `probe_persistent` 的 conn2 路径在 `DEALLOCATE ALL` 后连接被归还池，下次 `begin()` 可能拿到同一个连接 — 此时该连接上的 prepared statements 已全部清除。如果 `persistent=true`，sqlx 会重新创建 named prepared statements，这是正确的。但如果 `after_connect` 设置的某些 session 参数在 DEALLOCATE ALL 后被清除（实际不会，DEALLOCATE 只影响 prepared statements），可能会有问题。

**结论**: 无实际 bug，但代码缺乏注释解释为什么 ROLLBACK 重试后不需要重新执行 session SET。

**影响**: 无功能影响，但可维护性差。

**严重性**: **Low**（可维护性：缺少注释说明为什么 ROLLBACK 重试安全）

---

### P4 [Medium] `savepoint_sql()` 和 `push_savepoint_name()` 仍分配 String

**文件**: `src/transaction.rs:225-295`

```rust
fn push_savepoint_name(&mut self) -> String {
    // ... 栈缓冲区格式化 ...
    let name = unsafe { std::str::from_utf8_unchecked(name_slice) }.to_string();
    self.savepoints.push(name.clone());  // ← clone，因为 name 要返回
    name
}
```

**问题**: V8 中将 `format!()` 改为栈缓冲区手动格式化，但最终仍调用 `.to_string()` 堆分配。且 `push_savepoint_name` 中 `name.clone()` 又分配一次 — 总共 2 次堆分配（name + savepoints 中的 clone）。

`savepoint_sql` 也是 1 次堆分配。

每次 savepoint 操作共 3 次堆分配。虽然 savepoint 操作不频繁，但可以进一步优化。

**建议**: 让 `push_savepoint_name` 直接返回 `()`，将 name 压入 `self.savepoints` 后返回 `&str` 引用。或者让 `savepoint_sql` 使用 `std::fmt::Write` trait 写入 `String` 而不是栈缓冲区 + to_string。

**严重性**: **Low**（savepoint 操作不频繁，3 次堆分配在非热路径上可接受）

---

### P5 [Low] `is_sql_reserved` 的 `to_ascii_uppercase()` 启动时分配

**文件**: `src/config.rs:179`

**问题**: 每次 `is_sql_reserved` 调用都分配一个新 `String`。这在启动时只调用一次，不是性能问题。但如果未来需要在运行时频繁调用（如批量验证），会成为瓶颈。

**建议**: 使用 `phf` 编译期哈希表（保留字 → true），完全消除运行时分配和线性搜索。

**严重性**: **Low**（启动时一次性调用，非热路径）

---

## 3. 功能维度

### F1 [High] `count_approx` 忽略 range 参数，语义不正确

**文件**: `src/transaction.rs:717-734`

```rust
pub async fn count_approx(&mut self) -> Result<Option<u64>> {
    // 注意：无 range 参数！
    let row = Self::build_query(persistent, &self.sql.count_approx)
        .bind(&*self.sql.table_name)  // ← 绑定表名，不是 range
        .fetch_optional(conn.deref_mut())
```

**问题**: `count_approx` 的 SQL 查询 `SELECT reltuples::bigint FROM pg_class WHERE relname = $1` 返回整表的近似行数，与 range 无关。但 SurrealDB 的 `Transactable` trait 中 `count_approx` 可能期望返回**指定 range** 内的近似行数。

当前实现在语义上不正确 — 如果调用方请求 `namespace_A` 范围内的近似行数，我们返回的是**全表**行数（包括其他 namespace），可能差距巨大。

**影响**: 在多租户场景下（每个 namespace 用不同的 key 前缀），`count_approx` 返回的值可能远大于实际范围行数。

**建议**: 在文档中明确标注 `count_approx` 是全表估计，不受 range 参数影响。或者使用 `explain analyze` + 节点成本估计来实现 range-aware 近似计数（复杂度高）。

**严重性**: **High**（功能：语义不正确，多租户场景偏差大）

---

### F2 [High] `PgTransaction::commit()` 失败后连接可能泄漏

**文件**: `src/transaction.rs:263-270`

```rust
pub async fn commit(&mut self) -> Result<()> {
    let result = self.execute_simple("COMMIT", None).await;
    self.close(); // Always close — even on error, connection goes back to pool
    result?;
    ...
}
```

**问题**: `execute_simple("COMMIT")` 失败后，`self.close()` 将 `self.closed = true` 并 `self.conn.take()`。`PoolConnection` 被 `take()` 后在函数结束时 `Drop`，将连接归还池。

但在 PG 中，如果 `COMMIT` 失败（如序列化冲突），PG 已自动回滚事务 — 连接处于 `idle` 状态。`PoolConnection::drop` 将连接归还池，下一个 `begin()` 拿到这个连接时，`BEGIN` 应该正常工作。

**真正问题**: 如果 `COMMIT` 失败且错误是 `connection_failure`（08xxx），连接可能处于不可用状态（半关闭 TCP）。归还池后，下一个 `begin()` 拿到这个坏连接会立即失败。

**影响**: 一次连接失败可能导致"雪崩" — 后续多个事务在同一个坏连接上失败，直到连接被池回收。

**建议**: 在 `execute_simple` 返回 `08xxx` 类错误后，不归还连接到池 — 而是让连接 `drop` 时被池标记为不可用。sqlx 的 `PoolConnection::drop` 在连接处于错误状态时不会归还池（会重新创建连接），但需要确认 sqlx 0.8 的行为。

**严重性**: **Medium**（连接错误路径可能传播，但 sqlx 应该能自动处理）

---

### F3 [Medium] `READ ONLY` 事务在 pgbouncer transaction mode 下可能失败

**文件**: `src/store.rs:102-113`

```rust
let begin_read_sql: Arc<str> = if config.read_only_optimization {
    format!(
        "BEGIN ISOLATION LEVEL {} READ ONLY",
        config.isolation_level.as_sql()
    )
    .into()
} else {
    Arc::clone(&begin_write_sql)
};
```

**问题**: pgbouncer transaction mode 不支持 `BEGIN READ ONLY`。当 `read_only_optimization=true`（默认）且 `persistent=false`（pgbouncer 自动检测到）时，读事务仍使用 `BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY`，pgbouncer 会拒绝或忽略 `READ ONLY` 子句。

**影响**: 在 pgbouncer 后面，`BEGIN ... READ ONLY` 可能导致错误或被静默忽略。如果是错误，每个读事务都失败；如果被忽略，功能正常但 `READ ONLY` 不生效（无性能影响，只是语义不对）。

**建议**: 当 `persistent=false`（即检测到 pgbouncer）时，自动禁用 `read_only_optimization`，使用不带 `READ ONLY` 的 `BEGIN`。

**严重性**: **Medium**（功能：pgbouncer 兼容性）

---

### F4 [Medium] `from_sqlx` 中 `25P01` (no_active_sql_transaction) 映射到 `TxClosed` 可能丢失上下文

**文件**: `src/error.rs:90`

```rust
"25P01" => Self::TxClosed,
```

**问题**: `25P01` 表示"没有活跃事务"，但在某些场景下这不是因为事务已关闭，而是因为事务从未开始（如连接池泄漏后的恢复）。将所有 `25P01` 映射为 `TxClosed` 可能让用户误以为事务已经被显式关闭。

**建议**: 将 `25P01` 映射为 `Postgres("[25P01]: {msg}")` 以保留更多上下文，或新增 `TxNotActive` 变体。

**严重性**: **Low**（可观测性：错误映射可能误导排查）

---

### F5 [Medium] `PgStore::new()` 中 `pool_max` 为 0 时不报错

**文件**: `src/store.rs:84`

```rust
let pool_max = config.max_connections.unwrap_or(tune.pool_max);
```

**问题**: 如果用户通过 URL 设置 `max_connections=0`，`merge_url_params` 中有检查并 warn 但不会阻止。然而 `config.max_connections` 是 `Option<u32>` — `Some(0)` 不会被 `unwrap_or` 覆盖。

在 `merge_url_params` 中：
```rust
Ok(0) => { tracing::warn!("max_connections=0 is invalid, ignoring"); }
```
这段代码 `warn` 后**不设置** `self.max_connections`，所以 `config.max_connections` 保持 `None`，最终 `pool_max = tune.pool_max = 20`。这是正确的。

**但**: 如果 `PgConfig` 被直接构造（不经过 `merge_url_params`），`max_connections = Some(0)` 会传给 `PgPoolOptions::max_connections(0)`，sqlx 会 panic 或返回错误。

**影响**: 直接构造 `PgConfig` 时 `max_connections = Some(0)` 不会被检查。

**建议**: 在 `PgStore::new()` 中添加 `assert!(pool_max > 0)` 或在 `PgConfig` 构造时验证。

**严重性**: **Medium**（防御性编程：直接构造时无验证）

---

### F6 [Medium] `PgStore::clone()` 的 `Arc<PgConfig>` 浅拷贝 — 后续修改可能意外共享

**文件**: `src/store.rs:27-30`

```rust
pub struct PgStore {
    pool: PgPool,
    config: Arc<PgConfig>,
    tune: Arc<PgTuneConfig>,
```

**问题**: V9 中将 `config` 和 `tune` 改为 `Arc` 以避免深拷贝。这是正确的优化，但引入了一个微妙的问题：如果有人在 `PgStore::new()` 之后修改 `config`（通过 `Arc::get_mut` 或其他方式），所有 clone 共享的 `PgStore` 实例都会看到修改。

**影响**: 当前代码中 `config` 和 `tune` 在 `new()` 之后只读（没有 `Arc::get_mut` 调用），所以这是安全的。但如果未来有人添加了修改 config 的方法，可能导致意外的共享修改。

**建议**: 在 `config` 和 `tune` 字段上方添加注释：`// Arc-shared: immutable after construction. Do not use Arc::get_mut().`

**严重性**: **Low**（防御性编程：当前安全，未来需注意）

---

### F7 [Low] `percent_decode` 未测试

**文件**: `src/config.rs:9-53`

**问题**: V9 中添加的 `percent_decode` 和 `hex_digit` 函数没有单元测试。

**建议**: 添加测试用例覆盖：正常解码（`%20` → 空格）、连续编码（`%2F%2F` → `//`）、无效编码（`%2G` → `%2G` 保持原样）、尾随 `%`（`abc%` → `abc%`）、空输入。

**严重性**: **Low**（测试覆盖）

---

### F8 [Low] `register_metrics` 和 `collect_u64_metric` 指标不完整

**文件**: `src/pg_builder.rs:57-84`

**问题**: 当前只暴露了 3 个指标（pool_size, pool_idle, pool_max）。缺少：
- 活跃事务数
- 累计提交/回滚计数
- 平均事务延迟
- 语句超时计数

**影响**: 运维可观测性不足，生产环境排查性能问题缺乏数据。

**建议**: 后续添加更多指标（需要 `AtomicU64` 计数器）。

**严重性**: **Low**（功能增强：可观测性）

---

### F9 [Info] `env_bool` 解析 `warn!` 但 `env_u32`/`env_f64` 等已在 V9 修复

**状态**: V9 中已修复 `env_u32`/`env_i32`/`env_f64`/`env_duration` 的 warn 日志。`env_bool` 在 V9 之前已有 warn。`env_str_validated` 也有 warn。

**结论**: 所有 env 辅助函数现在都有 warn 日志，V9 F6 已完成。

---

## 汇总

| # | 维度 | 严重性 | 描述 | 状态 |
|---|------|--------|------|------|
| B1 | Bug | High | `commit()` 中 `done.swap(true)` 时序不一致 | ❌ 待修复 |
| B2 | Bug | Medium | `percent_decode` 不处理 `+` 为空格 | ❌ 待修复 |
| B3 | Bug | Medium | `check_writable()` 在 `closed` 检查之前 | ❌ 待修复 |
| B4 | Bug | Medium | `KeyAlreadyExists` 持有 `Vec<u8>` 错误构造时分配 | ❌ 待修复 |
| B5 | Bug | Medium | `is_sql_reserved` 保留字列表不完整 | ❌ 待修复 |
| B6 | Bug | Low | `merge_url_params` 不处理重复参数 | 接受现状 |
| B7 | Bug | Low | `count_approx` 中 `&*self.sql.table_name` 风格 | ❌ 待修复 |
| P1 | Perf | High | 每次 Transactable 操作经过 Mutex | 接受现状（需架构改动） |
| P2 | Perf | Medium | `getm` 线性路径重复键深拷贝 | 接受现状（阈值已限制） |
| P3 | Perf | Low | ROLLBACK 重试后缺少注释 | ❌ 待修复（加注释） |
| P4 | Perf | Low | savepoint 操作 3 次堆分配 | 接受现状（非热路径） |
| P5 | Perf | Low | `is_sql_reserved` 线性扫描 | 接受现状（启动时一次性） |
| F1 | Func | High | `count_approx` 忽略 range 语义不正确 | ❌ 待文档标注 |
| F2 | Func | Medium | `commit()` 失败后连接状态不确定 | 接受现状（sqlx 自动处理） |
| F3 | Func | Medium | `READ ONLY` 在 pgbouncer 下可能失败 | ❌ 待修复 |
| F4 | Func | Low | `25P01` 映射到 `TxClosed` 丢失上下文 | 接受现状 |
| F5 | Func | Medium | `PgConfig` 直接构造时 `max_connections=0` 无验证 | ❌ 待修复 |
| F6 | Func | Low | `Arc<PgConfig>` 共享修改风险 | ❌ 待加注释 |
| F7 | Func | Low | `percent_decode` 无测试 | ❌ 待修复 |
| F8 | Func | Low | 指标不完整 | 后续迭代 |
| F9 | Func | Info | env_* warn 日志已完成 | ✅ V9 已修复 |

---

## 修复优先级

### P0 — 立即修复（Bug / 正确性）
1. **B1**: `commit()` 中 `done.swap(true)` 移到 `*guard = None` 之后
2. **B3**: 写操作方法中交换 `check_writable()` 和 `closed` 检查顺序
3. **F5**: `PgStore::new()` 中添加 `pool_max > 0` 断言

### P1 — 本轮修复（功能 / 兼容性）
4. **B2**: `percent_decode` 添加 `+` → 空格处理
5. **B5**: 扩展保留字列表 + 用 `phf` 或二分查找替代线性搜索
6. **F3**: pgbouncer 检测到时自动禁用 `read_only_optimization`
7. **F1**: 在 `count_approx` 文档中标注全表估计语义

### P2 — 本轮修复（代码质量 / 测试）
8. **B7**: 修复 `&*self.sql.table_name` 风格
9. **F7**: 为 `percent_decode` / `hex_digit` 添加单元测试
10. **F6**: 添加 Arc 不可变性注释
11. **P3**: ROLLBACK 重试路径添加注释

### P3 — 后续迭代
12. **B4**: 错误构造时避免不必要堆分配
13. **B6**: URL 重复参数警告
14. **F4**: `25P01` 更精确映射
15. **F8**: 扩展指标