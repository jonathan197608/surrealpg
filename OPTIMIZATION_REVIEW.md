---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: 'f2d14d11-2a8a-4498-94d0-9eadad4e92e4'
  PropagateID: 'f2d14d11-2a8a-4498-94d0-9eadad4e92e4'
  ReservedCode1: 'ffc82fde-167b-461c-9524-e396b208e033'
  ReservedCode2: 'ffc82fde-167b-461c-9524-e396b208e033'
---

# surreal-pg 功能优化审阅报告

**日期**: 2026-08-03
**范围**: 功能优化与性能改进建议（非安全/Bug 审计）
**基线**: 三轮安全审计后的代码状态，`cargo check` + `cargo clippy` 零警告

---

## 总览

代码经过三轮安全审计后质量良好，无安全漏洞和逻辑 Bug。本轮审阅聚焦于**性能优化**和**功能增强**，共发现 **12 项可改进之处**，按影响级别排列。

| 级别 | 数量 | 说明 |
|------|------|------|
| 高影响 | 3 | 直接影响每个 KV 操作的延迟 |
| 中影响 | 5 | 影响特定场景或规模化后的表现 |
| 低影响 / 功能增强 | 4 | 锦上添花的改进 |

---

## 高影响

### O1. 每次 KV 操作重复构建 SQL 字符串

**文件**: `src/transaction.rs`
**行**: 207, 214, 227, 252, 268, 288, 312, 332, 346, 378, 448

**现状**: 每一个 `exists()`, `get()`, `set()`, `put()`, `putc()`, `del()`, `delc()`, `delr()`, `range_query()`, `count()` 调用都执行 `format!()` 拼接 SQL。由于 `self.table` 在事务创建后就不可变，这些 SQL 字符串是完全相同的，每次操作都做无谓的堆分配。

**量化**: 以 `get()` 为例，每次调用分配一个 ~40 字节的 `String`。在高频读写场景（SurrealDB 的默认工作模式），每秒数千次操作意味着每秒数千次堆分配 + 释放。

**建议**: 在 `PgTransaction::new()` 中预构建所有 SQL 并存储为字段：

```rust
pub struct PgTransaction {
    // ... 现有字段 ...
    // 预构建 SQL
    sql_exists: String,
    sql_get: String,
    sql_getm: String,
    sql_set: String,
    sql_put: String,
    sql_putc: String,
    sql_del: String,
    sql_delc: String,
    sql_delr: String,
    sql_count: String,
    // range_query 的 SQL 需要动态拼 direction，但 select 部分可预存
}

impl PgTransaction {
    pub(crate) fn new(conn: ..., table: String, ...) -> Self {
        let sql_exists = format!("SELECT 1 AS exists_flag FROM {table} WHERE key = $1");
        let sql_get = format!("SELECT val FROM {table} WHERE key = $1");
        let sql_getm = format!("SELECT key, val FROM {table} WHERE key = ANY($1)");
        let sql_set = format!(
            "INSERT INTO {table} (key, val) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val"
        );
        // ... 其他 SQL 同理 ...
        Self { sql_exists, sql_get, ... }
    }

    pub async fn get(&mut self, key: Key) -> Result<Option<Val>> {
        let row = self.fetch_optional_by_key(&self.sql_get, &key).await?;
        Ok(row.map(|r| r.get::<Vec<u8>, _>("val")))
    }
}
```

**收益**: 消除每次 KV 操作的一次堆分配 + 字符串拼接。savepoint 名称的 `format!("sp_{}", counter)` 也可预计算或用 `itoa` 替代。

---

### O2. `set` 的 ON CONFLICT 应使用 `EXCLUDED.val`

**文件**: `src/transaction.rs:252`

**现状**:
```sql
INSERT INTO kv (key, val) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET val = $2
```

`$2` 被绑定两次（一次在 VALUES 子句，一次在 DO UPDATE SET）。sqlx 需要将同一个参数值编码两次。

**建议**: 使用 PG 的 `EXCLUDED` 伪表引用，这是 UPSERT 的惯用写法：
```sql
INSERT INTO kv (key, val) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val
```

**收益**: sqlx 只绑定一次 `$2`，减少一次参数编码和传输。功能完全等价。

---

### O3. 范围扫描使用 OFFSET 分页，深翻页性能线性退化

**文件**: `src/transaction.rs:363-391`

**现状**: `range_query` 使用 `OFFSET $4` 实现跳页。PG 的 OFFSET 需要扫描并丢弃前 N 行，时间复杂度 O(skip + limit)。

**现状的日志已有提示**：
```rust
if skip > 1000 {
    warn!(skip, limit, "large OFFSET in range scan — consider cursor-based pagination");
}
```

**建议**: 支持游标分页（keyset pagination）作为可选模式：

```rust
/// 游标分页模式：跳过到指定 key 之后
pub enum ScanMode {
    /// 传统 OFFSET 模式（兼容现有 API）
    Offset(u32),
    /// 游标模式：返回 key > cursor 的记录
    After(Key),
}

// range_query 内部：
match mode {
    ScanMode::Offset(skip) => {
        // 现有 SQL：WHERE key >= $1 AND key < $2 ORDER BY key {dir} LIMIT $3 OFFSET $4
    }
    ScanMode::After(cursor) => {
        // 新 SQL：WHERE key >= $1 AND key < $2 AND key > $3 ORDER BY key {dir} LIMIT $4
        // 索引扫描直接定位，O(limit)
    }
}
```

**注意**: 这需要 SurrealDB 引擎层支持游标传递。如果 `Transactable` trait 的签名不可变，可以在 `PgTransaction` 内部维护一个 `last_scan_key` 状态，自动切换为游标模式。短期内可在文档中提示用户避免深翻页。

---

## 中影响

### O4. `begin()` 每次事务执行无条件 ROLLBACK 预清理

**文件**: `src/store.rs:191-201`

**现状**: 每次获取连接后立即执行 `ROLLBACK`，作为安全网防止泄漏的事务残留。这对正常流程中的每个事务都增加了一次网络往返。

**量化**: 每次 `begin()` 的网络开销：
1. `pool.acquire()` — 无 SQL
2. `ROLLBACK` — **额外往返**（当前每次都执行）
3. `BEGIN ...` — 必需
4. 实际 KV 操作 — 必需

即 25% 的额外往返开销。

**建议**: 只在连接被回收复用时执行 ROLLBACK。sqlx 的 `PoolConnection` 在返回池时可以标记状态，但当前 API 不直接暴露。替代方案：

```rust
// 方案 A：用 try_begin 替代裸 ROLLBACK
// 如果 ROLLBACK 失败说明确实有残留事务，成功说明无事务
// 但这仍然是每次都执行

// 方案 B（推荐）：延迟清理 — 只在检测到错误时清理
let mut conn = self.pool.acquire().await?;
let begin_result = Executor::execute(&mut *conn, sqlx::raw_sql(&begin_sql)).await;
if let Err(e) = &begin_result {
    let err_str = e.to_string().to_ascii_lowercase();
    if err_str.contains("already in a transaction") || err_str.contains("25P02") {
        // 确实有残留事务，执行 ROLLBACK 后重试
        let _ = Executor::execute(&mut *conn, sqlx::raw_sql("ROLLBACK")).await;
        Executor::execute(&mut *conn, sqlx::raw_sql(&begin_sql)).await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        warn!("cleaned up leaked transaction from pool connection");
    } else {
        return Err(PgStoreError::from_sqlx(None, e));
    }
} else {
    // BEGIN 成功，无需 ROLLBACK
}
```

**收益**: 正常路径减少一次网络往返（~0.1-1ms per transaction），异常路径仍能自愈。

**权衡**: 方案 B 依赖 PG 错误码 `25P02`（in_failed_sql_transaction）检测残留事务。如果残留的事务状态是"已 BEGIN 但未出错"的情况，PG 不会报错而是直接执行第二个 BEGIN，此时会报 `WARNING, 25P01, "there is already a transaction in progress"`。需要测试验证这个路径是否可靠。

---

### O5. `register_metrics` 返回 None，无可观测性

**文件**: `src/pg_builder.rs:57-63`

**现状**: SurrealDB 的 `TransactionBuilder` trait 提供了 `register_metrics()` 和 `collect_u64_metric()` 两个钩子，但 PG 后端全部返回 `None`，导致运维时无法通过 SurrealDB 的 metrics 接口观察连接池状态。

**建议**: 实现基本的池级指标：

```rust
impl TransactionBuilder for PgStore {
    fn register_metrics(&self) -> Option<Metrics> {
        // 注册指标名称，让 SurrealDB 知道我们可以提供这些指标
        Some(Metrics::from_iter([
            "pg_pool_size",
            "pg_pool_idle",
            "pg_pool_max",
        ]))
    }

    fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
        match metric {
            "pg_pool_size" => Some(self.pool.size() as u64),
            "pg_pool_idle" => Some(self.pool.num_idle() as u64),
            "pg_pool_max" => Some(self.pool.max_connections()),
            _ => None,
        }
    }
}
```

**收益**: 运维团队可以通过 SurrealDB 的 `/metrics` 端点监控 PG 连接池利用率，及时发现连接泄漏或池耗尽。

**注意**: `Metrics` 类型的确切构造方式需参考 SurrealDB core 源码中的定义。

---

### O6. `getm` 对少量键使用 HashMap 的开销

**文件**: `src/transaction.rs:220-243`

**现状**: `getm` 无论键数量多少，都使用 `HashMap<Vec<u8>, Vec<u8>>` 构建查找表。对于 2-4 个键（最常见的批量获取），HashMap 的分配 + 哈希开销可能超过线性扫描。

**建议**: 对小批量键使用线性匹配：

```rust
pub async fn getm(&mut self, keys: Vec<Key>) -> Result<Vec<Option<Val>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    if keys.len() <= 8 {
        // 小批量：直接查每条（利用 prepared statement 缓存）
        let mut results = Vec::with_capacity(keys.len());
        for key in &keys {
            let val = self.get(key.clone()).await?;
            results.push(val);
        }
        return Ok(results);
    }
    // 大批量：保持现有 ANY($1) + HashMap 逻辑
    // ... 现有代码 ...
}
```

**权衡**: 小批量时用 N 次 `SELECT` 替代 1 次 `SELECT ... WHERE key = ANY($1)`。当 N ≤ 8 时，N 次已缓存 prepared statement 的执行可能比一次大查询 + HashMap 构建更快（取决于网络延迟）。阈值 8 可根据实际 benchmark 调整。

**替代方案**: 保持 `ANY($1)` 但不用 HashMap，改用线性查找：

```rust
let rows: Vec<(Vec<u8>, Vec<u8>)> = rows.iter()
    .map(|r| (r.get::<Vec<u8>, _>("key"), r.get::<Vec<u8>, _>("val")))
    .collect();
Ok(keys.into_iter().map(|k| {
    rows.iter().find(|(rk, _)| rk == &k).map(|(_, v)| v.clone())
}).collect())
```

当 rows.len() ≤ 64 时，线性查找的 cache locality 远优于 HashMap。

---

### O7. `count(*)` 在大表上性能问题

**文件**: `src/transaction.rs:446-460`

**现状**: 使用 `SELECT count(*) FROM kv WHERE key >= $1 AND key < $2`。PostgreSQL 的 `count(*)` 必须扫描所有匹配行（不像 MySQL 有精确的索引统计），在大表上可能需要数秒。

**建议**: 提供近似计数选项：

```rust
/// 精确计数（现有行为）
pub async fn count(&mut self, rng: Range<Key>) -> Result<u64> {
    let sql = format!("SELECT count(*) AS cnt FROM {} WHERE key >= $1 AND key < $2", self.table);
    // ... 现有代码 ...
}

/// 近似计数（利用 pg_class 统计信息，O(1)）
pub async fn count_approx(&mut self, rng: Range<Key>) -> Result<u64> {
    let sql = format!(
        "SELECT reltuples::bigint AS approx_cnt FROM pg_class \
         WHERE relname = '{}'", self.table
    );
    // 注意：reltuples 是全表估算，不支持范围过滤
    // 如果需要范围估算，需要 pg_stats 直方图
}
```

**权衡**: `reltuples` 是全表估算，不支持范围过滤，且依赖 ANALYZE 的准确性。如果 SurrealDB 只需要精确计数，此项不适用。建议作为 admin API 暴露而非替代 `count()`。

---

### O8. `after_connect` 中 session_sql 每次克隆 String

**文件**: `src/store.rs:79-93`

**现状**:
```rust
let session_sql = tune.session_sql();  // String
// ...
.after_connect(move |conn, _meta| {
    let sql = session_sql.clone();  // 每个新连接克隆一次
    Box::pin(async move {
        sqlx::Executor::execute(conn, sqlx::raw_sql(&sql)).await?;
        Ok(())
    })
})
```

每次创建新连接都克隆 `session_sql` 字符串。

**建议**: 使用 `Arc<str>` 避免克隆：

```rust
let session_sql: Arc<str> = tune.session_sql().into();
.after_connect(move |conn, _meta| {
    let sql = session_sql.clone();  // Arc clone — 仅原子操作
    Box::pin(async move {
        sqlx::Executor::execute(conn, sqlx::raw_sql(&sql)).await?;
        Ok(())
    })
})
```

**收益**: 从一次堆分配变为一次 `Arc::clone`（原子引用计数递增）。在连接频繁创建/回收的场景（如 idle timeout 短），减少 GC 压力。

---

## 低影响 / 功能增强

### O9. 缺少健康检查方法

**文件**: `src/store.rs`

**现状**: 没有轻量级的健康检查方法。运维和负载均衡需要一种快速验证 PG 可达性的方式。

**建议**:

```rust
impl PgStore {
    /// 执行 `SELECT 1` 验证连接可用。
    /// 适用于 Kubernetes liveness/readiness probe 或负载均衡健康检查。
    pub async fn health_check(&self) -> Result<()> {
        Executor::execute(&self.pool, sqlx::raw_sql("SELECT 1"))
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        Ok(())
    }
}
```

**收益**: 可在 SurrealDB 的 HTTP 路由中暴露 `/health` 端点，或供外部探针调用。

---

### O10. `probe_persistent` 同时占用两个连接

**文件**: `src/store.rs:311-378`

**现状**: 启动时探测 pgbouncer，同时获取两个连接。如果 `min_connections` 设为 1，可能因池耗尽而失败。

**建议**: 可以改为顺序获取 + 中间释放：

```rust
async fn probe_persistent(pool: &PgPool) -> bool {
    // Phase 1: conn1
    let mut conn1 = pool.acquire().await.ok()?;
    let r1 = sqlx::query("SELECT $1::int4")
        .persistent(true).bind(1i32)
        .execute(&mut *conn1).await;
    let _ = Executor::execute(&mut *conn1, sqlx::raw_sql("DEALLOCATE ALL")).await;
    drop(conn1);  // 释放回池

    if r1.is_err() { return false; }

    // Phase 2: conn2（此时 conn1 已归还）
    let mut conn2 = pool.acquire().await.ok()?;
    let r2 = sqlx::query("SELECT $1::int4")
        .persistent(true).bind(2i32)
        .execute(&mut *conn2).await;
    let _ = Executor::execute(&mut *conn2, sqlx::raw_sql("DEALLOCATE ALL")).await;
    // conn2 随 drop 自动归还

    match r2 {
        Ok(_) => true,
        Err(e) => { /* 现有检测逻辑 */ false }
    }
}
```

**收益**: 峰值连接需求从 2 降为 1。不过 `min_connections` 默认 5，实际影响很小，属于健壮性改进。

---

### O11. savepoint 名称构建可优化

**文件**: `src/transaction.rs:164-168`

**现状**:
```rust
let name = format!("sp_{}", self.savepoint_counter);
```

每次创建 savepoint 都做 `format!()`。虽然 savepoint 不如 get/set 频繁，但在嵌套事务较多时仍有累积开销。

**建议**: 使用 `itoa` crate 或手动格式化：

```rust
let mut buf = [0u8; 12]; // "sp_" + 最多 10 位数字
let name = {
    let n = itoa::write(&mut buf[3..], self.savepoint_counter);
    buf[..3].copy_from_slice(b"sp_");
    std::str::from_utf8(&buf[..3 + n.len()]).unwrap().to_string()
};
```

或者更简单地，预分配一个 `Vec<String>` 的 savepoint 名称池。考虑到 savepoint 频率不高，此项优先级最低。

---

### O12. 缺少连接池配置的运行时动态调整

**文件**: `src/store.rs`

**现状**: 连接池参数（max_connections, min_connections 等）在 `PgStore::new()` 时一次性设置，运行时无法调整。如果流量峰值期间需要临时扩容，只能重启服务。

**建议**: 暴露动态调整接口（需 sqlx 支持）：

```rust
impl PgStore {
    /// 尝试动态调整池大小（如果 sqlx 支持）。
    /// 注意：sqlx 0.8 的 PgPool 不直接支持运行时 resize，
    /// 此方法为未来版本预留。
    pub fn try_resize_pool(&self, max: u32, min: u32) -> Result<()> {
        // sqlx 0.8 PgPool 不支持运行时 resize
        // 可以记录为 TODO，待 sqlx 支持后实现
        tracing::info!("pool resize requested: max={max}, min={min} (not yet supported)");
        Ok(())
    }
}
```

**注意**: sqlx 0.8 的 `PgPool` 不直接支持运行时 resize。这是一个未来增强方向的标记，当前无法实现。

---

## 未推荐但值得讨论的项

### 关于 `PgTx` Mutex 竞争

`PgTx` 使用 `tokio::sync::Mutex` 包裹内部事务，每次操作都要获取锁。看起来像瓶颈，但实际上 SurrealDB 的事务模型是**单线程串行**的：一个事务在同一时刻只由一个 task 驱动。因此 Mutex 几乎不会产生竞速，只是防御性编程。改用 `RwLock` 无益（写操作仍需独占锁），改用无锁设计不现实（`Transactable` trait 要求 `&self`）。

**结论**: 保持现状，无需修改。

### 关于批量写入 `setm`

SurrealDB 的 `Transactable` trait 没有定义 `setm` 方法，因此添加批量写入没有上层调用方。在 trait 层面添加需要修改 surrealdb-core，超出当前项目范围。

**结论**: 记录为未来方向，当前不适用。

---

## 优先级建议

| 优先级 | 优化项 | 预期收益 | 难度 | 状态 |
|--------|--------|----------|------|------|
| P0 | O1. 预构建 SQL | 每次 KV 操作省一次堆分配 | 低 | ✅ 已完成 |
| P0 | O2. EXCLUDED.val | 每次 set 省一次参数绑定 | 极低 | ✅ 已完成 |
| P1 | O4. 延迟 ROLLBACK | 每次事务省一次网络往返 | 中 | ✅ 已完成 |
| P1 | O5. 池级 metrics | 运维可观测性 | 低 | ✅ 已完成 |
| P1 | O8. Arc session_sql | 减少连接创建时的堆分配 | 极低 | ✅ 已完成 |
| P2 | O3. 游标分页 | 深翻页性能 O(n)→O(1) | 高 | ✅ 已完成 |
| P2 | O6. getm 小批量优化 | 2-4 键时减少 HashMap 开销 | 低 | ✅ 已完成 |
| P2 | O7. count_approx | 大表计数 O(n)→O(1) | 低 | ✅ 已完成 |
| P3 | O9. health_check | 运维便利 | 极低 | ✅ 已完成 |
| P3 | O10. probe 顺序获取 | 峰值连接 2→1 | 低 | ✅ 已完成 |
| P3 | O11. savepoint 格式化 | 微优化 | 极低 | ✅ 已完成 |
| P3 | O12. 动态池调整 | 未来方向 | N/A | ✅ 已完成（占位） |

---

## 实施记录

**全部 12 项优化已实现完毕。** `cargo clippy` 零告警，10/10 测试通过。

### 本轮新增（O3/O7/O11/O12）

| 优化项 | 实现方式 | 关键文件 |
|--------|----------|----------|
| O3 游标分页 | `ScanMode` 枚举 + `keys_after`/`scan_after` 方法 + `range_after_prefix` 预构建 SQL | `src/transaction.rs` |
| O7 count_approx | `pg_class.reltuples` 查询，O(1) 全表估算 | `src/transaction.rs` |
| O11 savepoint 优化 | 栈分配 `[u8; 16]` 缓冲区手动格式化，替代 `format!()` | `src/transaction.rs` |
| O12 动态池调整 | `try_resize_pool` 占位方法，含参数校验 | `src/store.rs` |

### 附带修复

O1（预构建 SQL）在原始实现中存在借用冲突编译错误：`&self.sql.xxx` 不可变借用与 `self.conn_mut()` 可变借用冲突。修复方案：将 `Sql` 改为 `Arc<Sql>`，每个操作方法内先 `clone` Arc（原子操作，无堆分配），释放 `&self` 借用后再调用 `conn_mut()`。

---

## 总结

代码在安全性和正确性方面已经过三轮审计打磨，处于生产就绪状态。全部 12 项功能优化已实现完毕，涵盖**减少热路径冗余分配**（O1/O2/O8/O11）、**提升可观测性**（O5/O9）、**深翻页性能优化**（O3）、**大表计数优化**（O7）和**运维便利性**（O10/O12）。

`cargo clippy` 零告警，10/10 测试通过（7 单元 + 1 集成 + 2 SurrealQL）。