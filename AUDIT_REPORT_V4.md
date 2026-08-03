---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: 'dda25d00-5307-4100-91c4-112cb20b845a'
  PropagateID: 'dda25d00-5307-4100-91c4-112cb20b845a'
  ReservedCode1: 'd9e4a4fc-bcd9-484d-96c3-c68b6933bfb5'
  ReservedCode2: 'd9e4a4fc-bcd9-484d-96c3-c68b6933bfb5'
---

# surreal-pg 全面审计报告 V4

**日期**: 2026-08-03
**范围**: 遗留问题 + 功能/性能优化建议
**基线**: 三轮安全审计 + 12 项优化全部完成后的代码状态

---

## 构建与测试验证

| 检查项 | 结果 |
|--------|------|
| `cargo check` | 零警告零错误 |
| `cargo clippy` | 零警告 |
| `cargo test` | 10/10 通过（7 单元 + 1 集成 + 2 SurrealQL） |
| `src/` 中 `panic!` | 0 |
| `src/` 中 `unwrap()` | 0 |
| `src/` 中 `.expect()` | 1（savepoint UTF-8 — 合理，仅用于不可能失败的转换） |

---

## 历史修复状态总览

三轮安全审计发现 19 项问题 + 12 项优化建议，**全部已修复/已实现**。

| 轮次 | 发现 | 修复 |
|------|------|------|
| 第一轮（安全） | 17 项（2 Critical, 3 High, 7 Medium, 5 Low） | 17/17 |
| 第二轮（安全） | 2 项新发现（1 Medium, 1 Low） | 2/2 |
| 第三轮（安全） | 0 项遗留 | — |
| 优化审阅 | 12 项（3 高, 5 中, 4 低） | 12/12 |

---

## 本轮新发现

### 遗留问题

#### R1 (Low) — `count_approx` 使用字符串拼接而非参数化查询

**文件**: `src/transaction.rs:562-567`

```rust
pub async fn count_approx(&mut self) -> Result<Option<u64>> {
    let table = &self.sql.table_name;
    let sql = format!(
        "SELECT reltuples::bigint AS approx_cnt FROM pg_class \
         WHERE relname = '{}' AND reltuples > 0",
        table
    );
```

`table_name` 在 `config.rs` 的 `validate_identifier` 中已校验仅含 `[a-zA-Z0-9_]`，所以当前不构成注入风险。但此方法每次调用都执行 `format!()` 堆分配，且未使用参数化查询。

**建议**:
1. 预构建 SQL 并存储在 `Sql` 结构体中（与 O1 一致），使用参数化绑定 `$1`：
   ```rust
   // 在 Sql::new() 中：
   count_approx: format!("SELECT reltuples::bigint AS approx_cnt FROM pg_class WHERE relname = $1 AND reltuples > 0"),
   ```
   ```rust
   // count_approx 方法：
   let row = Self::build_query(persistent, &self.sql.count_approx)
       .bind(&self.sql.table_name)
       .fetch_optional(self.conn_mut()?)
       .await...
   ```

**收益**: 消除每次调用的 `format!()` 堆分配 + 允许 PG 缓存查询计划。

---

#### R2 (Low) — `begin()` 和 `probe_persistent()` 通过字符串匹配检测错误

**文件**: `src/store.rs:214-217`, `src/store.rs:414-416`

```rust
// begin() — 检测残留事务
let err_str = e.to_string().to_ascii_lowercase();
let is_tx_active = err_str.contains("already a transaction")
    || err_str.contains("25p01")
    || err_str.contains("25p02");
```

```rust
// probe_persistent() — 检测 pooler
let err_str = e.to_string().to_ascii_lowercase();
let is_pooler = err_str.contains("already exists")
    || err_str.contains("duplicate_prepared_statement")
    || err_str.contains("prepared statement") && err_str.contains("does not exist");
```

通过 `e.to_string()` 获取错误文本再做 `contains()` 匹配。这有两个问题：
1. PG 错误消息文本可能随版本变化，`contains("already a transaction")` 是脆弱的
2. `error.rs` 已有 `from_sqlx` 函数通过 SQLSTATE 码做结构化匹配，但这两处未使用

**建议**: 使用 `sqlx::Error::Database(db_err)` 模式匹配 + `db_err.code()`：

```rust
// begin() 中：
Err(e) => {
    let is_tx_active = matches!(&e, sqlx::Error::Database(db) if {
        let code = db.code().unwrap_or_default();
        code == "25P01" || code == "25P02"
    });
    if is_tx_active {
        // 清理并重试
    } else {
        return Err(PgStoreError::from_sqlx(None, &e));
    }
}
```

**收益**: 不依赖错误消息文本，跨 PG 版本可靠。

---

#### R3 (Low) — `probe_persistent` 逻辑表达式缺少显式括号

**文件**: `src/store.rs:416`

```rust
let is_pooler = err_str.contains("already exists")
    || err_str.contains("duplicate_prepared_statement")
    || err_str.contains("prepared statement") && err_str.contains("does not exist");
```

由于运算符优先级（`&&` 高于 `||`），实际等价于：
```
A || B || (C && D)
```
这在语义上是正确的，但可读性差且容易在后续维护中被误改。

**建议**: 加括号明确意图：
```rust
let is_pooler = err_str.contains("already exists")
    || err_str.contains("duplicate_prepared_statement")
    || (err_str.contains("prepared statement") && err_str.contains("does not exist"));
```

---

### 功能/性能优化建议

#### P1 — `count_approx` SQL 预构建 + 参数化

同 R1，既是遗留问题也是优化点。将 SQL 移入 `Sql::new()`，用 `$1` 绑定表名。

**影响**: 低频操作（admin），但每次调用省一次 `format!()` + 允许计划缓存。

---

#### P2 — `savepoint` SQL 仍使用 `format!()`

**文件**: `src/transaction.rs:581, 592, 603, 605`

```rust
self.execute_simple(&format!("SAVEPOINT {name}"), None).await?;
self.execute_simple(&format!("RELEASE SAVEPOINT {name}"), None).await?;
self.execute_simple(&format!("ROLLBACK TO SAVEPOINT {name}"), None).await?;
self.execute_simple(&format!("RELEASE SAVEPOINT {name}"), None).await?;
```

savepoint 名称已用栈分配的 `[u8; 16]` 缓冲区构建（O11），但 SQL 语句本身仍用 `format!()`。

**建议**: savepoint 是低频操作，当前实现可接受。如需优化，可改为手动拼接：

```rust
let mut buf = [0u8; 48]; // "SAVEPOINT sp_" + 10 digits
buf[..10].copy_from_slice(b"SAVEPOINT ");
// ... 复用 push_savepoint_name 的数字写入逻辑 ...
self.execute_simple(std::str::from_utf8(&buf[..pos]).unwrap(), None).await?;
```

**优先级**: 极低。savepoint 在嵌套事务中频率远低于 get/set。

---

#### P3 — `getm` 线性扫描阈值应考虑 `keys.len()`

**文件**: `src/transaction.rs:320-336`

```rust
if rows.len() <= 64 {
    Ok(keys.into_iter()
        .map(|k| {
            rows.iter()
                .find(|r| r.get::<Vec<u8>, _>("key") == k)
                .map(|r| r.get::<Vec<u8>, _>("val"))
        })
        .collect())
}
```

当前阈值仅检查 `rows.len() <= 64`。如果请求 10000 个 key 但只有 50 个存在（`rows.len() = 50`），会使用线性扫描，复杂度为 O(keys * rows) = 500,000 次比较。虽然每次比较只是 `Vec<u8>` 的 `==`，在大批量场景下仍有累积开销。

**建议**: 考虑 `keys.len() * rows.len()` 的乘积，或同时检查两个维度：

```rust
let use_linear = rows.len() <= 64 && keys.len() <= 256;
// 或者：
let use_linear = rows.len().saturating_mul(keys.len()) <= 8192;
```

**实际影响**: 低。SurrealDB 的 `getm` 调用通常批量获取少量 key（record fetch by ID），大批量场景罕见。

---

#### P4 — 缺少新增方法的测试

**文件**: `tests/integration_test.rs`

当前测试覆盖了基本 CRUD、put、range scan、savepoint、putc、namespace、exists+getm、delc、keys direction、read-only rejection（10 个用例）。

但以下优化后新增的方法没有测试覆盖：
- `count_approx()` — O7 新增
- `health_check()` — O9 新增
- `vacuum()` — 已有但未测试
- `pool_size()` — O5 新增
- `try_resize_pool()` — O12 新增

**建议**: 添加测试用例：

```rust
("count_approx accuracy", test_count_approx),
("health check", test_health_check),
("pool size reporting", test_pool_size),
```

---

#### P5 — 迁移脚本与运行时建表不一致

**文件**: `migrations/001_init_kv.sql`

迁移脚本只创建最基本的表：
```sql
CREATE TABLE IF NOT EXISTS kv (key BYTEA PRIMARY KEY, val BYTEA NOT NULL);
```

而 `PgStore::new()` 在 `auto_create_table = true` 时会执行 `create_table_sql()` + `tune_table_sql()`，后者包含 fillfactor、TOAST storage、autovacuum 调优等。

如果用户使用迁移工具（如 `sqlx migrate` 或 Flyway）而非让 surreal-pg 自动建表，会得到一个未调优的表。

**建议**: 在迁移脚本中添加注释说明调优参数，或提供 `001_init_kv_tuned.sql` 作为替代：

```sql
-- Note: For optimal performance, also apply table tuning:
-- ALTER TABLE kv SET (fillfactor = 90);
-- ALTER TABLE kv ALTER COLUMN val SET STORAGE external;
-- ... etc (see PgTuneConfig defaults)
```

---

#### P6 — `vacuum()` SQL 使用 `format!()` 拼接

**文件**: `src/store.rs:284`

```rust
let sql = format!("VACUUM ANALYZE {}", self.config.table_name);
```

VACUUM 不支持参数化绑定（PG 限制），所以必须拼接。`table_name` 已被 `validate_identifier` 校验，安全无虞。但为完整性起见，可以预构建或加注释说明安全保证。

**建议**: 可接受现状，或预构建并存储为 `PgStore` 的字段（与 O1 一致）。低优先级。

---

#### P7 — 考虑为 `PgTransaction::Drop` 添加主动 ROLLBACK

**文件**: `src/transaction.rs:618-626`

当前 `Drop` 仅打日志：
```rust
impl Drop for PgTransaction {
    fn drop(&mut self) {
        if !self.closed {
            warn!("PgTransaction dropped without explicit commit/cancel; PG will auto-rollback");
        }
    }
}
```

连接归还连接池后，如果有活跃事务，会占用后端进程直到 `idle_in_transaction_session_timeout`（默认 60s）超时。`begin()` 中的延迟 ROLLBACK 是下一使用者时的安全网，但如果连接长时间空闲，这 60 秒内会占用后端资源。

**权衡**: `Drop` 中无法执行 async 操作（`self.conn.take()` 后连接归还到池是同步的）。sqlx 的 `PoolConnection::drop` 会同步归还连接。如果要在 Drop 中 ROLLBACK，需要 blocking runtime 或 spawn。这增加了复杂性，且 SurrealDB 的事务模型通常保证显式 commit/cancel。

**建议**: 可接受现状。如果改为悲观策略，可在 `PgTx::cancel()` 和 `commit()` 中确保 `*guard = None` 已正确释放连接。当前实现已正确处理。

---

## 架构评估

### 设计优势

1. **委托模式（PostgresComposer）**: 干净地拦截 PG 路径，其余 fall through 到 CommunityComposer，不影响其他后端。
2. **5 层 26 参数调优系统**: 覆盖池、表、autovacuum、查询运行时、PG 服务器，全面且分层合理。
3. **persistent prepared statement 自动探测**: 通过命名冲突检测 pgbouncer/Supavisor，优雅且无需额外配置。
4. **延迟 ROLLBACK**: `begin()` 中先尝试 BEGIN，仅在检测到残留事务时才 ROLLBACK + 重试，正常路径省一次网络往返。
5. **Arc<Sql> 模式**: 预构建 SQL + Arc 共享，消除热路径堆分配，同时避免借用冲突。
6. **三层错误映射**: `PgStoreError` → `surrealdb_core::kvs::Error`，保留语义信息。

### 可接受的现状

| 项 | 说明 |
|----|------|
| `PgTx` Mutex | SurrealDB 事务单线程串行，Mutex 几乎无竞争 |
| OFFSET 分页 | SurrealDB 默认游标（`DefaultKeysCursor`）通过推进 `range.start` 实现 keyset 分页，OFFSET 仅在首批使用 |
| 无自动重试 | 死锁/序列化失败由 SurrealDB 引擎层处理，非存储后端职责 |
| `try_resize_pool` 占位 | sqlx 0.8 不支持运行时 resize，占位是合理设计 |

---

## 优先级总结

| 优先级 | 编号 | 描述 | 影响 | 难度 | 状态 |
|--------|------|------|------|------|------|
| P1 | R2 | 结构化错误匹配替代字符串匹配 | 跨版本可靠性 | 低 | ✅ 已完成 |
| P2 | R1/P1 | `count_approx` 预构建 + 参数化 | 安全 + 微性能 | 低 | ✅ 已完成 |
| P2 | R3 | `probe_persistent` 加括号 | 可读性 | 极低 | ✅ 已完成（通过 R2 重构消除） |
| P3 | P4 | 补充测试用例 | 测试覆盖 | 低 | 待办 |
| P3 | P3 | `getm` 阈值考虑 keys.len() | 大批量边缘场景 | 低 | ✅ 已完成 |
| P4 | P2 | savepoint SQL 去 format!() | 微优化 | 低 | ✅ 已完成 |
| P4 | P5 | 迁移脚本调优注释 | 运维便利 | 极低 | ✅ 已完成 |
| P4 | P6 | vacuum SQL 预构建 | 一致性 | 极低 | ✅ 已完成 |
| — | P7 | Drop 主动 ROLLBACK | 不建议改 | — | 不实施 |

---

## 结论

代码经过三轮安全审计 + 12 项优化后，已达到**生产就绪**质量水平。本轮审计未发现安全漏洞或逻辑 Bug，仅发现 3 项低优先级代码质量改进（R1-R3）和 7 项功能/性能优化建议（P1-P7），其中大部分为微优化或边缘场景。

**最值得做的两件事**：
1. **R2** — 将 `begin()` 和 `probe_persistent()` 的错误检测从字符串匹配改为 SQLSTATE 结构化匹配，提高跨 PG 版本可靠性 ✅
2. **R1/P1** — 将 `count_approx` 改为预构建参数化查询，消除 `format!()` + 允许计划缓存 ✅

整体代码结构清晰，设计合理，注释详尽，测试覆盖核心场景。建议后续迭代优先补齐新增方法的测试覆盖。