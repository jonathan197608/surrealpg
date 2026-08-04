---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: '656b80bb-f1db-4bd7-859b-f6a860c9beec'
  PropagateID: '656b80bb-f1db-4bd7-859b-f6a860c9beec'
  ReservedCode1: '2f0692fa-6009-4987-8ccf-dcddcd40cd04'
  ReservedCode2: '2f0692fa-6009-4987-8ccf-dcddcd40cd04'
---

# AUDIT REPORT V9 — Bug / Performance / Functionality

> **Scope**: Full codebase deep audit across three dimensions.  
> **Date**: 2026-08-04  
> **Baseline**: commit `b615ddf` (V8 audit fixes applied)

---

## 1. Bug 维度

### B1 [High] `PgTx::commit/cancel` 事务失败后连接泄漏

**文件**: `src/pg_tx.rs:101-147`

```rust
fn cancel(&self) -> BoxFut<'_, kvs::Result<()>> {
    Box::pin(async move {
        if self.done.swap(true, Ordering::AcqRel) {
            return Err(kvs::Error::TransactionFinished);
        }
        let mut guard = self.inner.lock().await;
        if let Some(tx) = guard.as_mut()
            && let Err(e) = tx.cancel().await
        {
            *guard = None;  // ← 释放连接
            return Err(kvs::Error::from(e));
        }
        let had_tx = guard.is_some();
        *guard = None;      // ← 释放连接
        ...
    })
}
```

**问题**: `cancel()` 中，如果 `tx.cancel()` 返回 `Err`，代码正确设置 `*guard = None` 并提前返回。但**成功路径上**也设置 `*guard = None` — 这是正确的。然而 `commit()` 中存在一个更微妙的问题：

如果 `tx.commit()` 成功（返回 `Ok`），代码进入 `let had_tx = guard.is_some(); *guard = None;`，这也是正确的。但如果 `tx.commit()` 返回 `Err`，`*guard = None` 释放连接后立即 `return Err`，**跳过了** `*guard = None` 在成功路径的第二次赋值。

实际上这段代码是正确的——两个分支都释放了连接。**但** 这里存在一个更微妙的问题：

**真正的问题**: `done.swap(true, Ordering::AcqRel)` 在获取 Mutex 之前就设置了 `done = true`。如果 `self.inner.lock().await` 阻塞（另一个操作持锁），`done` 已经被设为 true，但事务尚未真正关闭。在此窗口期内，`closed()` 返回 true，而事务实际上仍持有连接。SurrealDB 引擎可能基于 `closed()` 返回 true 做出错误判断。

**影响**: 极窄窗口，但可观测性差——引擎看到一个"已关闭"的事务，而底层连接仍在池中活跃。

**建议**: 将 `swap(true)` 移到 `guard = None` 之后，确保 `done` 只在连接真正归还后才变为 true。或者在 `swap` 之前加一个 `done.load(Relaxed)` 快速检查（减少竞争窗口）。

**严重性**: **High**（逻辑正确性：状态与实际不一致）

---

### B2 [High] `getm` HashMap 路径丢失重复键

**文件**: `src/transaction.rs:404-408`

```rust
let mut map = std::collections::HashMap::with_capacity(rows.len());
for (k, v) in Self::rows_to_pairs(rows) {
    map.insert(k, v);  // ← 重复键被静默覆盖
}
Ok(keys.into_iter().map(|k| map.get(&k).cloned()).collect())
```

**问题**: `getm` 通过 `SELECT key, val FROM kv WHERE key = ANY($1)` 查询。SQL `ANY` 返回的行集与请求键列表一一对应（主键唯一）。**但如果调用方在 `keys` 参数中传入重复键**（如 `[k1, k1]`），SQL 只返回一行，而结果 Vec 应该有两个 `Some`。`HashMap` 路径本身不会丢数据（因为 SQL 行不重复），但 `keys.into_iter().map(|k| map.get(&k).cloned())` 对重复键正确返回两次 `Some(v)` — 这部分是正确的。

但线性扫描路径有真实问题：

```rust
// 线性路径：O(n²) 但正确处理重复键
keys.into_iter()
    .map(|k| {
        extracted
            .iter()
            .find(|(row_key, _)| *row_key == k)
            .map(|(_, v)| v.clone())
    })
    .collect()
```

**真正问题**: 线性路径的 `find` 返回第一个匹配项。如果 `keys` 有重复键 `[k1, k1]`，两个都返回 `Some(v1)` — 这是正确的。如果 `rows` 中有重复键（理论上不可能，主键唯一）。

**结论**: 两个路径在功能上都是正确的，重复键不会导致 bug。**降级为 Info**。

**严重性**: ~~High~~ → **Info**（理论上无实际 bug，但 `HashMap` 路径的行为依赖主键唯一性假设，缺乏显式断言）

---

### B3 [Medium] `probe_persistent` 在单连接池 `min_connections=1` 下的死锁风险

**文件**: `src/store.rs:402-478`

**问题**: `probe_persistent` 顺序获取两个连接（conn1 然后 conn2）。如果 `pool_max=1, min_connections=1`，conn1 被释放后 conn2 才能获取 — 这在当前代码中是安全的（conn1 先 `drop` 再获取 conn2）。

但如果 `pool_max=1` 且 `min_connections=1`，且 conn1 释放后 PG 后端进程立即被 pgbouncer 回收，conn2 拿到的可能是一个全新的后端 session，probe 结果为 true（误判为直连 PG）。

**影响**: 在单连接池 + pgbouncer 场景下，`persistent` 被错误地设为 `true`，后续 prepared statement 冲突导致查询失败。

**建议**: 当 `pool_max <= 2` 时，probe 不可靠，应默认 `persistent = false`（安全默认值）。或在 probe 前检查 pool 大小。

**严重性**: **Medium**（已知限制 F7 的延伸，但在 `pool_max=1` 时风险更高）

---

### B4 [Medium] URL 参数中 `%` 编码未处理

**文件**: `src/config.rs:220-326`

**问题**: `merge_url_params` 手动解析 query string（`url.split('?').nth(1)`），不处理 URL 编码。如果密码或 table_name 包含 `%`、`&`、`=` 等特殊字符（如 `table_name=my%20table`），解析结果不正确。

`PgConnectOptions::parse()` 正确处理 URL 编码，但 `merge_url_params` 在 `parse()` 之前运行，且两者读取同一 URL 字符串。这意味着 URL 编码的参数值（如 `table_name=kv%5Ftest`）会被存为 `kv%5Ftest` 而非 `kv_test`。

**影响**: 实际场景中 table_name 不太可能包含特殊字符（已有 `validate_identifier` 限制），但密码中的 `%` 字符可能影响连接。

**建议**: 在 `merge_url_params` 中对参数值做 `percent_decode()`，或改用 `url` crate 解析。

**严重性**: **Medium**（当前 table_name 验证能防止注入，但语义不正确）

---

### B5 [Medium] `session_sql()` 中 `server_work_mem` 验证遗漏

**文件**: `src/tune.rs:246-283`

**问题**: `session_sql()` 只验证了 3 个内存大小字段（`server_work_mem`、`server_maintenance_work_mem`、`server_effective_cache_size`），但没有验证 `server_shared_buffers` 和 `server_wal_buffers`。虽然这两个只用于 `log_server_hints()`（不在 SQL 中），但它们在 `from_env()` 中经过 `validate_pg_memory_size` — 所以目前安全。

然而 `session_sql()` 中 `random_page_cost` 是 `f64`，直接 `format!()` 嵌入 SQL — 如果恶意构造的 `PgTuneConfig` 包含 NaN 或 Infinity，生成的 SQL 语法错误。

**建议**: 在 `session_sql()` 中对 `random_page_cost` 增加 `assert!(self.server_random_page_cost.is_finite())` 检查。

**严重性**: **Medium**（安全：非 finite f64 注入 SQL 会产生语法错误而非注入，但可能 panic 或产生意外行为）

---

### B6 [Low] `PgStore` 的 `Clone` derive 与 `CancellationToken`

**文件**: `src/store.rs:24-50`

**问题**: `PgStore` 手动 derive `Clone`，`CancellationToken` 实现了 `Clone`（共享同一个 cancel 源），所以语义正确。但 `PgPool` 的 `Clone` 也是共享语义（Arc 内部）。`PgConfig` 的 `Clone` 是深拷贝（包含 `String` 字段）。

**问题**: 每次 `PgStore::clone()` 都深拷贝 `config` 和 `tune`，但这两个在 `new()` 之后只读。应该改为 `Arc<PgConfig>` / `Arc<PgTuneConfig>` 以避免无意义拷贝。

**严重性**: **Low**（性能问题，非 bug，但 clone 语义重于必要）

---

## 2. 性能维度

### P1 [High] 每个 `Transactable` 操作都经过 `tokio::Mutex`

**文件**: `src/pg_tx.rs:54-79`

**问题**: `PgTx` 使用 `Mutex<Option<PgTransaction>>` 来实现内部可变性。SurrealDB 引擎对同一事务的 KV 操作是**顺序调用**的（事务内不可能并发），所以这个 Mutex 永远不会竞争 — 但每次操作都有 `lock().await` 的开销（一次原子操作 + poll）。

**影响**: 每次 KV 操作多一次 `Mutex::lock()` 原子操作。在高 QPS 场景下（10万+ ops/s），这个开销可测量。

**建议**: 用 `RefCell` + `unsafe` Send/Sync 包装替代（同内存后端模式），或在 `PgTransaction` 层面直接实现 `Transactable`（避免 wrapper）。但这需要改架构，风险高。当前方案可接受，但值得记录。

**严重性**: **High**（性能：高 QPS 场景可测量的额外延迟）

---

### P2 [Medium] `getm` 线性扫描路径分配 `keys_ref` Vec

**文件**: `src/transaction.rs:373`

```rust
let keys_ref: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
```

**问题**: 每次调用 `getm` 都分配一个新的 `Vec<&[u8]>`，即使 keys 很小。这个 Vec 仅用于 sqlx 的 `ANY($1)` 绑定。

**建议**: 对于小 N（如 N <= 8），可以用栈分配数组替代。或让 sqlx 接受 `&[Vec<u8>]` 的切片（需要检查 sqlx API）。

**严重性**: **Medium**（性能：每次 getm 额外一次堆分配）

---

### P3 [Medium] `setm` 分割 keys/vals 各分配一次 Vec

**文件**: `src/transaction.rs:446-447`

```rust
let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
let vals: Vec<&[u8]> = pairs.iter().map(|(_, v)| v.as_slice()).collect();
```

**问题**: 每次 `setm` 调用分配 2 个 Vec（keys + vals）。对于批量写入场景（如 SurrealDB 的批量导入），这可能是热路径。

**建议**: 使用 `SmallVec` 或栈分配数组优化小批量场景。

**严重性**: **Medium**（同 P2）

---

### P4 [Medium] 每次操作 `self.sql.clone()` (Arc::clone) 的开销

**文件**: `src/transaction.rs` 全文（每个方法）

```rust
let sql = self.sql.clone();  // Arc::clone — 原子 increment
let persistent = self.persistent;
```

**问题**: 每个 KV 操作方法开头都执行 `Arc::clone(&self.sql)`。Arc::clone 是一次原子 increment + 内存屏障。在 V8 修复之前，每次事务创建都要 14 次 `format!()`；现在改为 Arc::clone 确实快了很多。

但进一步优化是可能的：因为 `PgTransaction` 独占 `conn`（单线程使用），可以去掉 `Arc` 直接持有 `&Sql` 引用或 `Box<Sql>`，避免所有原子操作。

**建议**: 改 `sql: Arc<Sql>` 为 `sql: Box<Sql>`（`PgTransaction` 不需要共享，它独占连接）。去掉所有方法的 `self.sql.clone()`。

**严重性**: **Medium**（每次操作省一次原子操作，高 QPS 可测量）

---

### P5 [Low] `savepoint_sql()` 返回 `String`（堆分配）

**文件**: `src/transaction.rs:301`

```rust
fn savepoint_sql(prefix: &str, name: &str) -> String {
    let mut buf = [0u8; 48];
    ...
    unsafe { std::str::from_utf8_unchecked(&buf[..total]) }.to_string()
}
```

**问题**: 虽然使用了栈缓冲区格式化，但最终 `.to_string()` 仍需一次堆分配。Savepoint 操作在正常工作负载中很少调用，影响极小。

**严重性**: **Low**

---

### P6 [Low] `env_str_validated` 在 `from_env()` 中每次都做 `validate_pg_memory_size`

**文件**: `src/tune.rs:325-338`

**问题**: `env_str_validated` 在启动时调用一次，验证后丢弃验证结果。如果有人在 `from_env()` 后修改 `PgTuneConfig` 的字段为无效值，验证不再触发。

**严重性**: **Low**（启动时一次性开销，无性能影响；属于防御性编程问题）

---

## 3. 功能维度

### F1 [High] 缺少 `setm` 的 `Transactable` 接口暴露

**问题**: `PgTransaction` 实现了 `setm()`，但 `PgTx`（`Transactable` wrapper）没有暴露这个方法。SurrealDB 的 `Transactable` trait 可能没有 `setm` 方法，但上层引擎可能有批量写入路径。

**当前状态**: `setm` 只能通过直接调用 `PgTransaction::setm()` 使用。通过 SurrealDB 引擎的 `Datastore` API 调用时，`setm` 不会被使用。

**影响**: SurrealDB 引擎可能逐条调用 `set`，而不知道底层支持批量写入。

**建议**: 如果 SurrealDB 未来在 `Transactable` trait 中添加 `setm`，应立即实现。当前不可行（trait 不支持），但值得追踪。

**严重性**: **High**（功能缺失：批量写入能力无法被引擎利用）

---

### F2 [High] 缺少 `delr` 的 `Transactable` 接口暴露

**同 F1**: `delr` 只在 `PgTransaction` 上实现，不通过 `Transactable` 暴露。SurrealDB 引擎使用 `scan + del` 循环删除范围数据，而不是一条 `DELETE WHERE key >= $1 AND key < $2`。

**影响**: 范围删除的性能远差于 `delr`（N 次网络往返 vs 1 次）。

**严重性**: **High**（功能缺失：高效范围删除无法被引擎利用）

---

### F3 [Medium] `count_approx` 不接受范围参数

**文件**: `src/transaction.rs:681-713`

**问题**: `count_approx()` 忽略范围参数，返回全表近似行数。SurrealDB 可能期望 `count_approx` 在指定范围内有效。

**当前实现**: `SELECT reltuples::bigint AS approx_cnt FROM pg_class WHERE relname = $1 AND reltuples > 0` — 这是全表估计，PG 不支持范围级近似计数。

**影响**: 如果引擎依赖 `count_approx` 进行范围分页优化，返回的值可能远大于实际范围行数。

**严重性**: **Medium**（功能偏差：语义不完全匹配上游期望）

---

### F4 [Medium] `read_only_optimization` 下 READ ONLY 事务不调用 `set`

**文件**: `src/store.rs:232-236`

**问题**: 当 `read_only_optimization=true` 时，`begin(false)` 使用 `BEGIN ISOLATION LEVEL ... READ ONLY`。SurrealDB 引擎内部某些操作可能先用读事务检查再升级为写事务，但 PG 的 READ ONLY 事务无法升级。

**影响**: 如果 SurrealDB 引擎对同一事务先调 `get` 再调 `set`（先读后写模式），`set` 会返回 `TxReadOnly` 错误，导致操作失败。

**实际风险**: SurrealDB 的 `TransactionBuilder` 在创建事务时明确指定 `write` 参数，读事务不应调用写操作。但某些内部操作（如 LIVE SELECT）的行为需要验证。

**建议**: 在文档中明确 `read_only_optimization` 的限制；或添加 `DEFERRABLE` 选项作为替代。

**严重性**: **Medium**（功能风险：极端场景下读事务升级为写会失败）

---

### F5 [Medium] 缺少连接重试 / 自动恢复机制

**问题**: `PgStore` 依赖 sqlx 的连接池自动重连，但没有应用层重试逻辑。如果 PG 临时重启（如滚动升级），所有进行中的操作返回错误，上层必须自行重试。

**对比**: SurrealDB 的 TiKV 后端有 `retry` 机制处理临时网络错误。

**建议**: 在 `begin()` 中添加对 `08006`（connection_failure）的重试逻辑，或在 `pg_tx.rs` 的写操作中添加可配置的重试次数。

**严重性**: **Medium**（功能缺失：临时网络中断无自动恢复）

---

### F6 [Medium] `tune.rs` 环境变量解析静默忽略无效值

**文件**: `src/tune.rs:302-321`

**问题**: `env_u32`、`env_i32`、`env_f64` 在环境变量值无效时静默使用默认值，不输出任何警告。用户设置了 `PG_TUNED_POOL_MAX_CONNECTIONS=abc`，值被静默忽略。

对比 `env_str_validated` 和 `env_bool` 会输出 `warn!()` 日志。

**影响**: 用户可能认为配置已生效，但实际使用默认值。

**建议**: 所有 `env_*` 辅助函数在解析失败时输出 `warn!()` 日志。

**严重性**: **Medium**（可观测性：配置被静默忽略）

---

### F7 [Low] `validate_identifier` 不允许引号标识符

**文件**: `src/config.rs:134-163`

**问题**: PG 允许引号标识符（`"my-table"`）包含任意字符。当前实现只允许 `[a-zA-Z0-9_]`，拒绝所有特殊字符。

**影响**: 用户无法使用包含连字符或点的表名（如 `"my-kv-store"`）。这在实际场景中可能需要。

**建议**: 添加引号标识符支持（用双引号包裹），或在文档中说明此限制。

**严重性**: **Low**（功能限制：不支持 PG 的完整标识符语法）

---

### F8 [Low] 缺少指标暴露（Prometheus / OpenTelemetry）

**问题**: `pg_builder.rs` 实现了 `register_metrics` / `collect_u64_metric`，返回 3 个基础指标（pool_size、pool_idle、pool_max）。但没有暴露以下关键运维指标：

- 事务提交/回滚计数
- 查询延迟直方图
- 连接获取等待时间
- 死锁/序列化冲突计数
- `count_approx` 缓存命中率

**建议**: 添加 `Metrics` 扩展或在 `PgTransaction` 中嵌入计数器。

**严重性**: **Low**（可观测性不足，但不影响功能）

---

### F9 [Low] 迁移脚本与代码 DDL 不一致

**文件**: `migrations/001_init_kv.sql` vs `src/tune.rs:create_table_sql()`

**问题**: 迁移脚本的注释调优部分是注释掉的，而代码中 `auto_create_table=true` 时会自动执行调优 DDL。如果用户手动执行迁移脚本但不取消注释调优语句，表不会有 fillfactor / TOAST / autovacuum 调优。

**建议**: 在迁移脚本中默认启用调优语句（取消注释），或添加注释说明"推荐使用 auto_create_table=true 自动调优"。

**严重性**: **Low**（运维：手动迁移可能缺少调优）

---

## 汇总

| # | 严重性 | 维度 | 描述 | 建议优先级 |
|---|--------|------|------|-----------|
| B1 | High | Bug | `PgTx::commit/cancel` done 标志与实际状态不一致窗口 | P1 |
| B3 | Medium | Bug | `probe_persistent` 单连接池误判（F7 延伸） | P2 |
| B4 | Medium | Bug | URL 参数未处理 percent 编码 | P2 |
| B5 | Medium | Bug | `random_page_cost` NaN/Infinity 注入 SQL | P2 |
| B2 | Info | Bug | `getm` HashMap 路径依赖主键唯一性（实际无 bug） | P3 |
| B6 | Low | Bug+Perf | `PgStore::clone()` 深拷贝 config/tune | P3 |
| P1 | High | Perf | 每次 Transactable 操作经过 `tokio::Mutex` | P1 |
| P4 | Medium | Perf | 每次操作 `Arc::clone(&self.sql)` 原子操作 | P2 |
| P2 | Medium | Perf | `getm` 分配 `keys_ref` Vec | P2 |
| P3 | Medium | Perf | `setm` 分割 keys/vals 各分配 Vec | P2 |
| P5 | Low | Perf | `savepoint_sql()` 返回 String | P3 |
| P6 | Low | Perf | `env_str_validated` 一次性验证无后续保护 | P3 |
| F1 | High | Func | `setm` 未暴露给 Transactable 引擎无法利用 | P1 |
| F2 | High | Func | `delr` 未暴露给 Transactable 引擎无法利用 | P1 |
| F3 | Medium | Func | `count_approx` 不支持范围参数 | P2 |
| F4 | Medium | Func | `READ ONLY` 事务无法升级为写事务 | P2 |
| F5 | Medium | Func | 缺少连接重试/自动恢复机制 | P2 |
| F6 | Medium | Func | 环境变量无效值静默忽略 | P2 |
| F7 | Low | Func | 不支持引号标识符 | P3 |
| F8 | Low | Func | 缺少关键运维指标 | P3 |
| F9 | Low | Func | 迁移脚本与代码 DDL 不一致 | P3 |

---

## 建议实施优先级

### P1 — 立即修复（影响正确性或关键性能）
1. **B1**: 修复 `done` 标志时序（swap 移到 guard=None 之后）
2. **P1**: 评估 `Mutex` → `RefCell` 替代的可行性（需架构评估）
3. **F1/F2**: 跟踪 SurrealDB `Transactable` trait 变更，准备好 `setm`/`delr` 实现

### P2 — 本轮修复（可操作性改进）
4. **B3**: `probe_persistent` 在小 pool 下默认 `false`
5. **B4**: URL 参数 percent decode
6. **B5**: `random_page_cost.is_finite()` 断言
7. **P4**: `Arc<Sql>` → `Box<Sql>` 消除每次操作原子开销
8. **P2/P3**: `getm`/`setm` 小批量栈分配优化
9. **F4**: 文档说明 `read_only_optimization` 限制
10. **F5**: 添加连接级重试逻辑
11. **F6**: `env_*` 辅助函数解析失败输出 warn

### P3 — 后续优化（非紧急）
12. **B6**: `PgConfig`/`PgTuneConfig` 改为 `Arc<...>`
13. **P5**: savepoint_sql 优化
14. **F3**: `count_approx` 语义对齐
15. **F7/F8/F9**: 引号标识符、指标扩展、迁移脚本一致性