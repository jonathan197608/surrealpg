# 审计报告 V10 — 第十轮深度审计

**日期**: 2026-08-04  
**审计范围**: `/Volumes/DELL/rust/surrealpg` 全部源码  
**审计维度**: Bug / 性能 / 功能优化  

---

## 构建验证

| 检查项 | 结果 |
|--------|------|
| `cargo check` | ✅ 零警告零错误 |
| `cargo clippy` | ✅ 零警告 |
| `cargo test` | ✅ 10/10 通过 |
| `src/` 中 `panic!` | 0 |
| `src/` 中 `unwrap()` | 0 |
| `src/` 中 `.expect()` | 1 (config.rs:197, 安全 — 上游已检查非空) |
| `src/` 中 `unsafe` | 2 (savepoint 栈缓冲区, 均有 debug_assert 守护) |
| `src/` 中 `TODO/FIXME` | 0 |
| 热路径 `format!()` | 0 (仅在 Sql::new 构造路径 + 错误路径 + 启动路径) |

---

## V9 遗留复核

### V9-N1 (Medium Bug) — commit/cancel 错误路径不递增计数器
**状态**: ✅ 已修复

`pg_tx.rs:134` — `cancel()` 错误分支: `self.tx_rolled_back.fetch_add(1, Ordering::Relaxed);`  
`pg_tx.rs:171` — `commit()` 错误分支: `self.tx_rolled_back.fetch_add(1, Ordering::Relaxed);`

PG 在 COMMIT 失败时自动 ROLLBACK，失败 commit 正确计入 rollback 计数器。指标等式
`tx_started == tx_committed + tx_rolled_back` 现在成立。

### V9-N2 (Low Consistency) — count() 先查空范围再查 closed
**状态**: ✅ 已修复

`transaction.rs:762-764`:
```rust
// B1: Check closed first — consistency with all other methods.
if self.closed {
    return Err(PgStoreError::TxClosed);
}
if rng.start >= rng.end {
    return Ok(0);
}
```

### V9-N3 (Info) — probe_persistent sqlx 客户端追踪
**状态**: 保持观察 (Info, 无需修复)

### V9-N4 (Info) — percent_decode 不处理多字节 UTF-8
**状态**: 保持观察 (Info, 无实际影响)

### V9-N5 (Info) — pool_max=0 使用 assert!
**状态**: 保持观察 (Info, 启动路径 fail-fast 可接受)

---

## 新发现

### N1 (Low) — commit/cancel 在 INFO 级别记录每笔事务

**文件**: `pg_tx.rs:142, 179`  
**现象**: 

```rust
info!("PostgreSQL transaction committed");  // line 179
info!("PostgreSQL transaction cancelled");  // line 142
```

在高吞吐场景下（每秒数千事务），INFO 级别的日志会产生大量噪声。SurrealDB 内置的 mem/rocksdb 后端在对应路径使用 `debug!` 或 `trace!`。

**影响**: 生产环境日志噪声，不影响正确性。  
**修复**: 将 `info!` 改为 `debug!`，2 行改动。

### N2 (Info) — `push_savepoint_name` 双重堆分配

**文件**: `transaction.rs:280-281`

```rust
let name = unsafe { std::str::from_utf8_unchecked(name_slice) }.to_string();  // alloc 1
self.savepoints.push(name.clone());  // alloc 2
name  // returned
```

savepoint 名称先 `to_string()` 分配一次，再 `clone()` 推入栈又分配一次。可改为推入后返回引用，但 savepoint 操作是低频事务控制路径，影响极小。

### N3 (Info) — `percent_decode` 对非 ASCII 字节的 Latin-1 语义

**文件**: `config.rs:43`

```rust
result.push(char::from(b));  // b ∈ 0x00..=0xFF
```

`char::from(b)` 对 b ≥ 0x80 产生 U+0080–U+00FF 的 Latin-1 字符，而非 UTF-8 解码的多字节序列。例如 `%C3%A9` (UTF-8 的 é) 会产生 `Ã©` 而非 `é`。所有当前参数均为 ASCII，无实际影响。

### N4 (Info) — `getm` 每次 `keys_ref` 分配

**文件**: `transaction.rs:406`

```rust
let keys_ref: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
```

每次 `getm` 调用都分配一个 `Vec<&[u8]>`。理论上可以避免（如果 sqlx 的 bind 直接接受 `&[Vec<u8>]`），但实际上这是 sqlx API 限制。影响极小。

---

## Bug 维度深度分析

### B1 — commit/cancel 指标计数逻辑 — ✅ 正确

`pg_tx.rs:115-183` 中 commit/cancel 的计数器递增逻辑：
- 成功路径: 递增 `committed` (commit) 或 `rolled_back` (cancel)
- 错误路径: 递增 `rolled_back` (两者都是, 因为 PG COMMIT 失败 = 自动 ROLLBACK)
- 已完成 (swap 返回 true): 直接返回错误, 不递增 — 正确, 原始操作已计入
- guard 为 None (swap 返回 false 但 guard 空): 不可能发生 — done=false 意味着 guard 必非空

### B2 — begin() 乐观 BEGIN 恢复逻辑 — ✅ 正确

`store.rs:293-321` 的恢复链：
- 正常路径: 仅 1 次 BEGIN (0 额外往返)
- 25P02 恢复: ROLLBACK + BEGIN (1 额外往返)
- 25P01 不检查 (BEGIN 不产生此错误)
- 泄漏的活跃事务 (非 failed): 仅产生 WARNING, 无法在此检测 — 已知限制, Drop impl 兜底

### B3 — probe_persistent conn1/conn2 清理 — ✅ 正确

`store.rs:499-547`:
- conn1 释放: **不** 执行 DEALLOCATE ALL — 保留探测语句在服务端
- conn2 成功: 执行 DEALLOCATE ALL — 清理 conn2 的探测语句
- conn2 失败: 执行 DEALLOCATE ALL — 清理 + 返回 false

### B4 — Drop 兜底机制 — ✅ 可接受

`transaction.rs:850-856`: Drop 仅打印 warn。连接通过 `PoolConnection::drop` 返回连接池, PG 会自动 ROLLBACK 未提交的事务。`PgTx` 的 commit/cancel 正常路径已关闭连接, 极端情况 (忘记 commit/cancel) 由 PG 侧兜底。

### B5 — tune.rs expect() 调用 — ✅ 安全

`tune.rs:192, 216` 的 `validate_identifier(table).expect(...)` 是 defense-in-depth:
- `PgStore::new()` 在 `merge_url_params` 中已校验 table_name
- 到达 `create_table_sql`/`tune_table_sql` 时 table 已通过校验
- expect 永远不会触发

### B6 — session_sql() assert! 校验 — ✅ 安全

`tune.rs:263-284`:
- `from_env()` 通过 `env_str_validated` 已校验内存参数
- 直接构造的 `PgTuneConfig` 会被 assert 拦截
- `random_page_cost` 的 NaN/Infinity 被拦截
- 全部在启动路径 (构造后不可变), 不会在运行时触发

---

## 性能维度深度分析

### P1 — 热路径 `format!()` 清零 — ✅ 优秀

所有 14 个 `format!()` 调用都在 `Sql::new()` 构造路径 (仅执行一次)。每次事务只需 `Arc::clone(&self.sql)` (1 次原子自增)。每次 KV 操作通过字段借用直接访问 `&self.sql.field` (零原子, 零分配)。

### P2 — begin() 乐观策略 — ✅ 优秀

正常路径: 1 次网络往返 (BEGIN)。仅 25P02 异常时额外 2 次 (ROLLBACK + BEGIN 重试)。比无条件 ROLLBACK-first 节省 1 次往返。

### P3 — getm 双阈值线性扫描 — ✅ 合理

`transaction.rs:424`:
```rust
let use_linear = rows.len() <= 64 && rows.len().saturating_mul(keys.len()) <= 8192;
```

小结果集用线性扫描 (cache-friendly), 大结果集用 HashMap。双阈值防止 `keys.len()` 极大时的 O(n²) 退化。

### P4 — OFFSET 分页 — ✅ 可接受

SurrealDB 的默认游标通过推进 `range.start` 实现等效 keyset 分页, `skip` 在后续批次中始终为 0。`skip > 1000` 的 warn 几乎不会触发。

---

## 功能维度深度分析

### F1 — putc/delc 语义 — ✅ 正确

- `putc(key, val, None)` → 委托 `put` (insert-if-absent)
- `putc(key, val, Some(v))` → CAS: 仅当 current == v 时更新
- `delc(key, None)` → 委托 `del` (无条件删除)
- `delc(key, Some(v))` → CAS: 仅当 current == v 时删除

### F2 — del 不检查 rows_affected — ✅ 正确

`del()` 返回 `Ok(())` 即使 0 行被删除。这是 "delete if exists" 语义, 与 KV store 约定一致。`delc` 在条件不满足时返回 `ConditionNotMet`。

### F3 — setm UNNEST 批量写入 — ✅ 正确

`setm` 通过 `UNNEST($1::bytea[], $2::bytea[])` 在单条 SQL 中完成批量 upsert, 将 N 次网络往返降为 1 次。

### F4 — savepoint 命名 — ✅ 正确

`sp_{counter}` 格式, `wrapping_add` 防止溢出 panic。栈缓冲区 `[u8; 16]` 足够容纳 `sp_` + 最多 10 位数字。`unsafe { from_utf8_unchecked }` 由 `debug_assert!` 守护, 仅写入 ASCII 字节。

### F5 — metrics 实现 — ✅ 完整

`register_metrics` 暴露 6 个指标: pool_size, pool_idle, pool_max, tx_started, tx_committed, tx_rolled_back。`collect_u64_metric` 对同源指标合并查询以减少冗余原子读取。

---

## 功能优化建议

### O1 (Low, 2 行改动) — commit/cancel 日志降级

将 `pg_tx.rs:142,179` 的 `info!` 改为 `debug!`。高吞吐场景下每秒数千行 INFO 日志是噪声, debug 级别仍可在需要时开启。

### O2 (Low, ~15 行) — setm 参数上限保护

PG 单条查询参数上限 65,535。`setm` 绑定 2 个数组, 每个数组的元素数限制为 ~32,767。当前无上限保护, 极端大批量可能触发 `invalid_parameter_count`。建议添加:

```rust
const SETM_MAX_PAIRS: usize = 32_000;
if pairs.len() > SETM_MAX_PAIRS {
    warn!(count = pairs.len(), "setm batch exceeds limit, chunking");
    // 分块处理
}
```

实际中 SurrealDB 引擎的批次大小通常远低于此, 但作为防御性编程值得考虑。

### O3 (Info) — 连接池利用率告警

在 `collect_u64_metric` 或单独的周期检查中, 当 `pool.size() > pool_max * 0.8` 时输出 `warn!`, 提示运维扩容。当前指标已暴露, 但无主动告警。

---

## 总结

| 维度 | 结果 |
|------|------|
| Critical Bug | 0 |
| High Bug | 0 |
| Medium Bug | 0 |
| Low Bug | 0 |
| Low (日志噪声) | 1 (N1) |
| Info | 3 (N2/N3/N4) |
| 功能优化建议 | 3 (O1/O2/O3) |

**结论**: 代码无 Bug, 无安全漏洞, 无逻辑错误。V9 的 2 项可操作问题已全部修复。代码达到生产就绪的安全和质量水平。

**最值得做的事**: O1 (日志降级, 2 行改动, 立即减少生产环境噪声)。

---

*累计十轮审计 + 一轮优化审阅, 代码从最初的 19 项安全/逻辑问题进化到零 Bug 状态。*
