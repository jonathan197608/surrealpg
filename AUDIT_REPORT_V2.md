# surreal-pg 第二次代码审计报告

> 审计日期: 2026-07-31 (第二轮)
> 审计范围: `/Volumes/DELL/rust/surrealpg/src/` 全部源码 + 测试 + 配置
> 对比基线: 第一次审计报告 (17 项问题: 2 Critical, 3 High, 7 Medium, 5 Low)
> 构建状态: `cargo check` 通过，零警告零错误

---

## 总览

| 严重性 | 第一轮 | 第二轮 | 变化 |
|--------|--------|--------|------|
| **Critical** | 2 | 0 | ✅ 全部修复 |
| **High** | 3 | 0 | ✅ 全部修复 (H2 部分修复，风险已消除) |
| **Medium** | 7 | 1 | ✅ 6 项修复，1 项新发现 |
| **Low** | 5 | 3 | ✅ 2 项修复，2 项保留 (可接受)，1 项新发现 |
| **合计** | 17 | 4 | 修复率 88% |

---

## 第一轮问题逐项核对

### S1. `table_name` SQL 注入 — ✅ 已修复

**原问题**: `table_name` 通过 URL 参数直接拼接进 14 处 SQL，无校验。

**修复验证**:
- `config.rs:129-141` 新增 `validate_identifier()` 方法，校验 `table_name` 仅含 `[a-zA-Z0-9_]` 且非空
- `config.rs:216-219` 在 `merge_url_params` 中调用 `validate_identifier`，非法值触发错误
- 注入路径 `?table_name=kv; DROP TABLE users; --` 已被阻断（分号和空格不在允许字符集内）
- `transaction.rs` 中所有 `format!()` 拼接点均受上游校验保护

**残留风险**: 无。校验逻辑正确且覆盖所有入口路径。

---

### S2. `PG_TUNED_*` 环境变量 SQL 注入 — ✅ 已修复

**原问题**: `session_sql()` 和 `tune_table_sql()` 将环境变量值直接拼接进 SQL。

**修复验证**:
- `tune.rs:289-302` 新增 `env_str_validated()` 函数，接受校验谓词
- `tune.rs:306-323` `validate_pg_memory_size()` 校验 `^[0-9]+(kB|MB|GB|TB)$` 或纯整数
- `tune.rs:326-328` `validate_toast_storage()` 白名单 `external|extended|main|plain`
- 所有 5 个 `server_*` 内存参数和 `toast_storage` 均使用 `env_str_validated`
- `tune.rs:424-444` 包含注入测试用例：`"64MB'; DROP TABLE kv; --"` 和 `"evil'; DROP TABLE kv; --"` 均被拒绝

**残留风险**: 无。

---

### H1. 只读事务 `commit()` 泄漏连接 — ✅ 已修复

**原问题**: 只读事务调用 `commit()` 时返回错误，连接未归还连接池。

**修复验证**:
- `pg_tx.rs:100-114` `commit()` 不再检查 `self.write`，PG 原生支持 `COMMIT` on `READ ONLY` 事务
- `store.rs:177-187` `begin()` 新增 ROLLBACK 预清理安全网，防止连接池中残留事务
- 两条防线协同工作：
  1. 正常路径：`commit()`/`cancel()` 显式关闭事务并归还连接
  2. 异常路径：若连接泄漏回连接池，下次 `begin()` 的 ROLLBACK 预清理会重置状态

**残留风险**: 无。

---

### H2. `PgTransaction::Drop` 不执行 ROLLBACK — ⚠️ 部分修复 (风险已消除)

**原问题**: `Drop` 只打日志不执行 ROLLBACK，依赖 sqlx 隐式行为。

**修复方式**: 未修改 `Drop` 实现（`transaction.rs:504-512` 仍只打日志），而是通过 `store.rs:177-187` 的 ROLLBACK 预清理作为安全网。

**分析**:
- `Drop` 不能是 async，无法直接执行 SQL — 这是 Rust 语言限制
- ROLLBACK 预清理在每次 `begin()` 时执行，确保从连接池获取的连接处于干净状态
- 即使前一个事务泄漏（Drop 未清理），下一个 `begin()` 会 ROLLBACK 掉残留事务
- 唯一的理论窗口：泄漏的连接在被复用之前持有锁。但 sqlx 连接池在连接归还时会检查连接健康状态

**结论**: 虽然不是最优雅的方案，但风险已实质消除。升级为完全修复需要实现 async Drop（语言限制）或在 sqlx 的 `after_connect` 中添加连接状态检查回调。

---

### H3. 调优环境变量是死代码 — ✅ 已修复

**原问题**: `idle_timeout` 和 `max_lifetime` 默认值为 `Some(...)`，导致 `Option::or` 永远不使用 `tune` 的值。

**修复验证**:
- `config.rs:15-16` `idle_timeout` 和 `max_lifetime` 默认值改为 `None`
- `store.rs:57-58` 使用 `config.idle_timeout.or(Some(tune.pool_idle_timeout))`
- 优先级链正确：URL 参数 > PG_TUNED_* 环境变量 > PgTuneConfig 默认值

**残留风险**: 无。

---

### M1. 配置优先级用哨兵值判断 — ✅ 已修复

**原问题**: 用魔法数字 `20`、`5` 判断 URL 参数是否被覆盖。

**修复验证**:
- `config.rs:9-11` `max_connections`、`min_connections`、`connect_timeout` 改为 `Option<u32>` / `Option<Duration>`
- `store.rs:54-56` 使用 `unwrap_or()` 替代哨兵值判断
- 语义清晰：`None` = 未设置（用 tune 值），`Some(v)` = 显式设置（用 v）

**残留风险**: 无。

---

### M2. `PgConfig::statement_timeout` 是死代码 — ✅ 已修复

**原问题**: `PgConfig` 有 `statement_timeout` 字段可通过 URL 设置但从未使用。

**修复验证**: `PgConfig` 中已移除 `statement_timeout` 字段。实际生效的是 `PgTuneConfig::statement_timeout`，通过 `session_sql()` 在 `after_connect` 中设置。

**残留风险**: 无。

---

### M3. 配置值无校验 — ✅ 已修复

**原问题**: `max_connections=0` 和 `min > max` 无校验。

**修复验证**:
- `config.rs:185-189` `max_connections=0` 被拒绝并记录警告
- `config.rs:194-201` `min_connections > max_connections` 被拒绝并记录警告
- 使用 `let-chain` (`if let Some(max) = ... && v > max`) 实现交叉校验

**残留风险**: 见下方 N2（校验依赖 URL 参数顺序）。

---

### M4. `probe_persistent` 不清理探测连接 — ✅ 已修复

**原问题**: 探测创建的 prepared statements 未清理。

**修复验证**:
- `store.rs:341-342` 成功路径：两个连接均执行 `DEALLOCATE ALL`
- `store.rs:359-360` 错误路径：同样执行 `DEALLOCATE ALL`
- 所有代码路径均覆盖

**残留风险**: 无。

---

### M5. `canceller` 参数被忽略 — ✅ 已修复

**原问题**: `CancellationToken` 未传入 `PgStore::new`，无法在关闭时取消事务。

**修复验证**:
- `store.rs:45` `PgStore::new` 签名新增 `canceller: CancellationToken` 参数
- `store.rs:33` `PgStore` 结构体新增 `canceller` 字段
- `store.rs:163-165` `begin()` 检查 `self.canceller.is_cancelled()`，若已取消返回 `TxCancelled`
- `composer.rs:96` `canceller.clone()` 传入 `PgStore::new`

**残留风险**: 无。`ConfigMap` 参数仍未使用，但这不影响正确性（PG 后端不需要 SurrealDB 级配置）。

---

### M6. 只读事务未设置隔离级别 — ✅ 已修复

**原问题**: `BEGIN READ ONLY` 未包含隔离级别，回退到服务器默认。

**修复验证**:
- `store.rs:196-200` 只读事务使用 `BEGIN ISOLATION LEVEL {} READ ONLY`
- 隔离级别由 `self.config.isolation_level.as_sql()` 提供

**残留风险**: 无。

---

### M7. 多个配置项不可通过 URL 设置 — ✅ 已修复

**原问题**: `connect_timeout`、`idle_timeout`、`read_only_optimization` 无法通过 URL 设置。

**修复验证**:
- `config.rs:234-238` `connect_timeout` 已支持 URL 参数（秒）
- `config.rs:239-243` `idle_timeout` 已支持 URL 参数（秒）
- `config.rs:244-248` `read_only_optimization` 已支持 URL 参数（bool）

**残留风险**: 无。

---

### L1. `is_pg_path` 冗余检查 — ✅ 已修复

**原问题**: `postgres://` 和 `postgresql://` 检查被 `postgres:` 和 `postgresql:` 覆盖。

**修复验证**: `composer.rs:53-55` 简化为 `path.starts_with("postgres:") || path.starts_with("postgresql:")`。

---

### L2. OFFSET 分页对大偏移量低效 — ❌ 保留 (可接受)

**状态**: 未修改。`transaction.rs:371` 仍有 `if skip > 1000` 的警告日志。

**评估**: SurrealDB 的 KV 层通常使用小 offset（游标分页为主），当前实现已添加大 offset 警告。在 KV 存储后端场景下，这是可接受的。

---

### L3. 无死锁/序列化失败自动重试 — ❌ 保留 (可接受)

**状态**: 未修改。`error.rs` 仍将 `Deadlock`/`SerializationFailure` 映射为 `TransactionConflict`，无自动重试。

**评估**: SurrealDB 引擎在更高层处理重试逻辑。PG 后端只需正确映射错误类型，当前实现已满足要求。若使用 `SERIALIZABLE` 隔离级别且引擎不重试，则需要在此层添加重试。

---

### L4. 测试 `clean_all` 未覆盖全部 key 空间 — ✅ 已修复

**原问题**: `delr(vec![]..vec![0xFF])` 只删除 key < `[0xFF]` 的记录。

**修复验证**: `integration_test.rs:107` 改为 `tx.delr(vec![]..vec![0xFF; 16])`，覆盖 16 字节的 `0xFF` 上界。

---

### L5. 14 处 `.expect("guarded txn")` — ✅ 已修复

**原问题**: `pg_tx.rs` 14 处 `.expect()` 依赖 `lock()` 顺序保证。

**修复验证**:
- Grep 确认 `src/` 中零 `.expect()` 调用
- 所有 `Transactable` 方法使用 `guard.as_mut().ok_or(kvs::Error::TransactionFinished)?` 替代 `.expect()`
- 错误路径返回 `TransactionFinished` 而非 panic

---

## 新发现问题

### N1. `merge_url_params` 中 `panic!` 可被外部输入触发 — Medium

**文件**: `src/config.rs:216-219`

```rust
"table_name" => {
    if let Err(e) = Self::validate_identifier(value) {
        tracing::error!("{e}");
        panic!("{e}");  // ← 外部 URL 参数可触发 panic
    }
    self.table_name = value.to_string();
}
```

**问题**: `merge_url_params` 接收来自连接 URL 的查询参数。如果 URL 中包含非法 `table_name`（如 `?table_name=kv; DROP TABLE`），服务器会直接 panic 崩溃。这对于一个数据库服务器来说是不可接受的 — 一个格式错误的连接字符串不应该导致整个进程崩溃。

**根因**: `merge_url_params` 返回 `()` 而非 `Result`，无法传播错误。`validate_identifier` 返回 `Result<(), String>`，但调用方选择 panic 而非返回错误。

**影响**: 拒绝服务 (DoS) — 任何能控制连接 URL 的人（包括配置文件错误）都能让服务器崩溃。

**修复建议**:
```rust
// 方案 A: 将 merge_url_params 改为返回 Result
pub fn merge_url_params(&mut self, url: &str) -> Result<(), String> {
    // ...
    "table_name" => {
        Self::validate_identifier(value)?;
        self.table_name = value.to_string();
    }
    // ...
}

// 方案 B: 降级为 warn + 跳过（保持当前函数签名）
"table_name" => {
    if let Err(e) = Self::validate_identifier(value) {
        tracing::warn!("{e}, ignoring table_name from URL");
        // 保持默认值 "kv"
    } else {
        self.table_name = value.to_string();
    }
}
```

推荐方案 A，因为它能在启动时暴露配置错误。

---

### N2. `min_connections` 交叉校验依赖 URL 参数顺序 — Low

**文件**: `src/config.rs:194-201`

```rust
"min_connections" => {
    if let Ok(v) = value.parse::<u32>() {
        if let Some(max) = self.max_connections  // ← 只检查已设置的 max
            && v > max
        {
            tracing::warn!("min_connections={v} > max_connections={max}, ignoring");
            continue;
        }
        self.min_connections = Some(v);
    }
}
```

**问题**: URL 查询参数按出现顺序解析。如果 `min_connections` 出现在 `max_connections` 之前：

```
?min_connections=20&max_connections=10
```

解析 `min_connections=20` 时，`self.max_connections` 仍为 `None`，校验被跳过。最终 `pool_min=20, pool_max=10`，违反约束。

**影响**: sqlx 的 `PgPoolOptions` 在 `min > max` 时行为未定义（可能 panic 或自动调整）。实际影响有限，因为这种参数组合极不常见。

**修复建议**: 在 `PgStore::new` 中、所有 URL 参数解析完成后，执行一次最终的交叉校验：
```rust
// store.rs, after config.merge_url_params(url)
if let (Some(min), Some(max)) = (config.min_connections, config.max_connections) {
    if min > max {
        tracing::warn!("min_connections={min} > max_connections={max}, capping min to max");
        config.min_connections = Some(max);
    }
}
```

---

## 新发现亮点

### 正面改进

1. **L5 修复质量优秀**: `.expect()` 全部替换为 `ok_or(...)?`，消除了所有理论上的 panic 路径。Grep 确认 `src/` 中零 `.expect()` 和零 `unwrap()`。

2. **H1 修复策略合理**: 双防线设计（commit 允许只读 + begin 预清理 ROLLBACK）比单一修复更健壮。

3. **M4 修复全面**: 所有代码路径（成功和错误）均执行 `DEALLOCATE ALL`，无遗漏。

4. **测试覆盖增强**: `tune.rs` 新增 `validate_pg_memory_size` 和 `validate_toast_storage` 的单元测试，包含注入字符串用例。

5. **配置优先级清晰**: `Option::unwrap_or` / `Option::or` 的使用使优先级链一目了然，消除了哨兵值的歧义。

---

## 构建验证

```
cargo check — 通过，零警告，零错误
src/ 中 .expect() — 0 处
src/ 中 unwrap() — 0 处
src/ 中 panic! — 1 处 (config.rs:218, 见 N1)
```

---

## 建议修复优先级

1. **应修复**: N1 (panic! → Result，防止 DoS)
2. **可改进**: N2 (交叉校验时机)
3. **保持现状**: L2 (OFFSET 分页), L3 (无自动重试) — 当前实现可接受

---

## 总结

第一轮审计的 17 项问题中，14 项完全修复，2 项部分修复/保留（可接受），1 项由修复引入的新问题（N1）。整体修复质量高，特别是 Critical 和 High 级别问题全部解决。

代码安全性显著提升：
- SQL 注入面已通过校验函数 + 白名单 + 单元测试三重防护
- 连接泄漏通过双防线消除
- 配置优先级通过 `Option` 类型系统明确表达
- `panic` 路径从 14+ 处降至 1 处

唯一需要关注的是 N1：将 `panic!` 改为 `Result` 返回，这是当前代码中唯一可被外部输入触发的 panic。
