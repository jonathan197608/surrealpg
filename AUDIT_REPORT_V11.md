---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: '60456234-95e5-411d-a2ad-543deba28166'
  PropagateID: '60456234-95e5-411d-a2ad-543deba28166'
  ReservedCode1: '3ae9da65-cfeb-457a-8bdc-94328cb6853f'
  ReservedCode2: '3ae9da65-cfeb-457a-8bdc-94328cb6853f'
---

# AUDIT REPORT V11 — Bug / Performance / Functionality

> **Scope**: Full codebase deep audit across three dimensions.  
> **Date**: 2026-08-04  
> **Baseline**: commit `8ef5fb1` (V10 P3 iteration fixes applied)  
> **Diff from V10**: B4 Box<[u8]>、B6 重复参数 warn、F4 25P01 上下文、F8 事务指标 — 全部已修复

---

## 1. Bug 维度

### B1 [Medium] `transaction.rs` 中 `setm()`、`delr()`、`putc()`、`delc()` 存在冗余二次 `self.closed` 检查

**文件**: `src/transaction.rs:462-468, 510-516, 553-560, 577-584`

V10 B3 修复在每个写操作顶部添加了 `if self.closed { return Err(TxClosed); }` 检查，但 `setm()` 和 `delr()` 在中间逻辑后又重复了一次 `self.closed` 检查（位于 `check_writable()` 之后）。`putc()` 和 `delc()` 在 `chk` 分支后也有二次检查。虽然不会导致错误行为，但属于死代码，增加维护负担和阅读困惑。

```rust
// setm() line 462-468
if self.closed { return Err(PgStoreError::TxClosed); }  // 第1次
self.check_writable()?;
if pairs.is_empty() { return Ok(()); }
if self.closed { return Err(PgStoreError::TxClosed); }  // 第2次 — 冗余

// delr() line 577-584
if self.closed { return Err(PgStoreError::TxClosed); }  // 第1次
self.check_writable()?;
if rng.start >= rng.end { return Ok(()); }
if self.closed { return Err(PgStoreError::TxClosed); }  // 第2次 — 冗余
```

**修复**: 删除中间逻辑后的冗余 `self.closed` 检查。顶部检查已足够。

### B2 [Low] `tune.rs` 中 `checkpoint_target` 无上界校验

**文件**: `src/tune.rs:165`

PG 要求 `checkpoint_completion_target` 在 `[0.0, 1.0]` 范围内，但 `env_f64` 不做范围校验。如果用户设置 `PG_TUNED_SERVER_CHECKPOINT_TARGET=1.5`，会导致 PG 启动报错（虽然这只是 hint 参数，不影响运行）。

**修复**: 在 `from_env()` 后添加 `.clamp(0.0, 1.0)` 范围校验。

---

## 2. Performance 维度

### P1 [Medium] `config.rs` `is_sql_reserved()` 使用 `to_ascii_uppercase()` 堆分配

**文件**: `src/config.rs:252-253`

```rust
let upper = name.to_ascii_uppercase();  // 堆分配 String
RESERVED.binary_search(&upper.as_str()).is_ok()
```

每次调用 `is_sql_reserved()` 都会分配一个 `String`，但 `binary_search` 只需要比较即可。可以用自定义的 `eq_ignore_ascii_case` 比较替代，实现零分配二分查找。

**修复**: 使用 `binary_search_by()` 搭配大小写无关比较，消除 `to_ascii_uppercase()` 分配。

### P2 [Low] `transaction.rs` `getm()` 中 `keys_ref` 在 closed 检查前构建

**文件**: `src/transaction.rs:395-397`

```rust
let keys_ref: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect(); // 先构建
if self.closed { return Err(PgStoreError::TxClosed); }  // 再检查
```

如果事务已关闭，`keys_ref` 的分配就被浪费了。虽然实际影响很小（closed 事务是罕见路径），但正确的模式是先检查再分配。

**修复**: 将 `self.closed` 检查移到 `keys_ref` 构建之前。

---

## 3. Functionality / Quality 维度

### F1 [Low] `PgConfig` 字段全部 `pub`，可绕过验证

**文件**: `src/config.rs:61-82`

`PgConfig` 所有字段都是 `pub`，调用者可直接构造无效配置（如 `max_connections: Some(0)`、`table_name: "DROP TABLE"` 等）。虽然目前 `PgStore::new()` 有运行时断言和验证，但类型系统未强制执行。

**修复**: 将 `PgConfig` 字段改为 `pub(crate)` 并提供 getter 方法。这是低优先级，因为改 API 影响面较大，且运行时保护已存在。标记为 **不修复（需架构改动）**。

### F2 [Low] `composer.rs` 中 `new_transaction_builder` 忽略 `config: ConfigMap`

**文件**: `src/composer.rs:92`

```rust
async fn new_transaction_builder(
    &self, path: &str, canceller: CancellationToken, config: ConfigMap,
) -> anyhow::Result<...> {
    if Self::is_pg_path(path) {
        let store = PgStore::new(path, canceller.clone()).await?;  // config 未使用
```

`ConfigMap` 参数被忽略。如果未来 SurrealDB 在 ConfigMap 中传递配置项（如加密密钥），PG 后端会遗漏。

**修复**: 当前 PG 后端不需要 ConfigMap，添加 `let _ = &config;` 显式忽略并加注释。低优先级。

### F3 [Low] 缺少 `PgStore` 的单元测试

**文件**: `src/store.rs`

`PgStore` 的测试完全依赖集成测试（需要 PG 实例）。没有单元测试验证配置解析、参数覆盖等逻辑。

**修复**: 补充纯逻辑单元测试（不需要 PG 连接的测试），如 `test_pool_max_assertion`、`test_read_only_optimization_with_pgbouncer`。

---

## 修复优先级

| ID | 维度 | 优先级 | 修复方案 |
|----|------|--------|----------|
| B1 | Bug | P1 | 删除冗余 `self.closed` 检查 |
| P1 | Perf | P1 | `is_sql_reserved` 零分配二分查找 |
| P2 | Perf | P2 | `getm` closed 检查前移 |
| B2 | Bug | P2 | `checkpoint_target` 范围校验 |
| F3 | Qual | P2 | 补充 `PgStore` 单元测试 |
| F2 | Qual | P3 | 显式忽略 `ConfigMap` |
| F1 | Qual | 不修复 | 需架构改动，运行时保护已存在 |

**不修复清单**:
- F1: `PgConfig` 字段 `pub` → 需 API 改动，运行时保护已足够