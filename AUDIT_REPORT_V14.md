---
AIGC:
  ContentProducer: '001191110102MAD55U9H0F10002'
  ContentPropagator: '001191110102MAD55U9H0F10002'
  Label: '1'
  ProduceID: '761e6527-0328-4810-9d37-d1fb49187d1e'
  PropagateID: '761e6527-0328-4810-9d37-d1fb49187d1e'
  ReservedCode1: '19a39092-b67f-4165-850c-e69ee5a8f4db'
  ReservedCode2: '19a39092-b67f-4165-850c-e69ee5a8f4db'
---

# AUDIT_REPORT_V14 — 深度代码审计

**审计日期**: 2026-08-04
**审计范围**: 10 个源文件 + 2 个测试文件 + Cargo.toml
**审计维度**: Bug / 性能 / 功能

## 审计结果汇总

| 维度 | 发现数 | 已修复 | 保留现状 |
|------|--------|--------|----------|
| Bug | 2 | 1 | 1 (有兜底) |
| 性能 | 3 | 2 | 1 (非热路径) |
| 功能 | 1 | 0 | 1 (有兜底) |
| 死代码 | 1 | 1 | 0 |
| **合计** | **7** | **4** | **3** |

---

## Bug 类问题

### B1 — `count` 方法检查顺序不一致（已修复）

| 项 | 详情 |
|---|------|
| **严重性** | 中 |
| **文件** | `src/transaction.rs:706-711` |
| **状态** | ✅ 已修复 |

**问题**: `count` 方法先检查空范围再检查 `self.closed`，与所有其他方法（`exists`/`get`/`set`/`del`/`delr`/`keys`/`scan` 等）不一致。对已关闭事务传入空范围時返回 `Ok(0)` 而非 `Err(TxClosed)`。

**修复**: 将 `self.closed` 检查提前到空范围检查之前，与其他方法保持一致。

```diff
 pub async fn count(&mut self, rng: Range<Key>) -> Result<u64> {
+    // B1: Check closed first — consistency with all other methods.
+    if self.closed { return Err(PgStoreError::TxClosed); }
     // Empty range — skip DB round-trip.
     if rng.start >= rng.end {
         return Ok(0);
     }
-    if self.closed { return Err(PgStoreError::TxClosed); }
```

### B2 — `min_connections` 单字段检查无法捕获跨参数顺序问题（低风险，已有兜底）

| 项 | 详情 |
|---|------|
| **严重性** | 低 |
| **文件** | `src/config.rs:342-356` |
| **状态** | ⚠️ 已被 `store.rs:84-91` 后置校验兜底，无需修复 |

**问题**: 当 URL 参数为 `?min_connections=20&max_connections=10` 顺序時，`min_connections` 先设为 `Some(20)`（此时 `max_connections` 为 `None`），跳过校验。`store.rs` 的后置校验已正确兜底。

---

## 性能类问题

### P1 — `collect_u64_metric` 多次调用 `pool_size()` / `tx_metrics()`（已修复）

| 项 | 详情 |
|---|------|
| **严重性** | 低 |
| **文件** | `src/pg_builder.rs:95-106` |
| **状态** | ✅ 已修复 |

**问题**: `collect_u64_metric` 中 `pool_size()` 被调用两次（第 97-98 行），`tx_metrics()` 也被调用三次（第 101-103 行）。虽然不是热路径，但每次调用都产生冗余的原子操作。

**修复**: 合并 `pool_size` 和 `pool_idle` 到一次调用，合并三个 tx_metrics 计数器到一次调用。

### P2 — `count_approx` SQL 中 `reltuples > 0` 使空表返回 `None`（已修复）

| 项 | 详情 |
|---|------|
| **严重性** | 低 |
| **文件** | `src/transaction.rs:111-113, 736-746` |
| **状态** | ✅ 已修复 |

**问题**: PostgreSQL `pg_class.reltuples` 的值：
- 未分析的表：`-1`（应返回 `None`）
- 空表：`0`（应返回 `Some(0)`）
- 非空表：`> 0`

当前 `WHERE reltuples > 0` 过滤掉了空表（返回 `None` 而非 `Some(0)`），语义不精确。

**修复**: 改为 `WHERE reltuples >= 0`，只跳过未分析的表（`-1`），空表返回 `Some(0)`。同时更新文档注释。

### P3 — savepoint 栈缓冲优化仍产生 `.to_string()` 堆分配

| 项 | 详情 |
|---|------|
| **严重性** | 低（非热路径） |
| **文件** | `src/transaction.rs:240-281, 289-309` |
| **状态** | 保留现状 |

**问题**: 代码用 `[u8; 16]`/`[u8; 48]` 栈缓冲避免 `format!()`，但最终仍 `.to_string()` 堆分配。savepoint 操作非热路径，优化收益有限。

---

## 功能类问题

### F1 — `probe_persistent` 无法识别 PgBouncer `max_prepared_statements` 配置

| 项 | 详情 |
|---|------|
| **严重性** | 中 |
| **文件** | `src/store.rs:491-567` |
| **状态** | 保留现状（有环境变量兜底） |

**问题**: PgBouncer 1.21+ 的 `max_prepared_statements > 0` 配置允许 transaction mode 使用 named prepared statements，探测不会产生 `42P05`，返回 `true`。但 pgbouncer 仍不支持 `BEGIN READ ONLY`，可能导致 `read_only_optimization` 错误启用。

**兜底**: 用户可通过 `PG_PERSISTENT_STATEMENTS=disabled` 或 `read_only_optimization=false` 规避。

---

## 死代码

### L1 — `record_commit()` / `record_rollback()` 从未被调用（已修复）

| 项 | 详情 |
|---|------|
| **严重性** | 低 |
| **文件** | `src/store.rs:413-421` |
| **状态** | ✅ 已修复 |

**问题**: F8 的指标计数器在 `PgTx::commit()` / `PgTx::cancel()` 中通过 `Arc<AtomicU64>` 直接 `fetch_add` 更新。`PgStore` 上的 `record_commit()` 和 `record_rollback()` 是死代码，从未被任何代码路径调用。

**修复**: 删除这两个方法。

---

## 已确认干净的文件

| 文件 | 状态 |
|------|------|
| `src/composer.rs` | ✅ 干净 |
| `src/error.rs` | ✅ 干净 |
| `src/pg_builder.rs` | ✅ 干净（P1 修复后） |
| `src/pg_tx.rs` | ✅ 干净 |
| `src/config.rs` | ✅ 干净 |
| `src/lib.rs` | ✅ 干净 |
| `src/main.rs` | ✅ 干净 |
| `tests/integration_test.rs` | ✅ 干净 |
| `tests/surreal_kv_suite.rs` | ✅ 干净 |
| `Cargo.toml` | ✅ 干净 |

---

## 验证

- ✅ `cargo clippy --all-targets -- -D warnings` — 零告警
- ✅ `cargo test` — 全部通过