//! PgTransaction — implements Transactable over a PostgreSQL transaction.
//!
//! This is the core of the backend: every SurrealDB KV operation maps to one
//! or more SQL statements executed within a single PG transaction.

use std::ops::DerefMut;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use sqlx::Transaction;
use sqlx::{Executor, Row};
use tracing::{debug, trace, warn};

use crate::config::PgIsolation;
use crate::error::{PgStoreError, Result};

/// Type aliases matching SurrealDB conventions
pub type Key = Vec<u8>;
pub type Val = Vec<u8>;

/// Maximum number of key-value pairs per `setm` batch.
///
/// PostgreSQL limits each query to 65,535 parameters. `setm` binds 2 array
/// parameters (keys + vals), so each array can hold at most ~32,767 elements.
/// We use 32,000 as a conservative limit to leave headroom.
//
// O2: setm parameter-limit protection.
const SETM_MAX_PAIRS: usize = 32_000;

// ─── Pre-built SQL ───────────────────────────────────────

/// Pre-built SQL strings for all KV operations.
///
/// Constructed once when a `PgStore` is created, then shared with every
/// `PgTransaction` via `Arc<Sql>` (1 atomic refcount increment per tx).
///
/// Each method on `PgTransaction` references SQL strings directly via
/// `&self.sql.field` without cloning the `Arc`, eliminating per-operation
/// atomic overhead on the hot path. This works because Rust allows
/// simultaneous borrows of **different fields** of `self` — `&self.sql`
/// (immutable) and `&mut self.conn` (mutable) never conflict.
///
/// # Range-scan pagination
///
/// SurrealDB's `Transactable` trait provides `open_keys_cursor` /
/// `open_vals_cursor` for batched iteration. We do **not** override these —
/// the default `DefaultKeysCursor` / `DefaultValsCursor` implementation
/// drives pagination by calling `keys()`/`scan()` with `skip=0` on every
/// batch after the first, advancing `range.start` to `last_key + \x00`
/// (exclusive lower bound) between batches. This is functionally
/// equivalent to `WHERE key > $cursor` keyset pagination, so a separate
/// cursor-based SQL variant is unnecessary.
pub(crate) struct Sql {
    exists: String,
    get: String,
    getm: String,
    set: String,
    setm: String,
    put: String,
    putc: String,
    del: String,
    delc: String,
    delr: String,
    count: String,
    // ── Pre-built range-scan SQL (4 fixed combinations) ──
    // keys/keysr/scan/scanr — all use OFFSET-based pagination.
    // Cursor-based pagination is handled by the Transactable trait's
    // default cursor (see struct-level doc comment).
    range_keys_asc: String,
    range_keys_desc: String,
    range_kv_asc: String,
    range_kv_desc: String,
    // Original table name (for `count_approx` / `pg_class` queries).
    table_name: String,
    // Pre-built parameterised count_approx query (uses $1 for relname).
    count_approx: String,
}

impl Sql {
    pub(crate) fn new(table: &str) -> Self {
        // L1: validate table name at construction time. In production this
        // is always called from PgStore::new() which already validates, but
        // this catches misuse in tests or future code paths.
        debug_assert!(
            crate::config::PgConfig::validate_identifier(table).is_ok(),
            "Sql::new: table name '{table}' is not a valid SQL identifier"
        );
        Self {
            exists: format!("SELECT 1 AS exists_flag FROM {table} WHERE key = $1"),
            get: format!("SELECT val FROM {table} WHERE key = $1"),
            getm: format!("SELECT key, val FROM {table} WHERE key = ANY($1)"),
            set: format!(
                "INSERT INTO {table} (key, val) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val"
            ),
            setm: format!(
                "INSERT INTO {table} (key, val) \
                 SELECT * FROM UNNEST($1::bytea[], $2::bytea[]) \
                 ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val"
            ),
            put: format!(
                "INSERT INTO {table} (key, val) VALUES ($1, $2) \
                 ON CONFLICT (key) DO NOTHING"
            ),
            putc: format!("UPDATE {table} SET val = $2 WHERE key = $1 AND val = $3"),
            del: format!("DELETE FROM {table} WHERE key = $1"),
            delc: format!("DELETE FROM {table} WHERE key = $1 AND val = $2"),
            delr: format!("DELETE FROM {table} WHERE key >= $1 AND key < $2"),
            count: format!("SELECT count(*) AS cnt FROM {table} WHERE key >= $1 AND key < $2"),
            // Offset mode — SELECT … ORDER BY key {dir} LIMIT $3 OFFSET $4
            range_keys_asc: format!(
                "SELECT key FROM {table} WHERE key >= $1 AND key < $2 \
                 ORDER BY key ASC LIMIT $3 OFFSET $4"
            ),
            range_keys_desc: format!(
                "SELECT key FROM {table} WHERE key >= $1 AND key < $2 \
                 ORDER BY key DESC LIMIT $3 OFFSET $4"
            ),
            range_kv_asc: format!(
                "SELECT key, val FROM {table} WHERE key >= $1 AND key < $2 \
                 ORDER BY key ASC LIMIT $3 OFFSET $4"
            ),
            range_kv_desc: format!(
                "SELECT key, val FROM {table} WHERE key >= $1 AND key < $2 \
                 ORDER BY key DESC LIMIT $3 OFFSET $4"
            ),
            table_name: table.to_string(),
            count_approx: String::from(
                "SELECT reltuples::bigint AS approx_cnt FROM pg_class \
                 WHERE relname = $1 AND reltuples >= 0",
            ),
        }
    }
}

// ─── PgTransaction ──────────────────────────────────────

/// A transaction backed by a single PostgreSQL connection.
///
/// Implements all KV operations that SurrealDB's `Transactable` trait requires.
/// After `commit()` or `cancel()`, the internal `Transaction` is consumed
/// (sqlx's API takes `mut self`), setting `conn` to `None` — all subsequent
/// operations return `TxClosed`.
///
/// All SQL strings are pre-built at construction time to eliminate per-operation
/// `format!()` heap allocations on the hot path.
pub struct PgTransaction {
    /// The sqlx-managed PG transaction (auto-rollbacks on Drop).
    ///
    /// Using sqlx's `Transaction` API instead of raw `execute("BEGIN")` +
    /// `PoolConnection` ensures that `transaction_depth` is correctly
    /// maintained and `Transaction::drop` automatically queues a ROLLBACK
    /// for any uncommitted transaction, preventing the "there is already
    /// a transaction in progress" WARNING when connections return to the pool.
    ///
    /// `None` after commit/cancel (Transaction is consumed by sqlx's API).
    /// This replaces the former `closed: bool` field — the transaction's
    /// open/closed state is fully derivable from `conn.is_some()`.
    conn: Option<Transaction<'static, sqlx::Postgres>>,
    /// Whether this is a write transaction
    writeable: bool,
    /// Savepoint naming counter
    savepoint_counter: u32,
    /// Active savepoint name stack
    savepoints: Vec<String>,
    /// Isolation level (retained for future use / debugging)
    #[allow(dead_code)]
    isolation: PgIsolation,
    /// Whether to use persistent prepared statements.
    /// Must be false for pgbouncer/Supabase Pooler (transaction mode).
    persistent: bool,
    /// Pre-built SQL strings. Shared from `PgStore` via `Arc<Sql>` —
    /// one atomic refcount increment per transaction, zero per operation.
    ///
    /// On the hot path, methods borrow `&self.sql.field` directly alongside
    /// `&mut self.conn` — Rust allows this because they are different fields.
    sql: Arc<Sql>,
    /// Shared active-transaction counter (from PgStore). Decremented
    /// when the connection is released (commit/cancel/drop). Used for
    /// diagnostics: pool acquire failures log `tx_active` to show how
    /// many connections are held by in-flight transactions.
    tx_active: Arc<AtomicU64>,
}

impl PgTransaction {
    /// Create a new transaction wrapping a sqlx `Transaction`.
    ///
    /// The caller must have already started the transaction via
    /// `pool.begin_with()`. SQL strings are provided as a pre-built
    /// `Arc<Sql>`, shared from `PgStore` to avoid per-transaction
    /// `format!()` allocations.
    pub(crate) fn new_with_sql(
        conn: Transaction<'static, sqlx::Postgres>,
        writeable: bool,
        isolation: PgIsolation,
        persistent: bool,
        sql: Arc<Sql>,
        tx_active: Arc<AtomicU64>,
    ) -> Self {
        Self {
            conn: Some(conn),
            writeable,
            savepoint_counter: 0,
            savepoints: Vec::new(),
            isolation,
            persistent,
            sql,
            tx_active,
        }
    }

    // ─── Internal helpers ────────────────────────────────

    /// Decrement the active-transaction counter. Called when the connection
    /// is released back to the pool (commit/cancel/drop).
    fn release_active(&self) {
        self.tx_active.fetch_sub(1, AtomicOrdering::Relaxed);
    }

    /// Get a mutable reference to the inner `PgConnection`.
    ///
    /// **Note**: This method borrows `&mut self` and is only used by
    /// non-KV methods (savepoint operations). All KV operation methods
    /// use inline destructured borrows instead to avoid conflicts with
    /// `&self.sql` references.
    fn conn_mut(&mut self) -> Result<&mut sqlx::postgres::PgConnection> {
        let tx = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        Ok(tx.deref_mut())
    }

    fn check_writable(&self) -> Result<()> {
        if !self.writeable {
            return Err(PgStoreError::TxReadOnly);
        }
        Ok(())
    }

    /// Execute a parameterless SQL statement via simple query protocol.
    ///
    /// # Safety
    ///
    /// This method uses `raw_sql()` with no parameterization — the caller
    /// must ensure that `sql` does not contain user-controlled data. All
    /// current call sites construct SQL from savepoint names generated by
    /// `push_savepoint_name()` (which produces `sp_NNN` — only ASCII digits
    /// and the `sp_` prefix, no injection vector). Do NOT pass user input
    /// through this method.
    async fn execute_simple(&mut self, sql: &str, key_for_err: Option<&[u8]>) -> Result<()> {
        let conn = self.conn_mut()?;
        Executor::execute(conn, sqlx::raw_sql(sql))
            .await
            .map_err(|e| PgStoreError::from_sqlx(key_for_err, &e))?;
        Ok(())
    }

    /// Build a parameterised query with the configured `.persistent()` flag.
    #[inline]
    fn build_query(
        persistent: bool,
        sql: &str,
    ) -> sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments> {
        sqlx::query(sql).persistent(persistent)
    }

    /// Pop the last savepoint name, returning `None` if the stack is empty.
    fn pop_savepoint_name(&mut self) -> Option<String> {
        self.savepoints.pop()
    }

    /// Build the next unique savepoint name and push it onto the stack.
    ///
    /// Uses `format!()` to produce the savepoint name `sp_N` where N is a
    /// monotonically increasing counter (wrapping at u32::MAX).
    fn push_savepoint_name(&mut self) -> String {
        // Use wrapping_add so that overflow at u32::MAX wraps to 0.
        // In practice, 4 billion savepoints in a single transaction is
        // impossible, so collision with earlier names is not a concern.
        self.savepoint_counter = self.savepoint_counter.wrapping_add(1);
        let name = format!("sp_{}", self.savepoint_counter);
        self.savepoints.push(name.clone());
        name
    }

    /// Build a savepoint SQL statement.
    ///
    /// `prefix` is e.g. `"SAVEPOINT "`, `"RELEASE SAVEPOINT "`, or
    /// `"ROLLBACK TO SAVEPOINT "`. The savepoint name is appended
    /// directly. Uses `format!()` for safety — savepoint operations
    /// are not hot-path, so the allocation cost is acceptable.
    #[inline]
    fn savepoint_sql(prefix: &str, name: &str) -> String {
        format!("{prefix}{name}")
    }

    // ─── Transaction control ─────────────────────────────

    /// Commit the transaction.
    ///
    /// Uses sqlx's `Transaction::commit()` which sends `COMMIT` to PG
    /// and releases the connection back to the pool. On error (e.g.
    /// serialization conflict), the connection is still released — PG
    /// auto-rollbacks. After this call, `conn` is `None` so all
    /// subsequent operations return `TxClosed`.
    pub async fn commit(&mut self) -> Result<()> {
        // Take ownership of the Transaction — sqlx's commit() consumes self.
        // On success, PG commits. On error, PG auto-rollbacks.
        // conn.take() sets conn=None, making all subsequent operations
        // return TxClosed.
        let tx = self.conn.take().ok_or(PgStoreError::TxClosed)?;
        self.savepoints.clear();
        self.release_active();
        let result = tx.commit().await;
        if let Err(e) = &result {
            debug!("COMMIT failed (PG will auto-rollback): {e}");
        }
        result.map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        debug!("transaction committed");
        Ok(())
    }

    /// Rollback (cancel) the transaction.
    ///
    /// Uses sqlx's `Transaction::rollback()` which sends `ROLLBACK` to PG
    /// and releases the connection back to the pool. On error, the
    /// connection is still released via sqlx's Drop handling.
    pub async fn cancel(&mut self) -> Result<()> {
        // Take ownership of the Transaction — sqlx's rollback() consumes self.
        // conn.take() sets conn=None, making all subsequent operations
        // return TxClosed.
        let tx = self.conn.take().ok_or(PgStoreError::TxClosed)?;
        self.savepoints.clear();
        self.release_active();
        let result = tx.rollback().await;
        if let Err(e) = &result {
            debug!("ROLLBACK failed: {e}");
        }
        result.map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        debug!("transaction rolled back");
        Ok(())
    }

    /// Whether the transaction is still open.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.conn.is_some()
    }

    /// Whether this is a write transaction.
    #[must_use]
    pub fn is_writeable(&self) -> bool {
        self.writeable
    }

    // ─── Read operations ─────────────────────────────────
    //
    // All KV operation methods use "destructured borrows": they access
    // `self.conn` (mutable) and `self.sql` (immutable) as **separate
    // fields** of `self`. Rust allows simultaneous borrows of different
    // fields, so `&self.sql.field` and `conn.deref_mut()` coexist in the
    // same scope without `Arc::clone`. This eliminates per-operation
    // atomic refcount overhead on the hot path.

    /// Check whether a key exists.
    pub async fn exists(&mut self, key: Key) -> Result<bool> {
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &self.sql.exists)
            .bind(&key)
            .fetch_optional(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        Ok(row.is_some())
    }

    /// Get the value for a key.
    pub async fn get(&mut self, key: Key) -> Result<Option<Val>> {
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &self.sql.get)
            .bind(&key)
            .fetch_optional(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        Ok(row.map(|r| r.get::<Vec<u8>, _>("val")))
    }

    /// Batch-get multiple keys.
    pub async fn getm(&mut self, keys: Vec<Key>) -> Result<Vec<Option<Val>>> {
        if self.conn.is_none() {
            return Err(PgStoreError::TxClosed);
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let keys_ref: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;

        let rows = Self::build_query(persistent, &self.sql.getm)
            .bind(&keys_ref)
            .fetch_all(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        // For small result sets, linear scan has better cache locality
        // than a HashMap. We check the product of keys × rows to avoid
        // O(n²) blow-up when many keys are requested but few exist.
        // Threshold: 8192 comparisons ≈ 128 keys × 64 rows.
        //
        // We extract key/val from rows **once** into a Vec of tuples,
        // then search that Vec. This avoids calling r.get::<Vec<u8>,_>("key")
        // repeatedly inside find() which would allocate a new Vec per comparison.
        let use_linear = rows.len() <= 64 && rows.len().saturating_mul(keys.len()) <= 8192;
        if use_linear {
            let extracted = Self::rows_to_pairs(rows);
            Ok(keys
                .into_iter()
                .map(|k| {
                    extracted
                        .iter()
                        .find(|(row_key, _)| *row_key == k)
                        .map(|(_, v)| v.clone())
                })
                .collect())
        } else {
            let mut map = std::collections::HashMap::with_capacity(rows.len());
            for (k, v) in Self::rows_to_pairs(rows) {
                map.insert(k, v);
            }
            Ok(keys.into_iter().map(|k| map.get(&k).cloned()).collect())
        }
    }

    // ─── Write operations ────────────────────────────────

    /// Set a key to a value (insert or update).
    pub async fn set(&mut self, key: Key, val: Val) -> Result<()> {
        if self.conn.is_none() {
            return Err(PgStoreError::TxClosed);
        }
        self.check_writable()?;
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        Self::build_query(persistent, &self.sql.set)
            .bind(key.as_slice())
            .bind(val.as_slice())
            .execute(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        trace!(key_len = key.len(), "set");
        Ok(())
    }

    /// Batch-set multiple key-value pairs in a single SQL statement.
    ///
    /// Uses `UNNEST` to send all pairs as two array parameters, then a single
    /// `INSERT ... ON CONFLICT DO UPDATE` executes atomically. This reduces
    /// N individual `set` calls (N network round-trips) to 1 round-trip.
    ///
    /// If `pairs` is empty, returns immediately without hitting the DB.
    ///
    /// **O2: Parameter limit protection.** PostgreSQL limits each query to
    /// 65,535 parameters. `setm` uses 2 array parameters, but the total
    /// element count per array must stay below 32,767 to be safe. When
    /// `pairs` exceeds [`SETM_MAX_PAIRS`], the batch is automatically
    /// chunked into multiple sequential executions. Each chunk is atomic
    /// within the encompassing transaction.
    pub async fn setm(&mut self, pairs: Vec<(Key, Val)>) -> Result<()> {
        if self.conn.is_none() {
            return Err(PgStoreError::TxClosed);
        }
        self.check_writable()?;
        if pairs.is_empty() {
            return Ok(());
        }

        // O2: Chunk if the batch exceeds PG's parameter limit.
        // Each pair becomes 2 array elements (key + val), and PG's
        // max_parameters = 65,535. We use 32,000 as a conservative limit.
        if pairs.len() <= SETM_MAX_PAIRS {
            return self.setm_batch(&pairs).await;
        }

        // Process in chunks of SETM_MAX_PAIRS.
        let total = pairs.len();
        let mut start = 0;
        while start < total {
            let end = (start + SETM_MAX_PAIRS).min(total);
            self.setm_batch(&pairs[start..end]).await?;
            start = end;
        }
        trace!(
            count = total,
            chunks = total.div_ceil(SETM_MAX_PAIRS),
            "setm (chunked)"
        );
        Ok(())
    }

    /// Internal: execute a single `setm` batch (≤ `SETM_MAX_PAIRS` pairs).
    ///
    /// Borrows `pairs` as a slice to avoid moving the full Vec when chunking.
    async fn setm_batch(&mut self, pairs: &[(Key, Val)]) -> Result<()> {
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;

        // Split into two Vec<Vec<u8>> for UNNEST binding.
        let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
        let vals: Vec<&[u8]> = pairs.iter().map(|(_, v)| v.as_slice()).collect();

        Self::build_query(persistent, &self.sql.setm)
            .bind(&keys)
            .bind(&vals)
            .execute(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        trace!(count = pairs.len(), "setm batch");
        Ok(())
    }

    /// Set a key only if it does not already exist (insert-if-absent).
    /// Returns `KeyAlreadyExists` if the key exists.
    pub async fn put(&mut self, key: Key, val: Val) -> Result<()> {
        if self.conn.is_none() {
            return Err(PgStoreError::TxClosed);
        }
        self.check_writable()?;
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let result = Self::build_query(persistent, &self.sql.put)
            .bind(key.as_slice())
            .bind(val.as_slice())
            .execute(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        if result.rows_affected() == 0 {
            return Err(PgStoreError::KeyAlreadyExists(key.into_boxed_slice()));
        }
        Ok(())
    }

    /// Compare-and-swap: set a key only if its current value equals `chk`.
    /// `chk = None` means "only if key does not exist" (delegates to `put`).
    /// `chk = Some(v)` means "only if current value equals v".
    pub async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
        if self.conn.is_none() {
            return Err(PgStoreError::TxClosed);
        }
        self.check_writable()?;
        let Some(expected) = chk else {
            return self.put(key, val).await;
        };

        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let affected = Self::build_query(persistent, &self.sql.putc)
            .bind(key.as_slice())
            .bind(val.as_slice())
            .bind(expected.as_slice())
            .execute(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?
            .rows_affected();

        if affected == 0 {
            Err(PgStoreError::ConditionNotMet(key.into_boxed_slice()))
        } else {
            Ok(())
        }
    }

    /// Delete a key.
    pub async fn del(&mut self, key: Key) -> Result<()> {
        if self.conn.is_none() {
            return Err(PgStoreError::TxClosed);
        }
        self.check_writable()?;
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        Self::build_query(persistent, &self.sql.del)
            .bind(key.as_slice())
            .execute(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        trace!(key_len = key.len(), "del");
        Ok(())
    }

    /// Compare-and-delete: delete a key only if its current value equals `chk`.
    /// `chk = None` → unconditional delete (delegates to `del`).
    /// `chk = Some(v)` → key must exist and value must equal v.
    pub async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
        if self.conn.is_none() {
            return Err(PgStoreError::TxClosed);
        }
        self.check_writable()?;
        let Some(expected) = chk else {
            return self.del(key).await;
        };

        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let result = Self::build_query(persistent, &self.sql.delc)
            .bind(key.as_slice())
            .bind(expected.as_slice())
            .execute(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        if result.rows_affected() == 0 {
            Err(PgStoreError::ConditionNotMet(key.into_boxed_slice()))
        } else {
            Ok(())
        }
    }

    /// Delete all keys in a range (inclusive start, exclusive end).
    pub async fn delr(&mut self, rng: Range<Key>) -> Result<()> {
        if self.conn.is_none() {
            return Err(PgStoreError::TxClosed);
        }
        self.check_writable()?;
        // Empty range — skip DB round-trip.
        if rng.start >= rng.end {
            return Ok(());
        }
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let deleted = Self::build_query(persistent, &self.sql.delr)
            .bind(rng.start.as_slice())
            .bind(rng.end.as_slice())
            .execute(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?
            .rows_affected();
        trace!(deleted, "delr");
        Ok(())
    }

    // ─── Range scans ─────────────────────────────────────
    //
    // SurrealDB pagination is driven by the `Transactable` trait's default
    // `DefaultKeysCursor` / `DefaultValsCursor`: each batch calls
    // `keys()`/`scan()` with `skip=0` (after the first batch) and advances
    // `range.start` to `last_key + \x00`. This is equivalent to keyset
    // pagination (`WHERE key > $cursor`), so no separate cursor SQL is
    // needed. See the `Sql` struct doc comment for details.

    /// Internal: execute an OFFSET-based range scan, returning raw rows.
    ///
    /// This is an associated function (not `&mut self`) so that callers can
    /// pass `&self.sql.xxx` as `range_sql` without conflicting with the
    /// `&mut self.conn` borrow — Rust sees them as borrows of different
    /// fields, which is allowed.
    async fn range_query_offset(
        conn: &mut sqlx::postgres::PgConnection,
        persistent: bool,
        range_sql: &str,
        rng: Range<Key>,
        limit: u32,
        skip: u32,
    ) -> Result<Vec<sqlx::postgres::PgRow>> {
        // Empty range — skip DB round-trip.
        if rng.start >= rng.end {
            return Ok(Vec::new());
        }
        if skip > 1000 {
            warn!(
                skip,
                limit, "large OFFSET in range scan — consider cursor-based pagination"
            );
        }
        Self::build_query(persistent, range_sql)
            .bind(rng.start.as_slice())
            .bind(rng.end.as_slice())
            .bind(limit as i64)
            .bind(skip as i64)
            .fetch_all(conn)
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))
    }

    /// Internal: extract keys from rows.
    fn rows_to_keys(rows: Vec<sqlx::postgres::PgRow>) -> Vec<Key> {
        rows.into_iter()
            .map(|r| r.get::<Vec<u8>, _>("key"))
            .collect()
    }

    /// Internal: extract key-value pairs from rows.
    fn rows_to_pairs(rows: Vec<sqlx::postgres::PgRow>) -> Vec<(Key, Val)> {
        rows.into_iter()
            .map(|r| (r.get::<Vec<u8>, _>("key"), r.get::<Vec<u8>, _>("val")))
            .collect()
    }

    /// Scan keys in a range (ascending).
    pub async fn keys(&mut self, rng: Range<Key>, limit: u32, skip: u32) -> Result<Vec<Key>> {
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let rows = Self::range_query_offset(
            conn.deref_mut(),
            persistent,
            &self.sql.range_keys_asc,
            rng,
            limit,
            skip,
        )
        .await?;
        Ok(Self::rows_to_keys(rows))
    }

    /// Scan keys in a range (descending).
    pub async fn keysr(&mut self, rng: Range<Key>, limit: u32, skip: u32) -> Result<Vec<Key>> {
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let rows = Self::range_query_offset(
            conn.deref_mut(),
            persistent,
            &self.sql.range_keys_desc,
            rng,
            limit,
            skip,
        )
        .await?;
        Ok(Self::rows_to_keys(rows))
    }

    /// Scan key-value pairs in a range (ascending).
    pub async fn scan(
        &mut self,
        rng: Range<Key>,
        limit: u32,
        skip: u32,
    ) -> Result<Vec<(Key, Val)>> {
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let rows = Self::range_query_offset(
            conn.deref_mut(),
            persistent,
            &self.sql.range_kv_asc,
            rng,
            limit,
            skip,
        )
        .await?;
        Ok(Self::rows_to_pairs(rows))
    }

    /// Scan key-value pairs in a range (descending).
    pub async fn scanr(
        &mut self,
        rng: Range<Key>,
        limit: u32,
        skip: u32,
    ) -> Result<Vec<(Key, Val)>> {
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let rows = Self::range_query_offset(
            conn.deref_mut(),
            persistent,
            &self.sql.range_kv_desc,
            rng,
            limit,
            skip,
        )
        .await?;
        Ok(Self::rows_to_pairs(rows))
    }

    /// Count keys in a range.
    pub async fn count(&mut self, rng: Range<Key>) -> Result<u64> {
        // B1: Check open first — consistency with all other methods.
        if self.conn.is_none() {
            return Err(PgStoreError::TxClosed);
        }
        // Empty range — skip DB round-trip.
        if rng.start >= rng.end {
            return Ok(0);
        }
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &self.sql.count)
            .bind(rng.start.as_slice())
            .bind(rng.end.as_slice())
            .fetch_one(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        // COUNT(*) always returns ≥ 0; .max(0) is purely defensive —
        // sqlx should never return a negative i64 for COUNT(*), but we
        // guard against driver/encoding edge cases before casting to u64.
        Ok(row.get::<i64, _>("cnt").max(0) as u64)
    }

    /// Approximate row count using `pg_class.reltuples`.
    ///
    /// Returns an O(1) estimate based on the most recent `ANALYZE` statistics.
    /// This is a **whole-table** estimate — the `range` parameters from the
    /// calling `Transactable` method are **not** passed to `pg_class`, so the
    /// result reflects the entire table regardless of the key range. Returns
    /// `None` if the table has never been analyzed (i.e. `reltuples = -1`).
    /// Returns `Some(0)` for an analyzed empty table.
    ///
    /// **Note**: The estimate may be stale if `ANALYZE` hasn't run recently.
    pub async fn count_approx(&mut self) -> Result<Option<u64>> {
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &self.sql.count_approx)
            .bind(&self.sql.table_name)
            .fetch_optional(conn.deref_mut())
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        Ok(row.map(|r| r.get::<i64, _>("approx_cnt") as u64))
    }

    // ─── Savepoints (PG native) ──────────────────────────

    /// Create a new savepoint within the current transaction.
    pub async fn new_save_point(&mut self) -> Result<()> {
        let name = self.push_savepoint_name();
        let sql = Self::savepoint_sql("SAVEPOINT ", &name);
        self.execute_simple(&sql, None).await?;
        debug!(savepoint = %name, "savepoint created");
        Ok(())
    }

    /// Release the last savepoint.
    pub async fn release_last_save_point(&mut self) -> Result<()> {
        let Some(name) = self.pop_savepoint_name() else {
            return Ok(());
        };
        let sql = Self::savepoint_sql("RELEASE SAVEPOINT ", &name);
        self.execute_simple(&sql, None).await?;
        debug!(savepoint = %name, "savepoint released");
        Ok(())
    }

    /// Rollback to the last savepoint.
    ///
    /// Executes `ROLLBACK TO SAVEPOINT <name>` followed by `RELEASE SAVEPOINT <name>`.
    /// If the RELEASE fails (network error, etc.), the savepoint name is pushed
    /// back onto the stack so the caller can retry release or rollback. This
    /// keeps the internal savepoint stack consistent with the PG server state
    /// (the savepoint still exists on the PG side because RELEASE failed).
    pub async fn rollback_to_save_point(&mut self) -> Result<()> {
        let Some(name) = self.pop_savepoint_name() else {
            return Ok(());
        };
        let rollback_sql = Self::savepoint_sql("ROLLBACK TO SAVEPOINT ", &name);
        let release_sql = Self::savepoint_sql("RELEASE SAVEPOINT ", &name);
        self.execute_simple(&rollback_sql, None).await?;
        // M1: If RELEASE fails, push the name back so the stack stays
        // consistent with PG state. The caller can retry release_last_save_point
        // or rollback_to_save_point again.
        if let Err(e) = self.execute_simple(&release_sql, None).await {
            self.savepoints.push(name);
            return Err(e);
        }
        debug!(savepoint = %name, "rolled back to savepoint");
        Ok(())
    }

    /// Get the number of active savepoints.
    #[must_use]
    pub fn savepoint_depth(&self) -> usize {
        self.savepoints.len()
    }
}

impl Drop for PgTransaction {
    fn drop(&mut self) {
        // SurrealDB's engine routinely drops transactions without calling
        // commit()/cancel() — this is by design for internal housekeeping
        // paths (node registration, cluster events, etc.).
        //
        // sqlx's `Transaction::drop` automatically calls `start_rollback`,
        // which synchronously queues a ROLLBACK command into the connection's
        // write buffer (without flushing). The ROLLBACK is executed when the
        // connection is next used or returned to the pool (via `ping()`).
        // This ensures the connection always returns to the pool in Idle
        // state, preventing the "there is already a transaction in progress"
        // WARNING on the next `begin()` call.
        //
        // Decrement the active-transaction counter so that pool diagnostics
        // can accurately report how many connections are held by transactions.
        if self.conn.is_some() {
            self.release_active();
            debug!("PgTransaction dropped without explicit commit/cancel; sqlx will auto-rollback");
        }
    }
}
