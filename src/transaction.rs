//! PgTransaction — implements Transactable over a PostgreSQL transaction.
//!
//! This is the core of the backend: every SurrealDB KV operation maps to one
//! or more SQL statements executed within a single PG transaction.

use std::ops::{DerefMut, Range};
use std::sync::Arc;

use sqlx::{Executor, Row};
use tracing::{debug, trace, warn};

use crate::config::PgIsolation;
use crate::error::{PgStoreError, Result};

/// Type aliases matching SurrealDB conventions
pub type Key = Vec<u8>;
pub type Val = Vec<u8>;

// ─── Pre-built SQL ───────────────────────────────────────

/// Pre-built SQL strings for all KV operations.
///
/// Constructed once when a `PgTransaction` is created. Stored separately from
/// the connection so that SQL references (`&sql.x`) don't conflict with
/// `&mut self` borrows on the connection.
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
struct Sql {
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
    fn new(table: &str) -> Self {
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
                 WHERE relname = $1 AND reltuples > 0",
            ),
        }
    }
}

// ─── PgTransaction ──────────────────────────────────────

/// A transaction backed by a single PostgreSQL connection.
///
/// Implements all KV operations that SurrealDB's `Transactable` trait requires.
/// After `commit()` or `cancel()`, the transaction is closed and the connection
/// is returned to the pool.
///
/// All SQL strings are pre-built at construction time to eliminate per-operation
/// `format!()` heap allocations on the hot path.
pub struct PgTransaction {
    /// The dedicated PG connection (returned to pool on drop)
    conn: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
    /// Whether this is a write transaction
    writeable: bool,
    /// Whether the transaction has been closed
    closed: bool,
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
    /// Pre-built SQL strings, shared via Arc so cloning is an atomic
    /// increment (no heap allocation) and borrows don't conflict with
    /// `&mut self` on the connection.
    sql: Arc<Sql>,
}

impl PgTransaction {
    /// Create a new transaction wrapping an acquired PG connection.
    ///
    /// The caller must have already executed `BEGIN` on the connection.
    /// All SQL strings are pre-built here once, avoiding per-operation allocations.
    pub(crate) fn new(
        conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
        writeable: bool,
        isolation: PgIsolation,
        persistent: bool,
        table: &str,
    ) -> Self {
        Self {
            conn: Some(conn),
            writeable,
            closed: false,
            savepoint_counter: 0,
            savepoints: Vec::new(),
            isolation,
            persistent,
            sql: Arc::new(Sql::new(table)),
        }
    }

    // ─── Internal helpers ────────────────────────────────

    /// Get a mutable reference to the inner `PgConnection`.
    fn conn_mut(&mut self) -> Result<&mut sqlx::postgres::PgConnection> {
        if self.closed {
            return Err(PgStoreError::TxClosed);
        }
        let conn = self.conn.as_mut().ok_or(PgStoreError::TxClosed)?;
        Ok(conn.deref_mut())
    }

    fn check_writable(&self) -> Result<()> {
        if !self.writeable {
            return Err(PgStoreError::TxReadOnly);
        }
        Ok(())
    }

    /// Release the connection back to the pool (after commit/rollback).
    fn close(&mut self) {
        self.closed = true;
        self.savepoints.clear();
        let _ = self.conn.take();
    }

    /// Execute a parameterless SQL statement via simple query protocol.
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
    /// Uses stack-allocated buffer instead of `format!()` to avoid
    /// heap allocation. Savepoint names follow the pattern `sp_N`
    /// where N is a monotonically increasing counter.
    ///
    /// Returns both the name (for the savepoint stack) and the full
    /// SQL prefix (e.g. `"SAVEPOINT sp_3"`) to avoid a second allocation.
    fn push_savepoint_name(&mut self) -> String {
        // Use wrapping_add so that overflow at u32::MAX wraps to 0.
        // In practice, 4 billion savepoints in a single transaction is
        // impossible, so collision with earlier names is not a concern.
        self.savepoint_counter = self.savepoint_counter.wrapping_add(1);
        let n = self.savepoint_counter;
        // Manually format u32 to ASCII into a stack buffer.
        // sp_ prefix + up to 10 digits = 13 bytes, fits in [u8; 16].
        let mut buf = [0u8; 16];
        buf[0] = b's';
        buf[1] = b'p';
        buf[2] = b'_';
        let mut pos = 3;
        let mut remaining = n;
        let mut digits = [0u8; 10];
        let mut d_pos = 0;
        while remaining > 0 {
            digits[d_pos] = (remaining % 10) as u8;
            remaining /= 10;
            d_pos += 1;
        }
        if d_pos == 0 {
            // n == 0 after overflow wrapping
            buf[pos] = b'0';
            pos += 1;
        } else {
            for i in (0..d_pos).rev() {
                buf[pos] = digits[i] + b'0';
                pos += 1;
            }
        }
        // Safety: buf[..pos] is always valid UTF-8 because we only
        // write ASCII bytes ('s', 'p', '_', '0'-'9') into the buffer.
        // Using from_utf8_unchecked avoids a redundant validation branch,
        // but we add a debug_assert for defense-in-depth.
        let name_slice = &buf[..pos];
        debug_assert!(std::str::from_utf8(name_slice).is_ok(),
            "savepoint name must be valid UTF-8");
        let name = unsafe { std::str::from_utf8_unchecked(name_slice) }.to_string();
        self.savepoints.push(name.clone());
        name
    }

    /// Build a savepoint SQL statement on the stack without `format!()`.
    ///
    /// `prefix` is e.g. `"SAVEPOINT "`, `"RELEASE SAVEPOINT "`, or
    /// `"ROLLBACK TO SAVEPOINT "`. The savepoint name is appended
    /// directly into a stack buffer.
    #[inline]
    fn savepoint_sql(prefix: &str, name: &str) -> String {
        // Max: "ROLLBACK TO SAVEPOINT sp_" + 10 digits = 36 bytes.
        // Buffer [u8; 48] provides ample headroom.
        let mut buf = [0u8; 48];
        let prefix_len = prefix.len();
        let name_bytes = name.as_bytes();
        let total = prefix_len + name_bytes.len();
        debug_assert!(
            total <= buf.len(),
            "savepoint SQL buffer overflow: {prefix_len} + {name_len} > {buf_len}",
            prefix_len = prefix_len,
            name_len = name_bytes.len(),
            buf_len = buf.len(),
        );
        buf[..prefix_len].copy_from_slice(prefix.as_bytes());
        buf[prefix_len..total].copy_from_slice(name_bytes);
        // Safety: same as push_savepoint_name — only ASCII bytes.
        debug_assert!(std::str::from_utf8(&buf[..total]).is_ok(),
            "savepoint SQL must be valid UTF-8");
        unsafe { std::str::from_utf8_unchecked(&buf[..total]) }.to_string()
    }

    // ─── Transaction control ─────────────────────────────

    /// Commit the transaction.
    ///
    /// On success, the connection is released back to the pool.
    /// On error (e.g. serialization conflict), the connection is still
    /// released — PG auto-rollbacks on connection drop. The transaction
    /// is marked closed regardless of outcome to prevent further operations.
    pub async fn commit(&mut self) -> Result<()> {
        let result = self.execute_simple("COMMIT", None).await;
        self.close(); // Always close — even on error, connection goes back to pool
        result?;
        debug!("transaction committed");
        Ok(())
    }

    /// Rollback (cancel) the transaction.
    ///
    /// On success, the connection is released back to the pool.
    /// On error, the connection is still released via `close()`.
    pub async fn cancel(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let result = self.execute_simple("ROLLBACK", None).await;
        self.close(); // Always close — release connection even on error
        result?;
        debug!("transaction rolled back");
        Ok(())
    }

    /// Whether the transaction is still open.
    #[must_use]
    pub fn is_open(&self) -> bool {
        !self.closed
    }

    /// Whether this is a write transaction.
    #[must_use]
    pub fn is_writeable(&self) -> bool {
        self.writeable
    }

    // ─── Read operations ─────────────────────────────────

    /// Check whether a key exists.
    pub async fn exists(&mut self, key: Key) -> Result<bool> {
        let sql = self.sql.clone();
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &sql.exists)
            .bind(&key)
            .fetch_optional(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        Ok(row.is_some())
    }

    /// Get the value for a key.
    pub async fn get(&mut self, key: Key) -> Result<Option<Val>> {
        let sql = self.sql.clone();
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &sql.get)
            .bind(&key)
            .fetch_optional(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        Ok(row.map(|r| r.get::<Vec<u8>, _>("val")))
    }

    /// Batch-get multiple keys.
    pub async fn getm(&mut self, keys: Vec<Key>) -> Result<Vec<Option<Val>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let keys_ref: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let sql = self.sql.clone();
        let persistent = self.persistent;

        let rows = Self::build_query(persistent, &sql.getm)
            .bind(&keys_ref)
            .fetch_all(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        // For small result sets, linear scan has better cache locality
        // than a HashMap. We check the product of keys × rows to avoid
        // O(n²) blow-up when many keys are requested but few exist.
        // Threshold: 8192 comparisons ≈ 128 keys × 64 rows.
        let use_linear = rows.len() <= 64 && (rows.len() as usize).saturating_mul(keys.len()) <= 8192;
        if use_linear {
            Ok(keys.into_iter()
                .map(|k| {
                    rows.iter()
                        .find(|r| r.get::<Vec<u8>, _>("key") == k)
                        .map(|r| r.get::<Vec<u8>, _>("val"))
                })
                .collect())
        } else {
            let mut map = std::collections::HashMap::with_capacity(rows.len());
            for row in rows {
                let k: Vec<u8> = row.get::<Vec<u8>, _>("key");
                let v: Vec<u8> = row.get::<Vec<u8>, _>("val");
                map.insert(k, v);
            }
            Ok(keys.into_iter().map(|k| map.get(&k).cloned()).collect())
        }
    }

    // ─── Write operations ────────────────────────────────

    /// Set a key to a value (insert or update).
    pub async fn set(&mut self, key: Key, val: Val) -> Result<()> {
        self.check_writable()?;
        let sql = self.sql.clone();
        let persistent = self.persistent;
        Self::build_query(persistent, &sql.set)
            .bind(key.as_slice())
            .bind(val.as_slice())
            .execute(self.conn_mut()?)
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
    pub async fn setm(&mut self, pairs: Vec<(Key, Val)>) -> Result<()> {
        self.check_writable()?;
        if pairs.is_empty() {
            return Ok(());
        }

        let sql = self.sql.clone();
        let persistent = self.persistent;

        // Split into two Vec<Vec<u8>> for UNNEST binding.
        let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
        let vals: Vec<&[u8]> = pairs.iter().map(|(_, v)| v.as_slice()).collect();

        Self::build_query(persistent, &sql.setm)
            .bind(&keys)
            .bind(&vals)
            .execute(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        trace!(count = pairs.len(), "setm");
        Ok(())
    }

    /// Set a key only if it does not already exist (insert-if-absent).
    /// Returns `KeyAlreadyExists` if the key exists.
    pub async fn put(&mut self, key: Key, val: Val) -> Result<()> {
        self.check_writable()?;
        let sql = self.sql.clone();
        let persistent = self.persistent;
        let result = Self::build_query(persistent, &sql.put)
            .bind(key.as_slice())
            .bind(val.as_slice())
            .execute(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        if result.rows_affected() == 0 {
            return Err(PgStoreError::KeyAlreadyExists(key));
        }
        Ok(())
    }

    /// Compare-and-swap: set a key only if its current value equals `chk`.
    /// `chk = None` means "only if key does not exist" (delegates to `put`).
    /// `chk = Some(v)` means "only if current value equals v".
    pub async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
        self.check_writable()?;
        let Some(expected) = chk else {
            return self.put(key, val).await;
        };

        let sql = self.sql.clone();
        let persistent = self.persistent;
        let affected = Self::build_query(persistent, &sql.putc)
            .bind(key.as_slice())
            .bind(val.as_slice())
            .bind(expected.as_slice())
            .execute(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?
            .rows_affected();

        if affected == 0 {
            Err(PgStoreError::ConditionNotMet(key))
        } else {
            Ok(())
        }
    }

    /// Delete a key.
    pub async fn del(&mut self, key: Key) -> Result<()> {
        self.check_writable()?;
        let sql = self.sql.clone();
        let persistent = self.persistent;
        Self::build_query(persistent, &sql.del)
            .bind(key.as_slice())
            .execute(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        trace!(key_len = key.len(), "del");
        Ok(())
    }

    /// Compare-and-delete: delete a key only if its current value equals `chk`.
    /// `chk = None` → unconditional delete (delegates to `del`).
    /// `chk = Some(v)` → key must exist and value must equal v.
    pub async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
        self.check_writable()?;
        let Some(expected) = chk else {
            return self.del(key).await;
        };

        let sql = self.sql.clone();
        let persistent = self.persistent;
        let result = Self::build_query(persistent, &sql.delc)
            .bind(key.as_slice())
            .bind(expected.as_slice())
            .execute(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(&key), &e))?;
        if result.rows_affected() == 0 {
            Err(PgStoreError::ConditionNotMet(key))
        } else {
            Ok(())
        }
    }

    /// Delete all keys in a range (inclusive start, exclusive end).
    pub async fn delr(&mut self, rng: Range<Key>) -> Result<()> {
        self.check_writable()?;
        // Empty range — skip DB round-trip.
        if rng.start >= rng.end {
            return Ok(());
        }
        let sql = self.sql.clone();
        let persistent = self.persistent;
        let deleted = Self::build_query(persistent, &sql.delr)
            .bind(rng.start.as_slice())
            .bind(rng.end.as_slice())
            .execute(self.conn_mut()?)
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
    async fn range_query_offset(
        &mut self,
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
                limit,
                "large OFFSET in range scan — consider cursor-based pagination"
            );
        }
        let persistent = self.persistent;
        Self::build_query(persistent, range_sql)
            .bind(rng.start.as_slice())
            .bind(rng.end.as_slice())
            .bind(limit as i64)
            .bind(skip as i64)
            .fetch_all(self.conn_mut()?)
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
        let sql = self.sql.clone();
        let rows = self.range_query_offset(&sql.range_keys_asc, rng, limit, skip).await?;
        Ok(Self::rows_to_keys(rows))
    }

    /// Scan keys in a range (descending).
    pub async fn keysr(&mut self, rng: Range<Key>, limit: u32, skip: u32) -> Result<Vec<Key>> {
        let sql = self.sql.clone();
        let rows = self.range_query_offset(&sql.range_keys_desc, rng, limit, skip).await?;
        Ok(Self::rows_to_keys(rows))
    }

    /// Scan key-value pairs in a range (ascending).
    pub async fn scan(
        &mut self,
        rng: Range<Key>,
        limit: u32,
        skip: u32,
    ) -> Result<Vec<(Key, Val)>> {
        let sql = self.sql.clone();
        let rows = self.range_query_offset(&sql.range_kv_asc, rng, limit, skip).await?;
        Ok(Self::rows_to_pairs(rows))
    }

    /// Scan key-value pairs in a range (descending).
    pub async fn scanr(
        &mut self,
        rng: Range<Key>,
        limit: u32,
        skip: u32,
    ) -> Result<Vec<(Key, Val)>> {
        let sql = self.sql.clone();
        let rows = self.range_query_offset(&sql.range_kv_desc, rng, limit, skip).await?;
        Ok(Self::rows_to_pairs(rows))
    }

    /// Count keys in a range.
    pub async fn count(&mut self, rng: Range<Key>) -> Result<u64> {
        // Empty range — skip DB round-trip.
        if rng.start >= rng.end {
            return Ok(0);
        }
        let sql = self.sql.clone();
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &sql.count)
            .bind(rng.start.as_slice())
            .bind(rng.end.as_slice())
            .fetch_one(self.conn_mut()?)
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
    /// This is a **whole-table** estimate (range parameters are ignored) and
    /// may be stale if `ANALYZE` hasn't run recently. Returns `None` if the
    /// table has no statistics.
    pub async fn count_approx(&mut self) -> Result<Option<u64>> {
        let sql = self.sql.clone();
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &sql.count_approx)
            .bind(&*sql.table_name)
            .fetch_optional(self.conn_mut()?)
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
    pub async fn rollback_to_save_point(&mut self) -> Result<()> {
        let Some(name) = self.pop_savepoint_name() else {
            return Ok(());
        };
        let rollback_sql = Self::savepoint_sql("ROLLBACK TO SAVEPOINT ", &name);
        let release_sql = Self::savepoint_sql("RELEASE SAVEPOINT ", &name);
        self.execute_simple(&rollback_sql, None).await?;
        self.execute_simple(&release_sql, None).await?;
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
        if !self.closed {
            warn!(
                "PgTransaction dropped without explicit commit/cancel; PG will auto-rollback"
            );
        }
    }
}
