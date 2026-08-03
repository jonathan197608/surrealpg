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

// ─── ScanMode ────────────────────────────────────────────

/// Pagination mode for range scans.
///
/// `Offset` is the traditional `OFFSET $4` mode (compatible with the
/// existing `Transactable` trait). `After` uses keyset pagination
/// (`WHERE key > $cursor`) which avoids the linear-scan cost of deep
/// OFFSET on large tables.
#[derive(Clone)]
pub enum ScanMode {
    /// Traditional OFFSET-based pagination (default, backward-compatible).
    Offset(u32),
    /// Keyset (cursor) pagination: return rows after the given cursor key.
    /// The cursor is typically the last key returned by a previous scan,
    /// enabling O(limit) deep-page performance instead of O(skip+limit).
    After(Key),
}

impl Default for ScanMode {
    fn default() -> Self {
        Self::Offset(0)
    }
}

// ─── Pre-built SQL ───────────────────────────────────────

/// Pre-built SQL strings for all KV operations.
///
/// Constructed once when a `PgTransaction` is created. Stored separately from
/// the connection so that SQL references (`&sql.x`) don't conflict with
/// `&mut self` borrows on the connection.
struct Sql {
    exists: String,
    get: String,
    getm: String,
    set: String,
    put: String,
    putc: String,
    del: String,
    delc: String,
    delr: String,
    count: String,
    /// Range scan prefix with `{select}` and `{direction}` placeholders.
    range_prefix: String,
    /// Range scan prefix for keyset pagination with `{select}` placeholder.
    range_after_prefix: String,
    /// Original table name (for `count_approx` / `pg_class` queries).
    table_name: String,
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
            put: format!(
                "INSERT INTO {table} (key, val) VALUES ($1, $2) \
                 ON CONFLICT (key) DO NOTHING"
            ),
            putc: format!("UPDATE {table} SET val = $2 WHERE key = $1 AND val = $3"),
            del: format!("DELETE FROM {table} WHERE key = $1"),
            delc: format!("DELETE FROM {table} WHERE key = $1 AND val = $2"),
            delr: format!("DELETE FROM {table} WHERE key >= $1 AND key < $2"),
            count: format!("SELECT count(*) AS cnt FROM {table} WHERE key >= $1 AND key < $2"),
            range_prefix: format!(
                "SELECT {{select}} FROM {table} WHERE key >= $1 AND key < $2 \
                 ORDER BY key {{direction}} LIMIT $3 OFFSET $4"
            ),
            range_after_prefix: format!(
                "SELECT {{select}} FROM {table} WHERE key >= $1 AND key < $2 AND key > $3 \
                 ORDER BY key {{direction}} LIMIT $4"
            ),
            table_name: table.to_string(),
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
    /// Last key returned by a range scan (ascending), used to auto-switch
    /// to keyset pagination on subsequent calls when the caller passes
    /// `ScanMode::After` with this cursor.
    last_scan_key_asc: Option<Key>,
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
            last_scan_key_asc: None,
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
    fn build_query<'a>(
        persistent: bool,
        sql: &'a str,
    ) -> sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments> {
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
    fn push_savepoint_name(&mut self) -> String {
        self.savepoint_counter += 1;
        let mut buf = [0u8; 16];
        buf[0] = b's';
        buf[1] = b'p';
        buf[2] = b'_';
        let n = self.savepoint_counter;
        let mut pos = 3;
        if n == 0 {
            buf[pos] = b'0';
            pos += 1;
        } else {
            let mut remaining = n;
            let mut digits = [0u8; 10];
            let mut d_pos = 0;
            while remaining > 0 {
                digits[d_pos] = (remaining % 10) as u8;
                remaining /= 10;
                d_pos += 1;
            }
            for i in (0..d_pos).rev() {
                buf[pos] = digits[i] + b'0';
                pos += 1;
            }
        }
        let name = std::str::from_utf8(&buf[..pos])
            .expect("savepoint name is always valid UTF-8")
            .to_string();
        self.savepoints.push(name.clone());
        name
    }

    // ─── Transaction control ─────────────────────────────

    /// Commit the transaction.
    pub async fn commit(&mut self) -> Result<()> {
        self.execute_simple("COMMIT", None).await?;
        debug!("transaction committed");
        self.close();
        Ok(())
    }

    /// Rollback (cancel) the transaction.
    pub async fn cancel(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.execute_simple("ROLLBACK", None).await?;
        debug!("transaction rolled back");
        self.close();
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

        // For small result sets (≤64 rows), linear scan has better cache locality
        // than a HashMap. This covers the vast majority of batch-get calls.
        if rows.len() <= 64 {
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

    /// Internal: execute a range scan query, returning raw rows.
    ///
    /// Supports two pagination modes via `ScanMode`:
    /// - `Offset(skip)`: traditional `OFFSET` pagination (backward-compatible)
    /// - `After(cursor)`: keyset pagination using `WHERE key > $cursor`,
    ///   which avoids the O(skip) cost of deep OFFSET
    async fn range_query(
        &mut self,
        select: &str,
        rng: Range<Key>,
        limit: u32,
        mode: ScanMode,
        direction: &str,
    ) -> Result<Vec<sqlx::postgres::PgRow>> {
        let persistent = self.persistent;
        let sql = self.sql.clone();
        match mode {
            ScanMode::Offset(skip) => {
                if skip > 1000 {
                    warn!(
                        skip,
                        limit,
                        "large OFFSET in range scan — consider cursor-based pagination"
                    );
                }
                let sql = sql
                    .range_prefix
                    .replace("{select}", select)
                    .replace("{direction}", direction);
                let conn = self.conn_mut()?;
                Self::build_query(persistent, &sql)
                    .bind(rng.start.as_slice())
                    .bind(rng.end.as_slice())
                    .bind(limit as i64)
                    .bind(skip as i64)
                    .fetch_all(conn)
                    .await
                    .map_err(|e| PgStoreError::from_sqlx(None, &e))
            }
            ScanMode::After(cursor) => {
                let sql = sql
                    .range_after_prefix
                    .replace("{select}", select)
                    .replace("{direction}", direction);
                let conn = self.conn_mut()?;
                Self::build_query(persistent, &sql)
                    .bind(rng.start.as_slice())
                    .bind(rng.end.as_slice())
                    .bind(cursor.as_slice())
                    .bind(limit as i64)
                    .fetch_all(conn)
                    .await
                    .map_err(|e| PgStoreError::from_sqlx(None, &e))
            }
        }
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
        let rows = self.range_query("key", rng, limit, ScanMode::Offset(skip), "ASC").await?;
        Ok(Self::rows_to_keys(rows))
    }

    /// Scan keys in a range (descending).
    pub async fn keysr(&mut self, rng: Range<Key>, limit: u32, skip: u32) -> Result<Vec<Key>> {
        let rows = self.range_query("key", rng, limit, ScanMode::Offset(skip), "DESC").await?;
        Ok(Self::rows_to_keys(rows))
    }

    /// Scan key-value pairs in a range (ascending).
    pub async fn scan(
        &mut self,
        rng: Range<Key>,
        limit: u32,
        skip: u32,
    ) -> Result<Vec<(Key, Val)>> {
        let rows = self
            .range_query("key, val", rng, limit, ScanMode::Offset(skip), "ASC")
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
        let rows = self
            .range_query("key, val", rng, limit, ScanMode::Offset(skip), "DESC")
            .await?;
        Ok(Self::rows_to_pairs(rows))
    }

    /// Cursor-based key scan (ascending): return keys after the given cursor.
    ///
    /// Unlike `keys()` which uses `OFFSET`, this uses `WHERE key > $cursor`
    /// for O(limit) performance regardless of how deep the cursor is.
    /// If `cursor` is `None`, starts from the beginning of the range.
    pub async fn keys_after(
        &mut self,
        rng: Range<Key>,
        limit: u32,
        cursor: Option<Key>,
    ) -> Result<Vec<Key>> {
        let mode = match cursor {
            Some(c) => ScanMode::After(c),
            None => ScanMode::Offset(0),
        };
        let rows = self.range_query("key", rng, limit, mode, "ASC").await?;
        Ok(Self::rows_to_keys(rows))
    }

    /// Cursor-based key-value scan (ascending): return pairs after the given cursor.
    ///
    /// Automatically tracks the last key returned via `last_scan_key_asc`
    /// so callers can chain calls without manually managing cursors.
    pub async fn scan_after(
        &mut self,
        rng: Range<Key>,
        limit: u32,
        cursor: Option<Key>,
    ) -> Result<Vec<(Key, Val)>> {
        let mode = match cursor {
            Some(c) => ScanMode::After(c),
            None => ScanMode::Offset(0),
        };
        let rows = self
            .range_query("key, val", rng, limit, mode, "ASC")
            .await?;
        if let Some(last) = rows.last() {
            self.last_scan_key_asc = Some(last.get::<Vec<u8>, _>("key"));
        }
        Ok(Self::rows_to_pairs(rows))
    }

    /// Get the last key returned by the most recent ascending scan.
    #[must_use]
    pub fn last_scan_key(&self) -> Option<&Key> {
        self.last_scan_key_asc.as_ref()
    }

    /// Count keys in a range.
    pub async fn count(&mut self, rng: Range<Key>) -> Result<u64> {
        let sql = self.sql.clone();
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &sql.count)
            .bind(rng.start.as_slice())
            .bind(rng.end.as_slice())
            .fetch_one(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        Ok(row.get::<i64, _>("cnt") as u64)
    }

    /// Approximate row count using `pg_class.reltuples`.
    ///
    /// Returns an O(1) estimate based on the most recent `ANALYZE` statistics.
    /// This is a **whole-table** estimate (range parameters are ignored) and
    /// may be stale if `ANALYZE` hasn't run recently. Returns `None` if the
    /// table has no statistics.
    pub async fn count_approx(&mut self) -> Result<Option<u64>> {
        let table = &self.sql.table_name;
        let sql = format!(
            "SELECT reltuples::bigint AS approx_cnt FROM pg_class \
             WHERE relname = '{}' AND reltuples > 0",
            table
        );
        let persistent = self.persistent;
        let row = Self::build_query(persistent, &sql)
            .fetch_optional(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        Ok(row.map(|r| r.get::<i64, _>("approx_cnt") as u64))
    }

    // ─── Savepoints (PG native) ──────────────────────────

    /// Create a new savepoint within the current transaction.
    pub async fn new_save_point(&mut self) -> Result<()> {
        let name = self.push_savepoint_name();
        self.execute_simple(&format!("SAVEPOINT {name}"), None)
            .await?;
        debug!(savepoint = %name, "savepoint created");
        Ok(())
    }

    /// Release the last savepoint.
    pub async fn release_last_save_point(&mut self) -> Result<()> {
        let Some(name) = self.pop_savepoint_name() else {
            return Ok(());
        };
        self.execute_simple(&format!("RELEASE SAVEPOINT {name}"), None)
            .await?;
        debug!(savepoint = %name, "savepoint released");
        Ok(())
    }

    /// Rollback to the last savepoint.
    pub async fn rollback_to_save_point(&mut self) -> Result<()> {
        let Some(name) = self.pop_savepoint_name() else {
            return Ok(());
        };
        self.execute_simple(&format!("ROLLBACK TO SAVEPOINT {name}"), None)
            .await?;
        self.execute_simple(&format!("RELEASE SAVEPOINT {name}"), None)
            .await?;
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
            tracing::warn!(
                "PgTransaction dropped without explicit commit/cancel; PG will auto-rollback"
            );
        }
    }
}
