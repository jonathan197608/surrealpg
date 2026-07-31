//! PgTransaction — implements Transactable over a PostgreSQL transaction.
//!
//! This is the core of the backend: every SurrealDB KV operation maps to one
//! or more SQL statements executed within a single PG transaction.

use std::ops::{DerefMut, Range};

use sqlx::{Executor, Row};
use tracing::{debug, trace, warn};

use crate::config::PgIsolation;
use crate::error::{PgStoreError, Result};

/// Type aliases matching SurrealDB conventions
pub type Key = Vec<u8>;
pub type Val = Vec<u8>;

// ─── PgTransaction ──────────────────────────────────────

/// A transaction backed by a single PostgreSQL connection.
///
/// Implements all KV operations that SurrealDB's `Transactable` trait requires.
/// After `commit()` or `cancel()`, the transaction is closed and the connection
/// is returned to the pool.
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
    /// Table name (e.g. `kv` or `kv_test`)
    table: String,
}

impl PgTransaction {
    /// Create a new transaction wrapping an acquired PG connection.
    ///
    /// The caller must have already executed `BEGIN` on the connection.
    pub(crate) fn new(
        conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
        writeable: bool,
        isolation: PgIsolation,
        persistent: bool,
        table: String,
    ) -> Self {
        Self {
            conn: Some(conn),
            writeable,
            closed: false,
            savepoint_counter: 0,
            savepoints: Vec::new(),
            isolation,
            persistent,
            table,
        }
    }

    // ─── Internal helpers ────────────────────────────────

    /// Get a mutable reference to the inner `PgConnection`.
    ///
    /// In sqlx 0.8, `PoolConnection<Postgres>` does not implement `Executor`;
    /// only `&mut PgConnection` does. `PoolConnection` implements `DerefMut`
    /// to its inner `PgConnection`, so we double-deref to get there.
    ///
    /// Also validates that the transaction is not closed.
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
    ///
    /// Uses `raw_sql` instead of `query` to avoid the extended protocol
    /// (Parse/Bind/Execute/Close/Sync) overhead for control statements
    /// like `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`.
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
        &self,
        sql: &'a str,
    ) -> sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments> {
        sqlx::query(sql).persistent(self.persistent)
    }

    /// Execute a single-key query and return at most one row.
    ///
    /// Used by read operations (`exists`, `get`) that share the pattern:
    /// `build_query + bind(key) + fetch_optional + map_err`.
    async fn fetch_optional_by_key(
        &mut self,
        sql: &str,
        key: &[u8],
    ) -> Result<Option<sqlx::postgres::PgRow>> {
        self.build_query(sql)
            .bind(key)
            .fetch_optional(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(key), &e))
    }

    /// Execute a two-argument write query (key + one value) and return the
    /// `PgQueryResult` (which carries `rows_affected`).
    ///
    /// Used by write operations (`set`, `put`, `delc`) that share the pattern:
    /// `build_query + bind(key) + bind(val) + execute + map_err`.
    async fn execute_keyed_2arg(
        &mut self,
        sql: &str,
        key: &[u8],
        val: &[u8],
    ) -> Result<sqlx::postgres::PgQueryResult> {
        self.build_query(sql)
            .bind(key)
            .bind(val)
            .execute(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(Some(key), &e))
    }

    /// Pop the last savepoint name, returning `None` if the stack is empty.
    fn pop_savepoint_name(&mut self) -> Option<String> {
        self.savepoints.pop()
    }

    /// Build the next unique savepoint name and push it onto the stack.
    fn push_savepoint_name(&mut self) -> String {
        self.savepoint_counter += 1;
        let name = format!("sp_{}", self.savepoint_counter);
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
        let sql = format!("SELECT 1 AS exists_flag FROM {} WHERE key = $1", self.table);
        let row = self.fetch_optional_by_key(&sql, &key).await?;
        Ok(row.is_some())
    }

    /// Get the value for a key.
    pub async fn get(&mut self, key: Key) -> Result<Option<Val>> {
        let sql = format!("SELECT val FROM {} WHERE key = $1", self.table);
        let row = self.fetch_optional_by_key(&sql, &key).await?;
        Ok(row.map(|r| r.get::<Vec<u8>, _>("val")))
    }

    /// Batch-get multiple keys.
    pub async fn getm(&mut self, keys: Vec<Key>) -> Result<Vec<Option<Val>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let keys_ref: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        let sql = format!("SELECT key, val FROM {} WHERE key = ANY($1)", self.table);
        let rows = self
            .build_query(&sql)
            .bind(&keys_ref)
            .fetch_all(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        let mut map = std::collections::HashMap::new();
        for row in rows {
            let k: Vec<u8> = row.get::<Vec<u8>, _>("key");
            let v: Vec<u8> = row.get::<Vec<u8>, _>("val");
            map.insert(k, v);
        }

        Ok(keys.into_iter().map(|k| map.get(&k).cloned()).collect())
    }

    // ─── Write operations ────────────────────────────────

    /// Set a key to a value (insert or update).
    pub async fn set(&mut self, key: Key, val: Val) -> Result<()> {
        self.check_writable()?;

        let sql = format!(
            "INSERT INTO {} (key, val) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET val = $2",
            self.table,
        );
        self.execute_keyed_2arg(&sql, &key, &val).await?;

        trace!(key_len = key.len(), "set");
        Ok(())
    }

    /// Set a key only if it does not already exist (insert-if-absent).
    /// Returns `KeyAlreadyExists` if the key exists.
    pub async fn put(&mut self, key: Key, val: Val) -> Result<()> {
        self.check_writable()?;

        let sql = format!(
            "INSERT INTO {} (key, val) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
            self.table,
        );
        let result = self.execute_keyed_2arg(&sql, &key, &val).await?;

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

        let sql = format!(
            "UPDATE {} SET val = $2 WHERE key = $1 AND val = $3",
            self.table
        );
        let affected = self
            .build_query(&sql)
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

        let sql = format!("DELETE FROM {} WHERE key = $1", self.table);
        self.build_query(&sql)
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

        let sql = format!("DELETE FROM {} WHERE key = $1 AND val = $2", self.table);
        let result = self.execute_keyed_2arg(&sql, &key, &expected).await?;

        if result.rows_affected() == 0 {
            Err(PgStoreError::ConditionNotMet(key))
        } else {
            Ok(())
        }
    }

    /// Delete all keys in a range (inclusive start, exclusive end).
    pub async fn delr(&mut self, rng: Range<Key>) -> Result<()> {
        self.check_writable()?;

        let sql = format!("DELETE FROM {} WHERE key >= $1 AND key < $2", self.table);
        let deleted = self
            .build_query(&sql)
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
    async fn range_query(
        &mut self,
        select: &str,
        rng: Range<Key>,
        limit: u32,
        skip: u32,
        direction: &str,
    ) -> Result<Vec<sqlx::postgres::PgRow>> {
        if skip > 1000 {
            warn!(
                skip,
                limit,
                "large OFFSET in range scan — consider cursor-based pagination"
            );
        }
        let sql = format!(
            "SELECT {select} FROM {} WHERE key >= $1 AND key < $2 \
             ORDER BY key {direction} LIMIT $3 OFFSET $4",
            self.table,
        );
        self.build_query(&sql)
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
        let rows = self.range_query("key", rng, limit, skip, "ASC").await?;
        Ok(Self::rows_to_keys(rows))
    }

    /// Scan keys in a range (descending).
    pub async fn keysr(&mut self, rng: Range<Key>, limit: u32, skip: u32) -> Result<Vec<Key>> {
        let rows = self.range_query("key", rng, limit, skip, "DESC").await?;
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
            .range_query("key, val", rng, limit, skip, "ASC")
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
            .range_query("key, val", rng, limit, skip, "DESC")
            .await?;
        Ok(Self::rows_to_pairs(rows))
    }

    /// Count keys in a range.
    pub async fn count(&mut self, rng: Range<Key>) -> Result<u64> {
        let sql = format!(
            "SELECT count(*) AS cnt FROM {} WHERE key >= $1 AND key < $2",
            self.table
        );
        let row = self
            .build_query(&sql)
            .bind(rng.start.as_slice())
            .bind(rng.end.as_slice())
            .fetch_one(self.conn_mut()?)
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        Ok(row.get::<i64, _>("cnt") as u64)
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
