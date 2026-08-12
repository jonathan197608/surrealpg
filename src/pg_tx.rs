//! PgTx — a `Transactable` wrapper around `PgTransaction`.
//!
//! Uses `Mutex` for interior mutability (the trait requires `&self`) and
//! `AtomicBool` for the `closed` flag (lock-free check, matching the mem
//! backend pattern).

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

use surrealdb_core::kvs::{self, GetMultiResult, Key, KeysResult, ScanResult, Transactable, Val};
use tokio::sync::Mutex;
use tracing::debug;

use crate::pg_builder::BoxFut;
use crate::transaction::PgTransaction;

/// A `Transactable` wrapper around a `PgTransaction`.
///
/// Uses `Mutex` for interior mutability (the trait requires `&self`) and
/// `AtomicBool` for the `closed` flag (lock-free check, matching the mem
/// backend pattern).
pub struct PgTx {
    /// The inner PG transaction, guarded by a mutex.
    /// `None` after commit/cancel (connection returned to pool).
    inner: Mutex<Option<PgTransaction>>,
    /// Whether the transaction has been committed or cancelled.
    done: AtomicBool,
    /// Whether this is a write transaction.
    write: bool,
    /// F8: Shared commit counter (from PgStore).
    tx_committed: Arc<AtomicU64>,
    /// F8: Shared rollback counter (from PgStore).
    tx_rolled_back: Arc<AtomicU64>,
}

impl PgTx {
    /// Create a new `PgTx` wrapping a `PgTransaction`.
    ///
    /// F8: Accepts shared metric counters from PgStore for tracking
    /// commit/rollback events.
    #[must_use]
    pub fn new(
        tx: PgTransaction,
        tx_committed: Arc<AtomicU64>,
        tx_rolled_back: Arc<AtomicU64>,
    ) -> Self {
        let write = tx.is_writeable();
        Self {
            inner: Mutex::new(Some(tx)),
            done: AtomicBool::new(false),
            write,
            tx_committed,
            tx_rolled_back,
        }
    }

    /// Check if versioned queries are requested and reject them (PG backend
    /// does not support MVCC time-travel, same as the TiKV backend).
    fn check_version(version: Option<u64>) -> kvs::Result<()> {
        if version.is_some() {
            return Err(kvs::Error::UnsupportedVersionedQueries);
        }
        Ok(())
    }

    /// Acquire the inner transaction, returning an error if already closed.
    async fn lock(&self) -> kvs::Result<tokio::sync::MutexGuard<'_, Option<PgTransaction>>> {
        if self.done.load(AtomicOrdering::Relaxed) {
            return Err(kvs::Error::TransactionFinished);
        }
        let guard = self.inner.lock().await;
        if guard.is_none() {
            return Err(kvs::Error::TransactionFinished);
        }
        Ok(guard)
    }

    /// Like `lock()`, but also checks that the transaction is writable.
    /// Used by all write methods to avoid repeating the closed/writable guards.
    async fn lock_write(&self) -> kvs::Result<tokio::sync::MutexGuard<'_, Option<PgTransaction>>> {
        if self.done.load(AtomicOrdering::Relaxed) {
            return Err(kvs::Error::TransactionFinished);
        }
        if !self.write {
            return Err(kvs::Error::TransactionReadonly);
        }
        let guard = self.inner.lock().await;
        if guard.is_none() {
            return Err(kvs::Error::TransactionFinished);
        }
        Ok(guard)
    }
}

// R2-L3: Manual Debug impl — PgTx contains Mutex and AtomicBool which
// don't implement Debug. We provide a useful summary instead.
impl fmt::Debug for PgTx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgTx")
            .field("done", &self.done.load(AtomicOrdering::Relaxed))
            .field("write", &self.write)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PgTx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL transaction")
    }
}

impl Drop for PgTx {
    fn drop(&mut self) {
        // SurrealDB's engine routinely drops transactions without calling
        // commit()/cancel() — these are "ghost" transactions from a metrics
        // perspective. Record them as rollbacks so that
        // tx_started == tx_committed + tx_rolled_back holds.
        if !self.done.load(AtomicOrdering::Relaxed) {
            self.tx_rolled_back.fetch_add(1, AtomicOrdering::Relaxed);
            debug!("PgTx dropped without explicit commit/cancel — counted as rollback");
        }
    }
}

impl Transactable for PgTx {
    fn kind(&self) -> &'static str {
        "postgres"
    }

    // R58-M1: Use Acquire ordering to pair with the AcqRel swap in commit()/cancel().
    // On ARM, a Relaxed load may return a stale `false` even after swap(true, AcqRel)
    // has completed. While subsequent operations are protected by the Mutex (which
    // provides its own Acquire semantics), `closed()` is a public diagnostic method
    // that SurrealDB may rely on — it should return accurate results immediately.
    fn closed(&self) -> bool {
        self.done.load(AtomicOrdering::Acquire)
    }

    fn writeable(&self) -> bool {
        self.write
    }

    fn cancel(&self) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            // Fast check before acquiring the lock — if already done,
            // return early without blocking.
            if self.done.load(AtomicOrdering::Relaxed) {
                return Err(kvs::Error::TransactionFinished);
            }
            let mut guard = self.inner.lock().await;
            // Double-check under lock: another concurrent call may have
            // already set done. Use swap to atomically claim the close.
            if self.done.swap(true, AtomicOrdering::AcqRel) {
                return Err(kvs::Error::TransactionFinished);
            }
            // Take the transaction out while holding the lock, then release
            // the lock before performing the network I/O (ROLLBACK). This
            // minimizes lock hold time — other operations see done=true and
            // return TransactionFinished immediately.
            let tx_opt = guard.take();
            drop(guard);
            if let Some(mut tx) = tx_opt {
                match tx.cancel().await {
                    Ok(()) => {
                        self.tx_rolled_back.fetch_add(1, AtomicOrdering::Relaxed);
                        debug!("PostgreSQL transaction cancelled");
                    }
                    Err(e) => {
                        self.tx_rolled_back.fetch_add(1, AtomicOrdering::Relaxed);
                        return Err(kvs::Error::from(e));
                    }
                }
            }
            Ok(())
        })
    }

    fn commit(&self) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            // Fast check before acquiring the lock — if already done,
            // return early without blocking.
            if self.done.load(AtomicOrdering::Relaxed) {
                return Err(kvs::Error::TransactionFinished);
            }
            let mut guard = self.inner.lock().await;
            // Double-check under lock: another concurrent call may have
            // already set done. Use swap to atomically claim the close.
            // B1: swap must happen AFTER acquiring the lock (matching cancel()),
            // so that closed() returning true always means the connection is
            // released — no window where done=true but the lock is still held.
            if self.done.swap(true, AtomicOrdering::AcqRel) {
                return Err(kvs::Error::TransactionFinished);
            }
            // Take the transaction out while holding the lock, then release
            // the lock before performing the network I/O (COMMIT). This
            // minimizes lock hold time — other operations see done=true and
            // return TransactionFinished immediately.
            //
            // Note: done is already set to true via swap above, so closed()
            // returns true even though COMMIT hasn't been sent yet. This is
            // correct — once commit() is called, no further operations should
            // be accepted on this transaction.
            let tx_opt = guard.take();
            drop(guard);
            // PG natively supports COMMIT on read-only transactions.
            if let Some(mut tx) = tx_opt {
                match tx.commit().await {
                    Ok(()) => {
                        self.tx_committed.fetch_add(1, AtomicOrdering::Relaxed);
                        debug!("PostgreSQL transaction committed");
                    }
                    Err(e) => {
                        // PG auto-rollbacks on COMMIT failure.
                        self.tx_rolled_back.fetch_add(1, AtomicOrdering::Relaxed);
                        return Err(kvs::Error::from(e));
                    }
                }
            }
            Ok(())
        })
    }

    fn exists(&self, key: Key, version: Option<u64>) -> BoxFut<'_, kvs::Result<bool>> {
        Box::pin(async move {
            Self::check_version(version)?;
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.exists(key).await.map_err(kvs::Error::from)
        })
    }

    fn get(&self, key: Key, version: Option<u64>) -> BoxFut<'_, kvs::Result<Option<Val>>> {
        Box::pin(async move {
            Self::check_version(version)?;
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.get(key).await.map_err(kvs::Error::from)
        })
    }

    fn set(&self, key: Key, val: Val) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            let mut guard = self.lock_write().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.set(key, val).await.map_err(kvs::Error::from)
        })
    }

    fn put(&self, key: Key, val: Val) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            let mut guard = self.lock_write().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.put(key, val).await.map_err(kvs::Error::from)
        })
    }

    fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            let mut guard = self.lock_write().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.putc(key, val, chk).await.map_err(kvs::Error::from)
        })
    }

    fn del(&self, key: Key) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            let mut guard = self.lock_write().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.del(key).await.map_err(kvs::Error::from)
        })
    }

    fn delc(&self, key: Key, chk: Option<Val>) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            let mut guard = self.lock_write().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.delc(key, chk).await.map_err(kvs::Error::from)
        })
    }

    fn keys(
        &self,
        rng: std::ops::Range<Key>,
        limit: u32,
        skip: u32,
        version: Option<u64>,
    ) -> BoxFut<'_, kvs::Result<KeysResult>> {
        Box::pin(async move {
            Self::check_version(version)?;
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            let keys = tx.keys(rng, limit, skip).await.map_err(kvs::Error::from)?;
            let key_bytes = keys.iter().map(|k| k.len() as u64).sum();
            Ok(KeysResult { keys, key_bytes })
        })
    }

    fn keysr(
        &self,
        rng: std::ops::Range<Key>,
        limit: u32,
        skip: u32,
        version: Option<u64>,
    ) -> BoxFut<'_, kvs::Result<KeysResult>> {
        Box::pin(async move {
            Self::check_version(version)?;
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            let keys = tx.keysr(rng, limit, skip).await.map_err(kvs::Error::from)?;
            let key_bytes = keys.iter().map(|k| k.len() as u64).sum();
            Ok(KeysResult { keys, key_bytes })
        })
    }

    fn scan(
        &self,
        rng: std::ops::Range<Key>,
        limit: u32,
        skip: u32,
        version: Option<u64>,
    ) -> BoxFut<'_, kvs::Result<ScanResult>> {
        Box::pin(async move {
            Self::check_version(version)?;
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            let values = tx.scan(rng, limit, skip).await.map_err(kvs::Error::from)?;
            let key_bytes = values.iter().map(|(k, _)| k.len() as u64).sum();
            let value_bytes = values.iter().map(|(_, v)| v.len() as u64).sum();
            Ok(ScanResult {
                values,
                key_bytes,
                value_bytes,
            })
        })
    }

    fn scanr(
        &self,
        rng: std::ops::Range<Key>,
        limit: u32,
        skip: u32,
        version: Option<u64>,
    ) -> BoxFut<'_, kvs::Result<ScanResult>> {
        Box::pin(async move {
            Self::check_version(version)?;
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            let values = tx.scanr(rng, limit, skip).await.map_err(kvs::Error::from)?;
            let key_bytes = values.iter().map(|(k, _)| k.len() as u64).sum();
            let value_bytes = values.iter().map(|(_, v)| v.len() as u64).sum();
            Ok(ScanResult {
                values,
                key_bytes,
                value_bytes,
            })
        })
    }

    // R51-M1: Override getm() with PG-native batch implementation (WHERE key = ANY($1))
    // instead of the trait default which calls get() per key (N round-trips).
    fn getm(
        &self,
        keys: Vec<Key>,
        version: Option<u64>,
    ) -> BoxFut<'_, kvs::Result<GetMultiResult>> {
        Box::pin(async move {
            Self::check_version(version)?;
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            let values = tx.getm(keys).await.map_err(kvs::Error::from)?;
            let records = values.iter().filter(|v| v.is_some()).count() as u64;
            let value_bytes = values
                .iter()
                .map(|v| v.as_ref().map_or(0, |v| v.len() as u64))
                .sum();
            Ok(GetMultiResult {
                values,
                records,
                value_bytes,
            })
        })
    }

    // R52-M1: Override getr() with PG-native single-query range scan (no
    // LIMIT/OFFSET) instead of the trait default which loops batch_keys_vals()
    // accumulating all results (same memory, more round-trips).
    fn getr(
        &self,
        rng: std::ops::Range<Key>,
        version: Option<u64>,
    ) -> BoxFut<'_, kvs::Result<ScanResult>> {
        Box::pin(async move {
            Self::check_version(version)?;
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            let values = tx.getr(rng).await.map_err(kvs::Error::from)?;
            let key_bytes = values.iter().map(|(k, _)| k.len() as u64).sum();
            let value_bytes = values.iter().map(|(_, v)| v.len() as u64).sum();
            Ok(ScanResult {
                values,
                key_bytes,
                value_bytes,
            })
        })
    }

    // R51-M2: Override delr() with PG-native range DELETE (WHERE key >= $1 AND key < $2)
    // instead of the trait default which scans keys then deletes one-by-one (N+1 round-trips).
    fn delr(&self, rng: std::ops::Range<Key>) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            let mut guard = self.lock_write().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.delr(rng).await.map_err(kvs::Error::from)
        })
    }

    // R52-M2: Override clrr() — for PG (no versioning), clr(key) = del(key),
    // so clrr(range) is semantically identical to delr(range) which is already
    // a single DELETE WHERE key >= $1 AND key < $2. The trait default does
    // batch_keys() + clr() per key (N+1 round-trips).
    fn clrr(&self, rng: std::ops::Range<Key>) -> BoxFut<'_, kvs::Result<()>> {
        // Delegate directly to delr — same SQL, same semantics.
        self.delr(rng)
    }

    // R51-M3: Override count() with PG-native SELECT count(*) instead of
    // the trait default which scans all keys into memory via batch_keys().
    fn count(
        &self,
        rng: std::ops::Range<Key>,
        version: Option<u64>,
    ) -> BoxFut<'_, kvs::Result<usize>> {
        Box::pin(async move {
            Self::check_version(version)?;
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            let cnt = tx.count(rng).await.map_err(kvs::Error::from)?;
            Ok(cnt as usize)
        })
    }

    fn new_save_point(&self) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.new_save_point().await.map_err(kvs::Error::from)?;
            Ok(())
        })
    }

    fn release_last_save_point(&self) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.release_last_save_point()
                .await
                .map_err(kvs::Error::from)?;
            Ok(())
        })
    }

    fn rollback_to_save_point(&self) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.rollback_to_save_point()
                .await
                .map_err(kvs::Error::from)?;
            Ok(())
        })
    }
}
