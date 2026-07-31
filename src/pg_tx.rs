//! PgTx — a `Transactable` wrapper around `PgTransaction`.
//!
//! Uses `Mutex` for interior mutability (the trait requires `&self`) and
//! `AtomicBool` for the `closed` flag (lock-free check, matching the mem
//! backend pattern).

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

use surrealdb_core::kvs::{self, Key, KeysResult, ScanResult, Transactable, Val};
use tokio::sync::Mutex;
use tracing::info;

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
}

impl PgTx {
    /// Create a new `PgTx` wrapping a `PgTransaction`.
    #[must_use]
    pub fn new(tx: PgTransaction) -> Self {
        let write = tx.is_writeable();
        Self {
            inner: Mutex::new(Some(tx)),
            done: AtomicBool::new(false),
            write,
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
        if self.done.load(Ordering::Relaxed) {
            return Err(kvs::Error::TransactionFinished);
        }
        let guard = self.inner.lock().await;
        if guard.is_none() {
            return Err(kvs::Error::TransactionFinished);
        }
        Ok(guard)
    }
}

impl fmt::Display for PgTx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL transaction")
    }
}

impl Transactable for PgTx {
    fn kind(&self) -> &'static str {
        "postgres"
    }

    fn closed(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }

    fn writeable(&self) -> bool {
        self.write
    }

    fn cancel(&self) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            if self.done.swap(true, Ordering::AcqRel) {
                return Err(kvs::Error::TransactionFinished);
            }
            let mut guard = self.inner.lock().await;
            if let Some(tx) = guard.as_mut() {
                tx.cancel().await.map_err(kvs::Error::from)?;
            }
            *guard = None; // Drop the transaction, returning the connection to the pool
            info!("PostgreSQL transaction cancelled");
            Ok(())
        })
    }

    fn commit(&self) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            if self.done.swap(true, Ordering::AcqRel) {
                return Err(kvs::Error::TransactionFinished);
            }
            // PG natively supports COMMIT on read-only transactions.
            let mut guard = self.inner.lock().await;
            if let Some(tx) = guard.as_mut() {
                tx.commit().await.map_err(kvs::Error::from)?;
            }
            *guard = None;
            info!("PostgreSQL transaction committed");
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
            if self.closed() {
                return Err(kvs::Error::TransactionFinished);
            }
            if !self.writeable() {
                return Err(kvs::Error::TransactionReadonly);
            }
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.set(key, val).await.map_err(kvs::Error::from)
        })
    }

    fn put(&self, key: Key, val: Val) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            if self.closed() {
                return Err(kvs::Error::TransactionFinished);
            }
            if !self.writeable() {
                return Err(kvs::Error::TransactionReadonly);
            }
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.put(key, val).await.map_err(kvs::Error::from)
        })
    }

    fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            if self.closed() {
                return Err(kvs::Error::TransactionFinished);
            }
            if !self.writeable() {
                return Err(kvs::Error::TransactionReadonly);
            }
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.putc(key, val, chk).await.map_err(kvs::Error::from)
        })
    }

    fn del(&self, key: Key) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            if self.closed() {
                return Err(kvs::Error::TransactionFinished);
            }
            if !self.writeable() {
                return Err(kvs::Error::TransactionReadonly);
            }
            let mut guard = self.lock().await?;
            let tx = guard.as_mut().ok_or(kvs::Error::TransactionFinished)?;
            tx.del(key).await.map_err(kvs::Error::from)
        })
    }

    fn delc(&self, key: Key, chk: Option<Val>) -> BoxFut<'_, kvs::Result<()>> {
        Box::pin(async move {
            if self.closed() {
                return Err(kvs::Error::TransactionFinished);
            }
            if !self.writeable() {
                return Err(kvs::Error::TransactionReadonly);
            }
            let mut guard = self.lock().await?;
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
