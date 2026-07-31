//! PostgreSQL storage backend error types

use std::borrow::Cow;

use thiserror::Error;

/// All errors produced by the PostgreSQL storage backend
#[derive(Error, Debug)]
pub enum PgStoreError {
    /// A unique constraint was violated (key already exists) — used by `put`.
    #[error("key already exists: {0:?}")]
    KeyAlreadyExists(Vec<u8>),

    /// A compare-and-swap condition was not met — used by `putc`/`delc`.
    #[error("condition not met for key: {0:?}")]
    ConditionNotMet(Vec<u8>),

    /// The transaction has been closed (committed or rolled back)
    #[error("transaction already closed")]
    TxClosed,

    /// Attempted a write operation on a read-only transaction
    #[error("transaction is read-only")]
    TxReadOnly,

    /// Versioned queries are not supported by the PostgreSQL backend
    #[error("versioned queries are not supported by the PostgreSQL backend")]
    UnsupportedVersionedQueries,

    /// A deadlock was detected; the caller may retry
    #[error("deadlock detected: {0}")]
    Deadlock(String),

    /// A serialization failure occurred; the caller may retry
    #[error("serialization failure: {0}")]
    SerializationFailure(String),

    /// Connection pool exhausted
    #[error("connection pool timeout")]
    PoolTimeout,

    /// Connection pool closed
    #[error("connection pool closed")]
    PoolClosed,

    /// A PostgreSQL / SQLx error that doesn't map to a specific variant
    #[error("postgres error: {0}")]
    Postgres(String),

    /// General store error
    #[error("{0}")]
    Other(String),
}

impl PgStoreError {
    /// Map a sqlx::Error into our error type, preserving semantics for
    /// well-known PostgreSQL SQLSTATE codes.
    #[must_use]
    pub fn from_sqlx(key: Option<&[u8]>, e: &sqlx::Error) -> Self {
        match e {
            sqlx::Error::Database(db_err) => {
                let code = db_err.code().unwrap_or(Cow::Borrowed("00000"));
                let msg = db_err.message().to_string();

                match code.as_ref() {
                    // unique_violation
                    "23505" => key
                        .map(|k| Self::KeyAlreadyExists(k.to_vec()))
                        .unwrap_or_else(|| Self::Other(format!("unique violation: {msg}"))),

                    // connection exception (08xxx)
                    c if c.starts_with("08") => {
                        Self::Postgres(format!("connection error [{c}]: {msg}"))
                    }

                    // deadlock
                    "40P01" => Self::Deadlock(msg),

                    // serialization failure
                    "40001" => Self::SerializationFailure(msg),

                    // no active transaction
                    "25P01" => Self::TxClosed,

                    c => Self::Postgres(format!("[{c}]: {msg}")),
                }
            }
            sqlx::Error::PoolTimedOut => Self::PoolTimeout,
            sqlx::Error::PoolClosed => Self::PoolClosed,
            _ => Self::Postgres(format!("{e}")),
        }
    }
}

/// Convenience Result alias
pub type Result<T> = std::result::Result<T, PgStoreError>;

// ─── Conversion to surrealdb_core::kvs::Error ────────────

/// Convert our error into the `surrealdb_core` KVS error type.
///
/// This mapping is used by the `Transactable` and `TransactionBuilder`
/// implementations so the SurrealDB engine sees semantically correct
/// errors (e.g. `TransactionFinished`, `TransactionConflict`, …).
impl From<PgStoreError> for surrealdb_core::kvs::Error {
    fn from(e: PgStoreError) -> Self {
        match e {
            PgStoreError::KeyAlreadyExists(_) => Self::TransactionKeyAlreadyExists,
            PgStoreError::ConditionNotMet(_) => Self::TransactionConditionNotMet,
            PgStoreError::TxClosed => Self::TransactionFinished,
            PgStoreError::TxReadOnly => Self::TransactionReadonly,
            PgStoreError::UnsupportedVersionedQueries => Self::UnsupportedVersionedQueries,
            PgStoreError::Deadlock(msg) => Self::TransactionConflict(msg),
            PgStoreError::SerializationFailure(msg) => Self::TransactionConflict(msg),
            PgStoreError::PoolTimeout => {
                Self::ConnectionFailed("connection pool timeout".to_string())
            }
            PgStoreError::PoolClosed => {
                Self::ConnectionFailed("connection pool closed".to_string())
            }
            PgStoreError::Postgres(msg) => Self::Transaction(msg),
            PgStoreError::Other(msg) => Self::Transaction(msg),
        }
    }
}
