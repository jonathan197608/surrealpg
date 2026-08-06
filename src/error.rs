//! PostgreSQL storage backend error types

use std::borrow::Cow;

use thiserror::Error;

/// All errors produced by the PostgreSQL storage backend
#[derive(Error, Debug)]
pub enum PgStoreError {
    /// A unique constraint was violated (key already exists) — used by `put`.
    /// B4: Uses `Box<[u8]>` instead of `Vec<u8>` to avoid storing unused
    /// capacity and reduce allocation overhead on the write-conflict path.
    #[error("key already exists: {0:?}")]
    KeyAlreadyExists(Box<[u8]>),

    /// A compare-and-swap condition was not met — used by `putc`/`delc`.
    /// B4: Same `Box<[u8]>` optimization as `KeyAlreadyExists`.
    #[error("condition not met for key: {0:?}")]
    ConditionNotMet(Box<[u8]>),

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

    /// Direct-mode TCP connect timeout
    #[error("connection timeout: failed to connect within {0:?}")]
    ConnectTimeout(std::time::Duration),

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
    /// Whether this error is likely transient and worth retrying.
    ///
    /// - `PoolTimeout`: pool exhausted, may recover when connections are returned.
    /// - `Postgres` errors with `connection error [08`: the pooler/PG server
    ///   may have dropped the connection; retrying with a fresh connection
    ///   typically succeeds.
    /// - `Deadlock` / `SerializationFailure`: PG may resolve on retry.
    /// - All other errors are considered non-transient.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::PoolTimeout => true,
            Self::Postgres(msg) => msg.starts_with("connection error [08"),
            Self::Deadlock(_) | Self::SerializationFailure(_) => true,
            _ => false,
        }
    }

    /// Map a sqlx::Error into our error type, preserving semantics for
    /// well-known PostgreSQL SQLSTATE codes.
    #[must_use]
    pub fn from_sqlx(key: Option<&[u8]>, e: &sqlx::Error) -> Self {
        match e {
            sqlx::Error::Database(db_err) => {
                // When the SQLSTATE code is missing, use "58000" (system error)
                // instead of "00000" (successful completion) — we are in an
                // error path, so "00000" would be semantically misleading.
                let code = db_err.code().unwrap_or(Cow::Borrowed("58000"));
                let msg = db_err.message().to_string();

                match code.as_ref() {
                    // unique_violation
                    "23505" => key
                        .map(|k| Self::KeyAlreadyExists(k.to_vec().into_boxed_slice()))
                        .unwrap_or_else(|| Self::Other(format!("unique violation: {msg}"))),

                    // connection exception (08xxx)
                    c if c.starts_with("08") => {
                        Self::Postgres(format!("connection error [{c}]: {msg}"))
                    }

                    // deadlock
                    "40P01" => Self::Deadlock(msg),

                    // serialization failure
                    "40001" => Self::SerializationFailure(msg),

                    // no active transaction — F4: preserve SQLSTATE context
                    // instead of mapping to TxClosed, which may be misleading
                    // (the transaction might never have started, not just closed).
                    "25P01" => Self::Postgres(format!("[25P01]: {msg}")),

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
            PgStoreError::ConnectTimeout(d) => Self::ConnectionFailed(format!(
                "connection timeout: failed to connect within {d:?}"
            )),
            PgStoreError::PoolClosed => {
                Self::ConnectionFailed("connection pool closed".to_string())
            }
            PgStoreError::Postgres(msg) => Self::Transaction(msg),
            PgStoreError::Other(msg) => Self::Transaction(msg),
        }
    }
}
