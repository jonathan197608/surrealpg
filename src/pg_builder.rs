//! TransactionBuilder implementation for PgStore, connecting the PG backend
//! to SurrealDB's kvs engine.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use surrealdb_core::kvs::{
    self, Metrics, Transactable, TransactionBuilder, TransactionBuilderRequirements,
};

use crate::pg_tx::PgTx;
use crate::store::PgStore;

/// Boxed future type matching surrealdb-core's internal `BoxFut`.
///
/// The `BoxFut` type in `surrealdb_core::kvs::api` is `pub(crate)`, so we
/// define a compatible alias here. On non-WASM it requires `Send`.
#[cfg(not(target_family = "wasm"))]
pub(crate) type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(target_family = "wasm")]
pub(crate) type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

// ─── PgStore: TransactionBuilder impl ───────────────────

impl fmt::Display for PgStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL store (table: {})", self.config().table_name)
    }
}

// `TransactionBuilderRequirements` = `Display + Send + Sync + 'static`.
// `PgStore` is `Send + Sync + 'static` and we impl'd `Display` above.
impl TransactionBuilderRequirements for PgStore {}

impl TransactionBuilder for PgStore {
    fn new_transaction(
        &self,
        write: bool,
        _lock: bool,
    ) -> BoxFut<'_, anyhow::Result<(Box<dyn Transactable>, bool)>> {
        Box::pin(async move {
            let tx = self.begin(write).await.map_err(kvs::Error::from)?;
            let pg_tx = PgTx::new(tx);
            // `true` = local transaction (same process), matching mem/rocksdb
            Ok((Box::new(pg_tx) as Box<dyn Transactable>, true))
        })
    }

    fn shutdown(&self) -> BoxFut<'_, anyhow::Result<()>> {
        Box::pin(async move {
            PgStore::shutdown(self).await;
            Ok(())
        })
    }

    fn register_metrics(&self) -> Option<Metrics> {
        None
    }

    fn collect_u64_metric(&self, _metric: &str) -> Option<u64> {
        None
    }
}
