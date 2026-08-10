//! TransactionBuilder implementation for PgStore, connecting the PG backend
//! to SurrealDB's kvs engine.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use surrealdb_core::kvs::{
    self, Metric, Metrics, Transactable, TransactionBuilder, TransactionBuilderRequirements,
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
            let (tx_committed, tx_rolled_back) = self.tx_commit_rollback_arcs();
            let pg_tx = PgTx::new(tx, tx_committed, tx_rolled_back);
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
        Some(Metrics {
            name: "surrealdb.postgresql",
            u64_metrics: vec![
                Metric {
                    name: "pg_pool_size",
                    description: "Current number of connections in the pool (0 in direct mode)",
                },
                Metric {
                    name: "pg_pool_idle",
                    description: "Number of idle connections in the pool (0 in direct mode)",
                },
                Metric {
                    name: "pg_pool_max",
                    description: "Maximum number of connections allowed in the pool (0 in direct mode)",
                },
                // F8: Transaction lifecycle metrics.
                Metric {
                    name: "pg_tx_started",
                    description: "Total number of transactions started",
                },
                Metric {
                    name: "pg_tx_committed",
                    description: "Total number of transactions committed successfully",
                },
                Metric {
                    name: "pg_tx_rolled_back",
                    description: "Total number of transactions rolled back or cancelled",
                },
                Metric {
                    name: "pg_tx_active",
                    description: "Number of currently active transactions",
                },
                // Direct mode indicator: 1 = direct (pooler), 0 = pooled.
                Metric {
                    name: "pg_direct_mode",
                    description: "1 if using direct connection mode (pooler), 0 if pooled",
                },
            ],
        })
    }

    fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
        match metric {
            "pg_pool_size" | "pg_pool_idle" => {
                let (size, idle) = self.pool_size();
                self.check_pool_utilization();
                if metric == "pg_pool_size" {
                    Some(size as u64)
                } else {
                    Some(idle) // already u64 from pool_size()
                }
            }
            "pg_pool_max" => Some(if self.is_direct_mode() {
                0
            } else {
                self.pool_max() as u64
            }),
            "pg_tx_started" | "pg_tx_committed" | "pg_tx_rolled_back" | "pg_tx_active" => {
                let (started, committed, rolled_back, active) = self.tx_metrics();
                match metric {
                    "pg_tx_started" => Some(started),
                    "pg_tx_committed" => Some(committed),
                    "pg_tx_rolled_back" => Some(rolled_back),
                    _ => Some(active),
                }
            }
            "pg_direct_mode" => Some(if self.is_direct_mode() { 1 } else { 0 }),
            _ => None,
        }
    }
}
