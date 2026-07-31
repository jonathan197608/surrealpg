//! PgStore — the datastore / factory layer that holds a PG connection pool
//! and spawns [`PgTransaction`] instances.

use std::sync::Arc;
use std::time::Duration;

use sqlx::Executor;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tracing::{info, warn};

use crate::config::{PersistentStatements, PgConfig};
use crate::error::{PgStoreError, Result};
use crate::transaction::PgTransaction;
use crate::tune::PgTuneConfig;

// ─── PgStore ────────────────────────────────────────────

/// PostgreSQL-backed key-value store.
///
/// Holds a connection pool and configuration. Each call to [`PgStore::begin`]
/// acquires a connection from the pool and starts a PostgreSQL transaction,
/// returning a [`PgTransaction`].
#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
    config: PgConfig,
    tune: PgTuneConfig,
    /// Resolved persistent-statements flag (concrete `bool` after startup probe).
    persistent: bool,
}

impl PgStore {
    /// Create a new `PgStore` from a PostgreSQL connection URL.
    ///
    /// The URL should be in the standard libpq format:
    /// `postgres://user:pass@host:5432/dbname?param=value`
    ///
    /// Tuning parameters are loaded from `PG_TUNED_*` environment variables;
    /// URL query params (`max_connections`, `min_connections`, etc.) override
    /// the pool-level tuning defaults.
    pub async fn new(url: &str) -> Result<Arc<Self>> {
        // ── Load configs ──
        let mut config = PgConfig::default();
        config.merge_url_params(url);
        config.merge_env();

        let tune = PgTuneConfig::from_env();

        // URL params override tuning env vars for pool sizing (URL is more specific)
        let pool_max = if config.max_connections != 20 {
            config.max_connections
        } else {
            tune.pool_max
        };
        let pool_min = if config.min_connections != 5 {
            config.min_connections
        } else {
            tune.pool_min
        };
        let acquire_timeout = if config.connect_timeout != Duration::from_secs(10) {
            config.connect_timeout
        } else {
            tune.pool_acquire_timeout
        };
        let idle_timeout = config.idle_timeout.or(Some(tune.pool_idle_timeout));
        let max_lifetime = config.max_lifetime.or(Some(tune.pool_max_lifetime));

        let opts: PgConnectOptions = url
            .parse()
            .map_err(|e: sqlx::Error| PgStoreError::Postgres(format!("invalid URL: {e}")))?;

        // ── Build session SQL for after_connect ──
        let session_sql = tune.session_sql();

        let pool = PgPoolOptions::new()
            .max_connections(pool_max)
            .min_connections(pool_min)
            .acquire_timeout(acquire_timeout)
            .idle_timeout(idle_timeout)
            .max_lifetime(max_lifetime)
            .after_connect(move |conn, _meta| {
                let sql = session_sql.clone();
                Box::pin(async move {
                    sqlx::Executor::execute(conn, sqlx::raw_sql(&sql)).await?;
                    Ok(())
                })
            })
            .connect_with(opts)
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        info!(
            "connection pool created: max={}, min={}, acquire_timeout={:?}, \
             idle_timeout={:?}, max_lifetime={:?}",
            pool_max, pool_min, acquire_timeout, idle_timeout, max_lifetime
        );

        // ── DDL: create table + table tuning ──
        if config.auto_create_table {
            let table = &config.table_name;

            // 1. CREATE TABLE
            let create_sql = tune.create_table_sql(table);
            Executor::execute(&pool, sqlx::raw_sql(&create_sql))
                .await
                .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
            info!("table '{table}' initialized");

            // 2. Table tuning (non-fatal: table works without these)
            let tune_sql = tune.tune_table_sql(table);
            match Executor::execute(&pool, sqlx::raw_sql(&tune_sql)).await {
                Ok(_) => info!(
                    "table '{table}' tuning applied (fillfactor={}, toast={}, autovac tuned)",
                    tune.fillfactor, tune.toast_storage
                ),
                Err(e) => warn!("table tuning partially failed (non-fatal): {e}"),
            }
        }

        // ── Log PG server hints (params that need postgresql.conf) ──
        tune.log_server_hints();

        // ── Resolve persistent-statements policy ──
        let persistent = match config.persistent_statements {
            PersistentStatements::Auto => {
                let detected = Self::probe_persistent(&pool).await;
                info!(
                    policy = %config.persistent_statements,
                    detected,
                    "persistent-statements auto-detected"
                );
                detected
            }
            ref p => {
                let resolved = p.resolve(false);
                info!(
                    policy = %p,
                    persistent = resolved,
                    "persistent-statements explicitly configured"
                );
                resolved
            }
        };

        info!(
            max_conn = pool_max,
            min_conn = pool_min,
            table = &config.table_name,
            isolation = config.isolation_level.as_sql(),
            persistent,
            "PgStore created"
        );

        Ok(Arc::new(Self {
            pool,
            config,
            tune,
            persistent,
        }))
    }

    /// Begin a new transaction.
    ///
    /// If `write` is true, starts a read-write transaction with the configured
    /// isolation level. If `write` is false, starts a read-only transaction
    /// (or a regular transaction if `read_only_optimization` is disabled).
    pub async fn begin(&self, write: bool) -> Result<PgTransaction> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        // Begin transaction — session-level params (statement_timeout, lock_timeout,
        // idle_in_transaction_timeout, etc.) are already set via after_connect.
        let begin_sql = if write {
            format!(
                "BEGIN ISOLATION LEVEL {}",
                self.config.isolation_level.as_sql()
            )
        } else if self.config.read_only_optimization {
            "BEGIN READ ONLY".to_string()
        } else {
            "BEGIN".to_string()
        };

        Executor::execute(&mut *conn, sqlx::raw_sql(&begin_sql))
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        Ok(PgTransaction::new(
            conn,
            write,
            self.config.isolation_level,
            self.persistent,
            self.config.table_name.clone(),
        ))
    }

    /// Shut down the connection pool gracefully.
    pub async fn shutdown(&self) {
        self.pool.close().await;
        info!("PgStore shut down");
    }

    /// Get a reference to the underlying pool (for admin operations like VACUUM).
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get a reference to the current config.
    #[must_use]
    pub fn config(&self) -> &PgConfig {
        &self.config
    }

    /// Get a reference to the tuning config.
    #[must_use]
    pub fn tune(&self) -> &PgTuneConfig {
        &self.tune
    }

    /// Get the resolved persistent-statements flag.
    ///
    /// After `PgStore::new`, this is the concrete `bool` value that was
    /// either auto-detected or explicitly configured.
    #[must_use]
    pub fn persistent(&self) -> bool {
        self.persistent
    }

    /// Run VACUUM ANALYZE on the table (must be called outside a transaction).
    pub async fn vacuum(&self) -> Result<()> {
        let sql = format!("VACUUM ANALYZE {}", self.config.table_name);
        Executor::execute(self.pool(), sqlx::raw_sql(&sql))
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        info!("VACUUM ANALYZE {} completed", self.config.table_name);
        Ok(())
    }

    /// Return the current pool size info.
    #[must_use]
    pub fn pool_size(&self) -> (u32, u32) {
        (self.pool.size(), self.pool.num_idle() as u32)
    }

    /// Probe whether the server is behind a connection pooler (pgbouncer /
    /// Supavisor) in transaction-pooling mode.
    ///
    /// ## Strategy: named prepared statement conflict
    ///
    /// In **direct PG**, each client connection maps 1:1 to a backend
    /// process. sqlx assigns sequential statement IDs (`sqlx_s_1`,
    /// `sqlx_s_2`, …) **per `PgConnection`**. Two different pool
    /// connections will each start from `sqlx_s_1` — no conflict because
    /// they are in separate backend sessions.
    ///
    /// Behind a **transaction-mode pooler** (pgbouncer tx mode, Supavisor),
    /// the pooler may route client connections through **shared** backend
    /// sessions. When two `PgConnection`s both create a named prepared
    /// statement with the same ID (`sqlx_s_1`) on the same backend, the
    /// second `Parse` fails with `42P05` (`duplicate_prepared_statement`).
    ///
    /// This probe:
    /// 1. Acquires **two** connections from the pool.
    /// 2. On conn-1: creates a named prepared statement (`.persistent(true)`).
    /// 3. On conn-2: attempts to create a named prepared statement with the
    ///    same SQL. Because both connections start their statement-ID
    ///    counter at 0, the IDs will collide if they hit the same backend
    ///    session.
    /// 4. On success → direct PG (or session-mode pooler) → `true`.
    ///    On `42P05` → transaction-mode pooler → `false`.
    /// 5. Other errors → log and return `false` (safe default).
    ///
    /// We also use a **non-trivial SQL** (`SELECT $1::int4`) so the
    /// statement is actually prepared server-side (not just a simple query).
    async fn probe_persistent(pool: &PgPool) -> bool {
        let probe_sql = "SELECT $1::int4";

        // Acquire two connections
        let mut conn1 = match pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                warn!("persistent probe: failed to acquire conn1: {e}");
                return false;
            }
        };
        let mut conn2 = match pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                warn!("persistent probe: failed to acquire conn2: {e}");
                return false;
            }
        };

        // Phase 1: create a named prepared statement on conn1
        let r1 = sqlx::query(probe_sql)
            .persistent(true)
            .bind(1i32)
            .execute(&mut *conn1)
            .await;

        if let Err(e) = r1 {
            warn!("persistent probe: phase 1 (conn1) failed: {e}");
            return false;
        }

        // Phase 2: create a named prepared statement on conn2 with the same SQL.
        // In direct PG: conn2 has its own backend session, no conflict.
        // In pooler tx mode: conn2 may share a backend session, causing 42P05.
        let r2 = sqlx::query(probe_sql)
            .persistent(true)
            .bind(2i32)
            .execute(&mut *conn2)
            .await;

        match r2 {
            Ok(_) => {
                info!("persistent probe: no conflict, direct PG or session mode");
                true
            }
            Err(e) => {
                let err_str = e.to_string().to_ascii_lowercase();
                let is_pooler = err_str.contains("already exists")
                    || err_str.contains("duplicate_prepared_statement")
                    || err_str.contains("prepared statement") && err_str.contains("does not exist");

                if is_pooler {
                    info!(
                        "persistent probe: pooler transaction mode detected (statement conflict)"
                    );
                } else {
                    warn!("persistent probe: phase 2 (conn2) failed (disabling persistent): {e}");
                }
                false
            }
        }
    }
}
