//! PgStore — the datastore / factory layer that holds a PG connection pool
//! and spawns [`PgTransaction`] instances.

use std::sync::Arc;

use sqlx::Executor;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tokio_util::sync::CancellationToken;
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
    /// Cancellation token — checked before starting new transactions.
    /// When the server shuts down, this token is cancelled and all in-flight
    /// `begin()` calls return `TxCancelled` instead of acquiring a connection.
    canceller: CancellationToken,
    /// Maximum pool connections (from config), used by metrics reporting.
    /// sqlx 0.8 `PgPool` doesn't expose `max_connections()`, so we store it.
    pool_max: u32,
    /// Pre-built VACUUM SQL string. VACUUM doesn't support parameterised
    /// binding (PG limitation), but `table_name` is validated by
    /// `validate_identifier`, so this is safe.
    vacuum_sql: Arc<str>,
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
    pub async fn new(url: &str, canceller: CancellationToken) -> Result<Arc<Self>> {
        // ── Load configs ──
        let mut config = PgConfig::default();
        config
            .merge_url_params(url)
            .map_err(PgStoreError::Other)?;
        config.merge_env();

        // Post-merge cross-validation: min_connections must not exceed max_connections.
        // This catches the case where URL params appear in an order that defeats
        // the per-field check (e.g. ?min_connections=20&max_connections=10).
        if let (Some(min), Some(max)) = (config.min_connections, config.max_connections)
            && min > max
        {
            warn!(
                "min_connections={min} > max_connections={max}, capping min to max"
            );
            config.min_connections = Some(max);
        }

        let tune = PgTuneConfig::from_env();

        // URL params override tuning env vars for pool sizing (URL is more specific)
        let pool_max = config.max_connections.unwrap_or(tune.pool_max);
        let pool_min = config.min_connections.unwrap_or(tune.pool_min);
        let acquire_timeout = config.connect_timeout.unwrap_or(tune.pool_acquire_timeout);
        let idle_timeout = config.idle_timeout.or(Some(tune.pool_idle_timeout));
        let max_lifetime = config.max_lifetime.or(Some(tune.pool_max_lifetime));
        // Pre-build the VACUUM SQL — table_name is validated by validate_identifier.
        let vacuum_sql: Arc<str> = format!("VACUUM ANALYZE {}", config.table_name).into();

        let opts: PgConnectOptions = url
            .parse()
            .map_err(|e: sqlx::Error| PgStoreError::Postgres(format!("invalid URL: {e}")))?;

        // ── Build session SQL for after_connect ──
        let session_sql: Arc<str> = tune.session_sql().into();

        let pool = PgPoolOptions::new()
            .max_connections(pool_max)
            .min_connections(pool_min)
            .acquire_timeout(acquire_timeout)
            .idle_timeout(idle_timeout)
            .max_lifetime(max_lifetime)
            .after_connect(move |conn, _meta| {
                let sql = session_sql.clone(); // Arc clone — atomic refcount, no heap alloc
                Box::pin(async move {
                    Executor::execute(conn, sqlx::raw_sql(&sql)).await?;
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
            canceller,
            pool_max,
            vacuum_sql,
        }))
    }

    /// Begin a new transaction.
    ///
    /// If `write` is true, starts a read-write transaction with the configured
    /// isolation level. If `write` is false, starts a read-only transaction
    /// (or a regular transaction if `read_only_optimization` is disabled).
    pub async fn begin(&self, write: bool) -> Result<PgTransaction> {
        // Check cancellation before acquiring a connection from the pool.
        // This prevents new transactions from starting during shutdown.
        if self.canceller.is_cancelled() {
            return Err(PgStoreError::TxCancelled);
        }

        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        // Build the BEGIN statement.
        let begin_sql = if write {
            format!(
                "BEGIN ISOLATION LEVEL {}",
                self.config.isolation_level.as_sql()
            )
        } else if self.config.read_only_optimization {
            format!(
                "BEGIN ISOLATION LEVEL {} READ ONLY",
                self.config.isolation_level.as_sql()
            )
        } else {
            "BEGIN".to_string()
        };

        // Attempt BEGIN directly. On the normal path (no leaked transaction),
        // this saves a network round-trip compared to always doing ROLLBACK first.
        // If BEGIN fails with a "already in a transaction" error (25P01 or 25P02),
        // we ROLLBACK the leaked transaction and retry once.
        let result = Executor::execute(&mut *conn, sqlx::raw_sql(&begin_sql)).await;
        match result {
            Ok(_) => {}
            Err(e) => {
                // Use SQLSTATE codes instead of string matching for
                // cross-version reliability.
                // 25P01 = no_active_sql_transaction
                // 25P02 = in_failed_sql_transaction
                let is_tx_active = matches!(&e, sqlx::Error::Database(db)
                    if matches!(db.code().as_deref(), Some("25P01") | Some("25P02")));
                if is_tx_active {
                    // Leaked transaction detected — clean up and retry.
                    let _ = Executor::execute(&mut *conn, sqlx::raw_sql("ROLLBACK")).await;
                    Executor::execute(&mut *conn, sqlx::raw_sql(&begin_sql))
                        .await
                        .map_err(|e2| PgStoreError::from_sqlx(None, &e2))?;
                    warn!("cleaned up leaked transaction from pool connection");
                } else {
                    return Err(PgStoreError::from_sqlx(None, &e));
                }
            }
        }

        Ok(PgTransaction::new(
            conn,
            write,
            self.config.isolation_level,
            self.persistent,
            &self.config.table_name,
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

    /// Get the configured maximum pool size.
    ///
    /// sqlx 0.8's `PgPool` doesn't expose `max_connections()`, so we store
    /// the value from our config at construction time.
    #[must_use]
    pub fn pool_max(&self) -> u32 {
        self.pool_max
    }

    /// Run VACUUM ANALYZE on the table (must be called outside a transaction).
    pub async fn vacuum(&self) -> Result<()> {
        Executor::execute(self.pool(), sqlx::raw_sql(&self.vacuum_sql))
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        info!("VACUUM ANALYZE {} completed", self.config.table_name);
        Ok(())
    }

    /// Execute a lightweight health check (`SELECT 1`).
    ///
    /// Suitable for Kubernetes liveness/readiness probes or load-balancer
    /// health checks. Acquires a connection from the pool, executes `SELECT 1`,
    /// and returns it.
    pub async fn health_check(&self) -> Result<()> {
        Executor::execute(&self.pool, sqlx::raw_sql("SELECT 1"))
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
        Ok(())
    }

    /// Return the current pool size info.
    #[must_use]
    pub fn pool_size(&self) -> (u32, u32) {
        (self.pool.size(), self.pool.num_idle() as u32)
    }

    /// Attempt to dynamically resize the connection pool.
    ///
    /// **Note**: sqlx 0.8's `PgPool` does not support runtime resize.
    /// This method logs the request and returns `Ok(())` as a future
    /// placeholder. When sqlx adds resize support, this can be
    /// implemented without changing the public API.
    pub fn try_resize_pool(&self, max: u32, min: u32) -> Result<()> {
        if max < min {
            return Err(PgStoreError::Other(format!(
                "max_connections ({max}) must be >= min_connections ({min})"
            )));
        }
        info!(
            "pool resize requested: max={max}, min={min} (not yet supported by sqlx 0.8)"
        );
        Ok(())
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
    /// 1. Acquires a connection, creates a named prepared statement, then
    ///    releases it back to the pool.
    /// 2. Acquires a second connection and attempts the same. Because both
    ///    connections start their statement-ID counter at 0, the IDs will
    ///    collide if they hit the same backend session.
    /// 3. On success → direct PG (or session-mode pooler) → `true`.
    ///    On `42P05` → transaction-mode pooler → `false`.
    ///    Other errors → log and return `false` (safe default).
    ///
    /// Connections are acquired **sequentially** (not simultaneously) so the
    /// peak connection requirement is 1, not 2. This avoids pool exhaustion
    /// when `min_connections` is set very low.
    ///
    /// We also use a **non-trivial SQL** (`SELECT $1::int4`) so the
    /// statement is actually prepared server-side (not just a simple query).
    async fn probe_persistent(pool: &PgPool) -> bool {
        let probe_sql = "SELECT $1::int4";

        // Phase 1: acquire conn1, create a named prepared statement, then
        // release it back to the pool before acquiring conn2.
        let mut conn1 = match pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                warn!("persistent probe: failed to acquire conn1: {e}");
                return false;
            }
        };

        let r1 = sqlx::query(probe_sql)
            .persistent(true)
            .bind(1i32)
            .execute(&mut *conn1)
            .await;

        if let Err(e) = r1 {
            warn!("persistent probe: phase 1 (conn1) failed: {e}");
            return false;
        }

        // Clean up and release conn1 back to the pool.
        let _ = Executor::execute(&mut *conn1, sqlx::raw_sql("DEALLOCATE ALL")).await;
        drop(conn1);

        // Phase 2: acquire conn2 and attempt the same named prepared statement.
        // In direct PG: conn2 has its own backend session, no conflict.
        // In pooler tx mode: conn2 may share a backend session, causing 42P05.
        let mut conn2 = match pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                warn!("persistent probe: failed to acquire conn2: {e}");
                return false;
            }
        };

        let r2 = sqlx::query(probe_sql)
            .persistent(true)
            .bind(2i32)
            .execute(&mut *conn2)
            .await;

        match r2 {
            Ok(_) => {
                info!("persistent probe: no conflict, direct PG or session mode");
                let _ = Executor::execute(&mut *conn2, sqlx::raw_sql("DEALLOCATE ALL")).await;
                true
            }
            Err(e) => {
                // Use SQLSTATE codes instead of string matching.
                // 42P05 = duplicate_prepared_statement
                // 26000 = invalid_sql_statement_name (prepared statement not found)
                // 08006 = connection_failure (pooler may drop)
                let is_pooler = matches!(&e, sqlx::Error::Database(db)
                    if matches!(db.code().as_deref(),
                        Some("42P05") | Some("26000") | Some("08006")));

                if is_pooler {
                    info!(
                        "persistent probe: pooler transaction mode detected (statement conflict)"
                    );
                } else {
                    warn!("persistent probe: phase 2 (conn2) failed (disabling persistent): {e}");
                }
                let _ = Executor::execute(&mut *conn2, sqlx::raw_sql("DEALLOCATE ALL")).await;
                false
            }
        }
    }
}
