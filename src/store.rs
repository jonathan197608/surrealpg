//! PgStore — the datastore / factory layer that holds a PG connection pool
//! and spawns [`PgTransaction`] instances.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

use sqlx::ConnectOptions;
use sqlx::Executor;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tracing::{info, warn};

use crate::config::{PersistentStatements, PgConfig};
use crate::error::{PgStoreError, Result};
use crate::transaction::PgTransaction;
use crate::transaction::Sql;
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
    /// B6: Arc-wrapped config to avoid deep clone on PgStore::clone().
    /// Arc-shared: immutable after construction. Do not use Arc::get_mut().
    config: Arc<PgConfig>,
    /// B6: Arc-wrapped tune to avoid deep clone on PgStore::clone().
    /// Arc-shared: immutable after construction. Do not use Arc::get_mut().
    tune: Arc<PgTuneConfig>,
    /// Resolved persistent-statements flag (concrete `bool` after startup probe).
    persistent: bool,
    /// Maximum pool connections (from config), used by metrics reporting.
    /// sqlx 0.8 `PgPool` doesn't expose `max_connections()`, so we store it.
    pool_max: u32,
    /// Pre-built BEGIN SQL (isolation level fixed at construction).
    /// Used for both read and write transactions — all SurrealDB transactions
    /// use the same BEGIN since read-only enforcement is handled at the
    /// application layer via `check_writable()`. Passed to sqlx's
    /// `pool.begin_with()` which manages the transaction lifecycle
    /// (including automatic ROLLBACK on Drop), eliminating the
    /// "there is already a transaction in progress" WARNING that
    /// occurred with raw `execute("BEGIN …")` + manual `PoolConnection`
    /// management.
    begin_sql: Arc<str>,
    /// Pre-built VACUUM SQL string. VACUUM doesn't support parameterised
    /// binding (PG limitation), but `table_name` is validated by
    /// `validate_identifier`, so this is safe.
    vacuum_sql: Arc<str>,
    /// Pre-built SQL strings for all KV operations. Shared with each
    /// `PgTransaction` via `Arc::clone` (1 atomic increment) instead of
    /// rebuilding 14 `format!()` strings per transaction.
    sql: Arc<Sql>,
    // ── F8: Transaction metrics (AtomicU64 for lock-free concurrent updates) ──
    /// Total number of transactions started (both read and write).
    tx_started: Arc<AtomicU64>,
    /// Total number of transactions committed successfully.
    tx_committed: Arc<AtomicU64>,
    /// Total number of transactions rolled back / cancelled.
    tx_rolled_back: Arc<AtomicU64>,
    /// Number of currently active transactions (connections checked out from pool).
    /// Incremented on begin(), decremented on commit/cancel/drop. Used for
    /// diagnostics: when pool acquire times out, this tells us how many
    /// connections are held by active transactions vs. how many are idle.
    tx_active: Arc<AtomicU64>,
    /// O3: One-shot flag for pool utilization warning. Prevents log spam
    /// when the pool hovers at high utilization. Reset only on restart.
    pool_warned: Arc<AtomicBool>,
}

// ─── URL sanitization ─────────────────────────────────

/// Custom query parameters that we parse ourselves and must not be
/// passed to sqlx (which would emit "ignoring unrecognized connect parameter"
/// warnings). These are all consumed by [`PgConfig::merge_url_params`].
const CUSTOM_PARAMS: &[&str] = &[
    "max_connections",
    "min_connections",
    "max_lifetime",
    "auto_create_table",
    "table_name",
    "isolation_level",
    "persistent_statements",
    "connect_timeout",
    "idle_timeout",
    "slow_acquire_threshold_secs",
    "slow_statements_threshold_secs",
];

/// Strip our custom query parameters from a PostgreSQL URL so that sqlx's
/// `PgConnectOptions` parser won't emit "ignoring unrecognized connect
/// parameter" warnings.
///
/// Only removes keys listed in [`CUSTOM_PARAMS`]; all other query parameters
/// (e.g. `sslmode`, `application_name`) are preserved for sqlx to handle.
fn strip_custom_params(url: &str) -> String {
    let Some(qmark) = url.find('?') else {
        return url.to_string();
    };
    let base_before_qmark = &url[..qmark]; // excludes '?'
    let query = &url[qmark + 1..];
    let fragment_start = query.find('#');
    let (query_part, fragment) = match fragment_start {
        Some(i) => (&query[..i], &query[i..]), // includes '#'
        None => (query, ""),
    };

    let filtered: String = query_part
        .split('&')
        .filter(|pair| {
            let key = pair.split_once('=').map(|(k, _)| k).unwrap_or(pair);
            !CUSTOM_PARAMS.contains(&key)
        })
        .collect::<Vec<_>>()
        .join("&");

    // R2-H1: When all custom params are stripped and nothing remains,
    // omit the trailing '?' — a bare '?' is semantically "empty query
    // string" which some parsers treat differently from "no query string".
    if filtered.is_empty() {
        format!("{base_before_qmark}{fragment}")
    } else {
        format!("{base_before_qmark}?{filtered}{fragment}")
    }
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
        config.merge_url_params(url).map_err(PgStoreError::Other)?;
        config.merge_env();

        // Post-merge cross-validation: min_connections must not exceed max_connections.
        // This catches the case where URL params appear in an order that defeats
        // the per-field check (e.g. ?min_connections=20&max_connections=10).
        if let (Some(min), Some(max)) = (config.min_connections, config.max_connections)
            && min > max
        {
            warn!("min_connections={min} > max_connections={max}, capping min to max");
            config.min_connections = Some(max);
        }

        let tune = PgTuneConfig::from_env();

        // URL params override tuning env vars for pool sizing (URL is more specific)
        let pool_max = config.max_connections.unwrap_or(tune.pool_max);
        let pool_min = config.min_connections.unwrap_or(tune.pool_min);
        // H1: Final cross-validation after resolving pool_max/pool_min from
        // both URL and tuning env. The earlier check only fires when both
        // values come from URL. Here we catch the mixed case (e.g. URL
        // min_connections > tune pool_max, or URL max_connections < tune
        // pool_min).
        let pool_min = pool_min.min(pool_max);
        let acquire_timeout = config.connect_timeout.unwrap_or(tune.pool_acquire_timeout);
        let idle_timeout = config.idle_timeout.or(Some(tune.pool_idle_timeout));
        let max_lifetime = config.max_lifetime.or(Some(tune.pool_max_lifetime));
        // Pre-build the VACUUM SQL — table_name is validated by validate_identifier.
        let vacuum_sql: Arc<str> = format!("VACUUM ANALYZE {}", config.table_name).into();

        let sql: Arc<Sql> = Arc::new(Sql::new(&config.table_name));

        // Pre-build BEGIN SQL — isolation_level is immutable after construction,
        // so we build it once here and avoid a format!() allocation on every begin() call.
        let begin_sql: Arc<str> =
            format!("BEGIN ISOLATION LEVEL {}", config.isolation_level.as_sql()).into();

        // F5: Guard against zero max_connections — sqlx panics with pool_max=0.
        // from_env() already clamps env vars, but direct config construction
        // could still produce 0. Return an error instead of panicking so the
        // caller can handle it gracefully.
        if pool_max == 0 {
            return Err(PgStoreError::Other(
                "max_connections must be > 0".to_string(),
            ));
        }
        if pool_min > pool_max {
            warn!(
                min_connections = pool_min,
                max_connections = pool_max,
                "pool_min > pool_max after resolution, clamping"
            );
        }

        let slow_acquire = config.slow_acquire_threshold_secs;
        let slow_stmts = config.slow_statements_threshold_secs;

        let mut opts: PgConnectOptions = strip_custom_params(url)
            .parse()
            .map_err(|e: sqlx::Error| PgStoreError::Postgres(format!("invalid URL: {e}")))?;

        if let Some(threshold) = slow_stmts {
            opts = opts.log_slow_statements(tracing::log::LevelFilter::Warn, threshold);
        }

        // ── Build session SQL for after_connect ──
        let session_sql: Arc<str> = tune.session_sql().into();
        // ── TCP keepalive SQL ──
        // Supabase Pooler / pgbouncer can silently reclaim idle connections
        // on the server side. Without keepalive, sqlx doesn't know the
        // connection is dead until it tries to use it (test_before_acquire
        // ping). By setting aggressive TCP keepalive, the OS proactively
        // detects dead connections, allowing sqlx's idle_timeout / max_lifetime
        // to recycle them before they cause pool exhaustion.
        //
        // Default: idle=60s, interval=10s, count=5 → detects dead conn in ~110s.
        // These are PG session parameters, so they work even behind poolers
        // (each client connection gets its own backend process).
        // Note: Supabase / some hosted PG may ignore tcp_keepalive settings,
        // but setting them is harmless and helps with direct PG connections.
        let keepalive_sql: Arc<str> = format!(
            "SET tcp_keepalives_idle = {idle}; \
             SET tcp_keepalives_interval = {interval}; \
             SET tcp_keepalives_count = {count}",
            idle = tune.keepalive_idle.as_secs(),
            interval = tune.keepalive_interval.as_secs(),
            count = tune.keepalive_count,
        )
        .into();

        let mut pool_opts = PgPoolOptions::new()
            .max_connections(pool_max)
            .min_connections(pool_min)
            .acquire_timeout(acquire_timeout)
            .idle_timeout(idle_timeout)
            .max_lifetime(max_lifetime);

        if let Some(threshold) = slow_acquire {
            pool_opts = pool_opts.acquire_slow_threshold(threshold);
        }

        let pool = pool_opts
            .after_connect(move |conn, _meta| {
                let sql = session_sql.clone(); // Arc clone — atomic refcount, no heap alloc
                let ka = keepalive_sql.clone();
                Box::pin(async move {
                    // Apply TCP keepalive first — if this fails (e.g. hosted PG
                    // doesn't support it), log a warning but don't fail the
                    // connection. The session SET below may also set keepalive
                    // params, but these explicit SETs give a more specific error.
                    if let Err(e) = Executor::execute(&mut *conn, sqlx::raw_sql(&ka)).await {
                        warn!(
                            error = %e,
                            "tcp_keepalive SET failed (non-fatal, may not be supported by this PG)"
                        );
                    }
                    Executor::execute(conn, sqlx::raw_sql(&sql)).await?;
                    Ok(())
                })
            })
            .before_acquire(|conn, meta| {
                // Conditional ping: only ping connections that have been idle
                // longer than the keepalive idle threshold. This avoids the
                // overhead of pinging recently-used connections (the common
                // case) while still catching stale connections that survived
                // the keepalive detection window.
                //
                // Returns Ok(true) = connection is usable, Ok(false) = discard
                // and try another, Err = connection error.
                Box::pin(async move {
                    if meta.idle_for > std::time::Duration::from_secs(60) {
                        sqlx::Connection::ping(conn).await?;
                    }
                    Ok(true)
                })
            })
            .connect_with(opts)
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        info!(
            "connection pool created: max={}, min={}, acquire_timeout={:?}, \
             idle_timeout={:?}, max_lifetime={:?}, slow_acquire_threshold={:?}",
            pool_max, pool_min, acquire_timeout, idle_timeout, max_lifetime, slow_acquire
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
                // B3: Small pool (pool_max ≤ 2) cannot reliably probe — the
                // probe requires acquiring and releasing conn1 before acquiring
                // conn2. With only 2 connections, the pool may not have enough
                // headroom. Skip the probe and default to false (safe for pgbouncer).
                if pool_max <= 2 {
                    info!(
                        policy = %config.persistent_statements,
                        pool_max,
                        "persistent-statements auto: pool too small for probe, defaulting to false"
                    );
                    false
                } else {
                    let detected = Self::probe_persistent(&pool).await;
                    info!(
                        policy = %config.persistent_statements,
                        detected,
                        "persistent-statements auto-detected"
                    );
                    detected
                }
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
            config: Arc::new(config),
            tune: Arc::new(tune),
            persistent,
            pool_max,
            begin_sql,
            vacuum_sql,
            sql,
            // F8: Initialize transaction metric counters.
            tx_started: Arc::new(AtomicU64::new(0)),
            tx_committed: Arc::new(AtomicU64::new(0)),
            tx_rolled_back: Arc::new(AtomicU64::new(0)),
            tx_active: Arc::new(AtomicU64::new(0)),
            // O3: Initialize pool utilization warning flag.
            pool_warned: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Begin a new transaction.
    ///
    /// Uses sqlx's `pool.begin_with()` to start a transaction with the
    /// configured isolation level. sqlx manages the full transaction
    /// lifecycle: `Transaction::drop` automatically queues a ROLLBACK
    /// (via `start_rollback`) for any uncommitted transaction, preventing
    /// the "there is already a transaction in progress" WARNING that
    /// occurred when `PoolConnection` was returned to the pool with an
    /// active transaction.
    ///
    /// Read-only enforcement is handled at the application layer via
    /// `check_writable()` — SurrealDB's `Transactable` trait distinguishes
    /// read/write transactions, and our write methods reject writes on
    /// non-writable transactions. This avoids PG's hard `READ ONLY` constraint
    /// which conflicts with SurrealDB's internal write operations (node
    /// registration, event processing) that may occur inside transactions
    /// requested as read-only.
    ///
    /// Note: the former `canceller.is_cancelled()` check has been removed.
    /// SurrealDB's shutdown sequence cancels the `CancellationToken` *before*
    /// calling `Datastore::shutdown()`, which needs to create a transaction
    /// to archive the node. The canceller check prevented this shutdown
    /// transaction from being created, causing "Couldn't update a finished
    /// transaction" errors. Connection pool closure (`pool.close()`) already
    /// prevents new transactions after shutdown completes.
    ///
    /// # Retry behavior
    ///
    /// When the connection pool is temporarily exhausted (e.g. due to a brief
    /// spike in concurrent tasks), a single `begin_with()` attempt may time
    /// out even though the pool would recover within seconds. We retry up to
    /// `BEGIN_MAX_RETRIES` times with exponential backoff, logging pool
    /// diagnostics on each failure to help distinguish between:
    ///
    /// - **Pool exhausted**: all connections are in use (size == max).
    ///   Indicates long-running transactions holding connections.
    /// - **Pool depleted**: few connections exist (size << max).
    ///   Indicates connection establishment failures (e.g. PG server down,
    ///   network issues, Pooler rejecting connections).
    pub async fn begin(&self, write: bool) -> Result<PgTransaction> {
        use tracing::warn;

        const BEGIN_MAX_RETRIES: u32 = 2;
        const BASE_DELAY_MS: u64 = 200;
        const MAX_DELAY_MS: u64 = 2000;
        // Zombie pool: connections are stuck in sqlx rebuild — they need
        // much longer to recover than a normal transient pool exhaustion.
        // Use 5s base delay to give sqlx time to rebuild dead connections.
        const ZOMBIE_BASE_DELAY_MS: u64 = 5000;
        const ZOMBIE_MAX_DELAY_MS: u64 = 15000;

        let mut attempt = 0;
        let mut zombie_detected = false;
        loop {
            attempt += 1;

            match self.pool.begin_with(String::from(&*self.begin_sql)).await {
                Ok(tx) => {
                    self.tx_started.fetch_add(1, AtomicOrdering::Relaxed);
                    self.tx_active.fetch_add(1, AtomicOrdering::Relaxed);
                    return Ok(PgTransaction::new_with_sql(
                        tx,
                        write,
                        self.config.isolation_level,
                        self.persistent,
                        Arc::clone(&self.sql),
                        Arc::clone(&self.tx_active),
                    ));
                }
                Err(e) => {
                    let pg_err = PgStoreError::from_sqlx(None, &e);

                    // Log pool diagnostics on every failure to help identify
                    // whether the pool is exhausted or depleted.
                    let (size, idle) = self.pool_size();
                    let active = self.tx_active.load(AtomicOrdering::Relaxed);

                    // Detect the "zombie pool" pattern: all connections are
                    // checked out from the pool (idle=0) but none are held
                    // by our code (tx_active=0). This means sqlx internally
                    // holds all connections — likely they are being rebuilt
                    // after the server-side (Pooler) silently closed them.
                    // Output actionable guidance to help the operator recover.
                    if idle == 0 && active == 0 && size > 0 {
                        zombie_detected = true;
                        warn!(
                            attempt,
                            max_retries = BEGIN_MAX_RETRIES,
                            pool_size = size,
                            pool_idle = idle,
                            pool_max = self.pool_max,
                            tx_active = active,
                            write,
                            error = %pg_err,
                            "begin_with() failed — ZOMBIE POOL detected: \
                             all {size} connections held by sqlx (not by our code). \
                             Root cause: Pooler/server silently closed connections. \
                             Recovery: set PG_TUNED_POOL_ACQUIRE_TIMEOUT=30s, \
                             add ?min_connections=5 to URL, and reduce \
                             PG_TUNED_POOL_IDLE_TIMEOUT to 300s to recycle \
                             stale connections faster. Consider restarting the process \
                             if the pool is completely stuck."
                        );
                    } else {
                        warn!(
                            attempt,
                            max_retries = BEGIN_MAX_RETRIES,
                            pool_size = size,
                            pool_idle = idle,
                            pool_max = self.pool_max,
                            tx_active = active,
                            write,
                            error = %pg_err,
                            "begin_with() failed — pool diagnostics logged"
                        );
                    }

                    // Only retry on pool timeout (transient); don't retry
                    // on other errors (e.g. PoolClosed, connection auth failure).
                    if !matches!(pg_err, PgStoreError::PoolTimeout) || attempt > BEGIN_MAX_RETRIES {
                        return Err(pg_err);
                    }

                    // Backoff strategy depends on pool state:
                    // - Zombie pool: use longer delays (5s base) because sqlx
                    //   needs time to rebuild dead connections. A short 200ms
                    //   delay just wastes retries while connections are still
                    //   being re-established (typically 2-3s per connection
                    //   over cross-region Pooler).
                    // - Normal pool exhaustion: use short exponential backoff
                    //   (200ms base) — a transaction will likely finish and
                    //   release its connection within a few hundred ms.
                    let delay = if zombie_detected {
                        let d = ZOMBIE_BASE_DELAY_MS * 2u64.pow(attempt - 1);
                        d.min(ZOMBIE_MAX_DELAY_MS)
                    } else {
                        let d = BASE_DELAY_MS * 2u64.pow(attempt - 1);
                        d.min(MAX_DELAY_MS)
                    };
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
            }
        }
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

    /// O3: Check pool utilization and warn once if it exceeds 80%.
    ///
    /// Called from `collect_u64_metric` when pool metrics are queried.
    /// Uses an `AtomicBool` to ensure the warning fires only once per
    /// process lifetime, avoiding log spam during sustained high load.
    pub(crate) fn check_pool_utilization(&self) {
        let size = self.pool.size();
        // Utilization = active connections / max connections.
        // size() includes both idle and in-use connections, so this
        // reflects total pool pressure.
        if size as u64 * 5 > self.pool_max as u64 * 4 {
            // > 80% utilization
            // swap(true) returns the *previous* value; if it was already
            // true, we've already warned — don't spam.
            if !self.pool_warned.swap(true, AtomicOrdering::Relaxed) {
                warn!(
                    pool_size = size,
                    pool_max = self.pool_max,
                    utilization_pct = (size as f64 / self.pool_max as f64 * 100.0) as u32,
                    "connection pool utilization exceeds 80% — consider increasing max_connections"
                );
            }
        }
    }

    // ── F8: Transaction metric methods ──

    /// Get transaction metric counters: (started, committed, rolled_back, active).
    #[must_use]
    pub fn tx_metrics(&self) -> (u64, u64, u64, u64) {
        (
            self.tx_started.load(AtomicOrdering::Relaxed),
            self.tx_committed.load(AtomicOrdering::Relaxed),
            self.tx_rolled_back.load(AtomicOrdering::Relaxed),
            self.tx_active.load(AtomicOrdering::Relaxed),
        )
    }

    /// F8: Get Arc clones of the commit/rollback counters for PgTx.
    /// Used by pg_builder to pass counters when constructing PgTx.
    #[must_use]
    pub(crate) fn tx_commit_rollback_arcs(&self) -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        (
            Arc::clone(&self.tx_committed),
            Arc::clone(&self.tx_rolled_back),
        )
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
        info!("pool resize requested: max={max}, min={min} (not yet supported by sqlx 0.8)");
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

        // Release conn1 back to the pool. Do NOT DEALLOCATE ALL here —
        // the named prepared statement (`sqlx_s_1`) must remain on the
        // server so that conn2 can detect a conflict if they share the
        // same backend session (pgbouncer transaction mode). Removing it
        // would defeat the probe, making it always return true (direct PG).
        // The statement is cleaned up by conn2's DEALLOCATE ALL below,
        // or by PG's session cleanup when the connection is eventually closed.
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

#[cfg(test)]
mod test_strip {
    use super::*;

    #[test]
    fn no_query() {
        assert_eq!(
            strip_custom_params("postgresql://u:p@h/db"),
            "postgresql://u:p@h/db"
        );
    }

    #[test]
    fn strip_all_custom() {
        // R2-H1: When all custom params are stripped, no trailing '?' remains.
        assert_eq!(
            strip_custom_params("postgresql://u:p@h/db?min_connections=0&max_connections=20"),
            "postgresql://u:p@h/db"
        );
    }

    #[test]
    fn preserve_sqlx_params() {
        assert_eq!(
            strip_custom_params("postgresql://u:p@h/db?sslmode=require&min_connections=0"),
            "postgresql://u:p@h/db?sslmode=require"
        );
    }

    #[test]
    fn mixed_params() {
        assert_eq!(
            strip_custom_params(
                "postgresql://u:p@h/db?sslmode=require&min_connections=0&application_name=test"
            ),
            "postgresql://u:p@h/db?sslmode=require&application_name=test"
        );
    }

    #[test]
    fn with_fragment() {
        // R2-H1: All custom params stripped → no trailing '?' before fragment.
        assert_eq!(
            strip_custom_params("postgresql://u:p@h/db?min_connections=0#frag"),
            "postgresql://u:p@h/db#frag"
        );
    }
}
