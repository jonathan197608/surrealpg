//! PgStore — the datastore / factory layer that holds a PG connection pool
//! (or direct connection options) and spawns [`PgTransaction`] instances.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

use sqlx::ConnectOptions;
use sqlx::Executor;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tracing::{info, warn};

use crate::config::PgConfig;
use crate::error::{PgStoreError, Result};
use crate::transaction::PgTransaction;
use crate::transaction::Sql;
use crate::tune::PgTuneConfig;

// ─── PgStore ────────────────────────────────────────────

/// Connection mode: pooled (sqlx `PgPool`) or direct (per-transaction connect).
///
/// `Direct` mode is used when the server is behind a connection pooler
/// (Supabase Pooler / pgbouncer transaction mode). In this mode, each
/// transaction creates a fresh TCP connection via `PgConnectOptions::connect()`
/// and closes it when the transaction ends. This avoids the "zombie pool"
/// problem where the pooler silently reclaims idle connections and sqlx's
/// internal pool enters a stuck rebuild state.
///
/// `Pooled` mode is the default for direct PG connections. It uses sqlx's
/// `PgPool` with `min_connections`, `idle_timeout`, `max_lifetime`, etc.
#[derive(Clone)]
enum ConnectionMode {
    /// sqlx connection pool (default, for direct PG).
    Pooled(PgPool),
    /// Per-transaction direct connect (for pooler / pgbouncer tx mode).
    Direct(Box<PgConnectOptions>),
}

/// PostgreSQL-backed key-value store.
///
/// Holds either a connection pool or direct-connection options, plus shared
/// configuration. Each call to [`PgStore::begin`] starts a PostgreSQL
/// transaction, returning a [`PgTransaction`].
#[derive(Clone)]
pub struct PgStore {
    /// Connection mode: pooled or direct.
    mode: ConnectionMode,
    /// B6: Arc-wrapped config to avoid deep clone on PgStore::clone().
    /// Arc-shared: immutable after construction. Do not use Arc::get_mut().
    config: Arc<PgConfig>,
    /// B6: Arc-wrapped tune to avoid deep clone on PgStore::clone().
    /// Arc-shared: immutable after construction. Do not use Arc::get_mut().
    tune: Arc<PgTuneConfig>,
    /// Resolved persistent-statements flag (concrete `bool` after startup).
    /// In pooler (direct) mode this is always `false`.
    persistent: bool,
    /// Maximum pool connections (from config), used by metrics reporting.
    /// In direct mode this is informational only (no pool).
    pool_max: u32,
    /// Pre-built BEGIN SQL (isolation level fixed at construction).
    begin_sql: Arc<str>,
    /// Pre-built VACUUM SQL string.
    vacuum_sql: Arc<str>,
    /// Pre-built SQL strings for all KV operations. Shared with each
    /// `PgTransaction` via `Arc::clone`.
    sql: Arc<Sql>,
    // ── F8: Transaction metrics ──
    tx_started: Arc<AtomicU64>,
    tx_committed: Arc<AtomicU64>,
    tx_rolled_back: Arc<AtomicU64>,
    /// Number of currently active transactions.
    tx_active: Arc<AtomicU64>,
    /// O3: One-shot flag for pool utilization warning.
    pool_warned: Arc<AtomicBool>,
    /// Session SQL applied to every new connection (both modes).
    session_sql: Arc<str>,
    /// TCP keepalive SQL applied to every new connection (both modes).
    keepalive_sql: Arc<str>,
    /// Connect timeout for direct-mode connections (TCP connect phase).
    connect_timeout: std::time::Duration,
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
    "pooler",
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
    /// If the URL contains `pooler=true`, the store uses **direct mode**:
    /// each transaction creates a fresh TCP connection and closes it when
    /// done, bypassing sqlx's connection pool entirely. This is required
    /// for Supabase Pooler / pgbouncer transaction mode to avoid the
    /// "zombie pool" problem.
    ///
    /// Without `pooler=true`, the store uses **pooled mode** (default):
    /// sqlx `PgPool` with `min_connections`, `idle_timeout`, etc.
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
        if let (Some(min), Some(max)) = (config.min_connections, config.max_connections)
            && min > max
        {
            warn!("min_connections={min} > max_connections={max}, capping min to max");
            config.min_connections = Some(max);
        }

        let tune = PgTuneConfig::from_env();

        // URL params override tuning env vars for pool sizing.
        let pool_max = config.max_connections.unwrap_or(tune.pool_max);
        let pool_min = config.min_connections.unwrap_or(tune.pool_min);
        let pool_min = pool_min.min(pool_max);
        let acquire_timeout = config.connect_timeout.unwrap_or(tune.pool_acquire_timeout);
        let idle_timeout = config.idle_timeout.or(Some(tune.pool_idle_timeout));
        let max_lifetime = config.max_lifetime.or(Some(tune.pool_max_lifetime));
        // Connect timeout for direct mode: if user specified connect_timeout,
        // use that; otherwise use a longer default (30s) since direct-mode
        // TCP connect across regions can be slow. Pool acquire_timeout
        // defaults to 10s which is too short for cross-region connect.
        let connect_timeout = config.connect_timeout.unwrap_or_else(|| {
            std::cmp::max(
                tune.pool_acquire_timeout,
                std::time::Duration::from_secs(30),
            )
        });

        let vacuum_sql: Arc<str> = format!("VACUUM ANALYZE {}", config.table_name).into();
        let sql: Arc<Sql> = Arc::new(Sql::new(&config.table_name));
        let begin_sql: Arc<str> =
            format!("BEGIN ISOLATION LEVEL {}", config.isolation_level.as_sql()).into();

        // pool_max == 0 is invalid only in pool mode (direct mode has no pool).
        if pool_max == 0 && !config.pooler {
            return Err(PgStoreError::Other(
                "max_connections must be > 0 in pool mode".to_string(),
            ));
        }

        let slow_acquire = config.slow_acquire_threshold_secs;
        let slow_stmts = config.slow_statements_threshold_secs;

        let mut opts: PgConnectOptions = strip_custom_params(url)
            .parse()
            .map_err(|e: sqlx::Error| PgStoreError::Postgres(format!("invalid URL: {e}")))?;

        if let Some(threshold) = slow_stmts {
            opts = opts.log_slow_statements(tracing::log::LevelFilter::Warn, threshold);
        }

        // Session SQL and TCP keepalive SQL — applied to every new connection
        // in both pooled and direct modes.
        let session_sql: Arc<str> = tune.session_sql().into();
        let keepalive_sql: Arc<str> = format!(
            "SET tcp_keepalives_idle = {idle}; \
             SET tcp_keepalives_interval = {interval}; \
             SET tcp_keepalives_count = {count}",
            idle = tune.keepalive_idle.as_secs(),
            interval = tune.keepalive_interval.as_secs(),
            count = tune.keepalive_count,
        )
        .into();

        // ── Branch: pooler (direct) vs pooled ──
        let mode = if config.pooler {
            info!(
                "pooler=true: using DIRECT connection mode (bypassing sqlx pool). \
                 Each transaction creates a fresh TCP connection."
            );
            ConnectionMode::Direct(Box::new(opts))
        } else {
            // Clone the Arcs before moving into the after_connect closure.
            let session_sql_for_pool = Arc::clone(&session_sql);
            let keepalive_sql_for_pool = Arc::clone(&keepalive_sql);

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
                    let sql = session_sql_for_pool.clone();
                    let ka = keepalive_sql_for_pool.clone();
                    Box::pin(async move {
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
                 connect_timeout={:?}, idle_timeout={:?}, max_lifetime={:?}, slow_acquire_threshold={:?}",
                pool_max,
                pool_min,
                acquire_timeout,
                connect_timeout,
                idle_timeout,
                max_lifetime,
                slow_acquire
            );
            ConnectionMode::Pooled(pool)
        };

        // ── DDL: create table + table tuning ──
        if config.auto_create_table {
            let table = &config.table_name;
            let create_sql = tune.create_table_sql(table);
            let tune_sql = tune.tune_table_sql(table);

            // Execute DDL via a one-off connection (works in both modes).
            match &mode {
                ConnectionMode::Pooled(pool) => {
                    Executor::execute(pool, sqlx::raw_sql(&create_sql))
                        .await
                        .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
                    info!("table '{table}' initialized");
                    match Executor::execute(pool, sqlx::raw_sql(&tune_sql)).await {
                        Ok(_) => info!(
                            "table '{table}' tuning applied (fillfactor={}, toast={}, autovac tuned)",
                            tune.fillfactor, tune.toast_storage
                        ),
                        Err(e) => warn!("table tuning partially failed (non-fatal): {e}"),
                    }
                }
                ConnectionMode::Direct(conn_opts) => {
                    let mut conn =
                        match tokio::time::timeout(connect_timeout, conn_opts.connect()).await {
                            Ok(Ok(c)) => c,
                            Ok(Err(e)) => {
                                return Err(PgStoreError::from_sqlx(None, &e));
                            }
                            Err(_) => {
                                return Err(PgStoreError::ConnectTimeout(connect_timeout));
                            }
                        };
                    // Apply session SQL + keepalive to this DDL connection too.
                    if let Err(e) =
                        Executor::execute(&mut conn, sqlx::raw_sql(&keepalive_sql)).await
                    {
                        warn!(
                            error = %e,
                            "tcp_keepalive SET failed on DDL connection (non-fatal)"
                        );
                    }
                    if let Err(e) = Executor::execute(&mut conn, sqlx::raw_sql(&session_sql)).await
                    {
                        warn!(
                            error = %e,
                            "session SQL failed on DDL connection (non-fatal, but \
                             subsequent transactions may also fail)"
                        );
                    }
                    Executor::execute(&mut conn, sqlx::raw_sql(&create_sql))
                        .await
                        .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
                    info!("table '{table}' initialized");
                    match Executor::execute(&mut conn, sqlx::raw_sql(&tune_sql)).await {
                        Ok(_) => info!(
                            "table '{table}' tuning applied (fillfactor={}, toast={}, autovac tuned)",
                            tune.fillfactor, tune.toast_storage
                        ),
                        Err(e) => warn!("table tuning partially failed (non-fatal): {e}"),
                    }
                    // Connection drops here — DDL is done.
                }
            }
        }

        // ── Log PG server hints ──
        tune.log_server_hints();

        // ── Resolve persistent-statements policy ──
        // In pooler (direct) mode, persistent is always false — pgbouncer
        // transaction mode doesn't support named prepared statements.
        // In pooled mode, Auto resolves to true (direct PG supports them).
        let persistent = if config.pooler {
            info!("pooler mode: persistent-statements forced to false");
            false
        } else {
            match config.persistent_statements {
                crate::config::PersistentStatements::Auto => {
                    // We no longer probe — direct PG (non-pooler) supports
                    // named prepared statements. If the user is behind a
                    // pooler, they should set pooler=true.
                    info!("pooled mode: persistent-statements auto → true (direct PG)");
                    true
                }
                ref p => {
                    let resolved = p.resolve(true);
                    info!(
                        policy = %p,
                        persistent = resolved,
                        "persistent-statements explicitly configured"
                    );
                    resolved
                }
            }
        };

        info!(
            mode = if config.pooler { "direct" } else { "pooled" },
            max_conn = pool_max,
            table = &config.table_name,
            isolation = config.isolation_level.as_sql(),
            persistent,
            "PgStore created"
        );

        Ok(Arc::new(Self {
            mode,
            config: Arc::new(config),
            tune: Arc::new(tune),
            persistent,
            pool_max,
            begin_sql,
            vacuum_sql,
            sql,
            tx_started: Arc::new(AtomicU64::new(0)),
            tx_committed: Arc::new(AtomicU64::new(0)),
            tx_rolled_back: Arc::new(AtomicU64::new(0)),
            tx_active: Arc::new(AtomicU64::new(0)),
            pool_warned: Arc::new(AtomicBool::new(false)),
            session_sql,
            keepalive_sql,
            connect_timeout,
        }))
    }

    /// Begin a new transaction.
    ///
    /// **Pool mode**: Uses sqlx's `pool.begin_with()` to start a transaction
    /// with the configured isolation level. sqlx manages the full transaction
    /// lifecycle (`Transaction::drop` auto-rollbacks). Includes retry logic
    /// with exponential backoff for transient pool exhaustion.
    ///
    /// **Direct mode** (pooler): Creates a fresh TCP connection via
    /// `PgConnectOptions::connect()`, applies session SQL + keepalive, sends
    /// `BEGIN`, and returns a `PgTransaction` wrapping the raw `PgConnection`.
    /// The connection is closed when the transaction is committed/cancelled/
    /// dropped. No retry logic is needed — there is no pool to exhaust.
    pub async fn begin(&self, write: bool) -> Result<PgTransaction> {
        match &self.mode {
            ConnectionMode::Pooled(pool) => self.begin_pooled(pool, write).await,
            ConnectionMode::Direct(opts) => self.begin_direct(opts, write).await,
        }
    }

    /// Pool-mode begin with retry and diagnostics.
    async fn begin_pooled(&self, pool: &PgPool, write: bool) -> Result<PgTransaction> {
        use tracing::warn;

        const BEGIN_MAX_RETRIES: u32 = 2;
        const BASE_DELAY_MS: u64 = 200;
        const MAX_DELAY_MS: u64 = 2000;
        const ZOMBIE_BASE_DELAY_MS: u64 = 5000;
        const ZOMBIE_MAX_DELAY_MS: u64 = 15000;

        let mut attempt = 0;
        let mut zombie_detected = false;
        loop {
            attempt += 1;

            match pool.begin_with(self.begin_sql.to_string()).await {
                Ok(tx) => {
                    self.tx_started.fetch_add(1, AtomicOrdering::Relaxed);
                    self.tx_active.fetch_add(1, AtomicOrdering::Relaxed);
                    return Ok(PgTransaction::new_pooled(
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
                    let (size, idle) = self.pool_size();
                    let active = self.tx_active.load(AtomicOrdering::Relaxed);

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
                            "begin_with() failed — ZOMBIE POOL detected. \
                             Consider using ?pooler=true to switch to direct mode."
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

                    if !pg_err.is_transient() || attempt > BEGIN_MAX_RETRIES {
                        return Err(pg_err);
                    }

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

    /// Direct-mode begin: connect, apply session SQL, send BEGIN.
    async fn begin_direct(&self, opts: &PgConnectOptions, write: bool) -> Result<PgTransaction> {
        // Connect with timeout.
        let mut conn = match tokio::time::timeout(self.connect_timeout, opts.connect()).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => return Err(PgStoreError::from_sqlx(None, &e)),
            Err(_) => return Err(PgStoreError::ConnectTimeout(self.connect_timeout)),
        };

        // Apply keepalive (non-fatal) + session SQL.
        if let Err(e) = Executor::execute(&mut conn, sqlx::raw_sql(&self.keepalive_sql)).await {
            warn!(
                error = %e,
                "tcp_keepalive SET failed (non-fatal, may not be supported by this PG)"
            );
        }
        Executor::execute(&mut conn, sqlx::raw_sql(&self.session_sql))
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        // Send BEGIN.
        Executor::execute(&mut conn, sqlx::raw_sql(&self.begin_sql))
            .await
            .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

        self.tx_started.fetch_add(1, AtomicOrdering::Relaxed);
        self.tx_active.fetch_add(1, AtomicOrdering::Relaxed);

        Ok(PgTransaction::new_direct(
            conn,
            write,
            self.config.isolation_level,
            self.persistent,
            Arc::clone(&self.sql),
            Arc::clone(&self.tx_active),
        ))
    }

    /// Shut down gracefully.
    ///
    /// **Pool mode**: closes the sqlx connection pool (waits for all
    /// connections to be returned, then closes them).
    ///
    /// **Direct mode**: waits for in-flight transactions to finish (up to
    /// 30s), then logs completion. There is no pool to close — each
    /// transaction owns its TCP connection, which is closed when the
    /// transaction commits/cancels/drops.
    pub async fn shutdown(&self) {
        match &self.mode {
            ConnectionMode::Pooled(pool) => {
                pool.close().await;
            }
            ConnectionMode::Direct(_) => {
                let active = self.tx_active.load(AtomicOrdering::Relaxed);
                if active > 0 {
                    info!(
                        tx_active = active,
                        "shutdown: waiting for in-flight direct-mode transactions to complete"
                    );
                    // Wait for active transactions to drain. Each transaction
                    // decrements tx_active on commit/cancel/drop.
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                    loop {
                        let remaining = self.tx_active.load(AtomicOrdering::Relaxed);
                        if remaining == 0 {
                            break;
                        }
                        if std::time::Instant::now() >= deadline {
                            warn!(
                                tx_active = remaining,
                                "shutdown: timed out waiting for direct-mode transactions \
                                 to complete — proceeding with shutdown"
                            );
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
        let active = self.tx_active.load(AtomicOrdering::Relaxed);
        if active > 0 {
            warn!(
                tx_active = active,
                "PgStore shut down with in-flight transactions (they will be \
                 rolled back on connection close/drop)"
            );
        } else {
            info!("PgStore shut down (0 active transactions)");
        }
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
    #[must_use]
    pub fn persistent(&self) -> bool {
        self.persistent
    }

    /// Get the configured maximum pool size.
    ///
    /// In direct mode, this is informational only (no pool exists).
    #[must_use]
    pub fn pool_max(&self) -> u32 {
        self.pool_max
    }

    /// Whether the store is in direct (pooler) mode.
    #[must_use]
    pub fn is_direct_mode(&self) -> bool {
        matches!(self.mode, ConnectionMode::Direct(_))
    }

    /// Run VACUUM ANALYZE on the table (must be called outside a transaction).
    pub async fn vacuum(&self) -> Result<()> {
        match &self.mode {
            ConnectionMode::Pooled(pool) => {
                Executor::execute(pool, sqlx::raw_sql(&self.vacuum_sql))
                    .await
                    .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
            }
            ConnectionMode::Direct(opts) => {
                let mut conn =
                    match tokio::time::timeout(self.connect_timeout, opts.connect()).await {
                        Ok(Ok(c)) => c,
                        Ok(Err(e)) => return Err(PgStoreError::from_sqlx(None, &e)),
                        Err(_) => return Err(PgStoreError::ConnectTimeout(self.connect_timeout)),
                    };
                let _ = Executor::execute(&mut conn, sqlx::raw_sql(&self.keepalive_sql)).await;
                let _ = Executor::execute(&mut conn, sqlx::raw_sql(&self.session_sql)).await;
                Executor::execute(&mut conn, sqlx::raw_sql(&self.vacuum_sql))
                    .await
                    .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
                // Connection drops here.
            }
        }
        info!("VACUUM ANALYZE {} completed", self.config.table_name);
        Ok(())
    }

    /// Execute a lightweight health check (`SELECT 1`).
    ///
    /// Suitable for Kubernetes liveness/readiness probes.
    pub async fn health_check(&self) -> Result<()> {
        match &self.mode {
            ConnectionMode::Pooled(pool) => {
                Executor::execute(pool, sqlx::raw_sql("SELECT 1"))
                    .await
                    .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
            }
            ConnectionMode::Direct(opts) => {
                let mut conn =
                    match tokio::time::timeout(self.connect_timeout, opts.connect()).await {
                        Ok(Ok(c)) => c,
                        Ok(Err(e)) => return Err(PgStoreError::from_sqlx(None, &e)),
                        Err(_) => return Err(PgStoreError::ConnectTimeout(self.connect_timeout)),
                    };
                // Apply keepalive + session SQL for consistency with begin_direct/vacuum.
                let _ = Executor::execute(&mut conn, sqlx::raw_sql(&self.keepalive_sql)).await;
                let _ = Executor::execute(&mut conn, sqlx::raw_sql(&self.session_sql)).await;
                Executor::execute(&mut conn, sqlx::raw_sql("SELECT 1"))
                    .await
                    .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
            }
        }
        Ok(())
    }

    /// Return the current pool size info.
    ///
    /// In direct mode, returns `(0, 0)` — there is no pool.
    #[must_use]
    pub fn pool_size(&self) -> (u32, u32) {
        match &self.mode {
            ConnectionMode::Pooled(pool) => (pool.size(), pool.num_idle() as u32),
            ConnectionMode::Direct(_) => (0, 0),
        }
    }

    /// O3: Check pool utilization and warn once if it exceeds 80%.
    ///
    /// Only meaningful in pooled mode. No-op in direct mode.
    pub(crate) fn check_pool_utilization(&self) {
        let (size, _idle) = self.pool_size();
        if size == 0 {
            return; // direct mode or empty pool
        }
        if size as u64 * 5 > self.pool_max as u64 * 4
            && !self.pool_warned.swap(true, AtomicOrdering::Relaxed)
        {
            warn!(
                pool_size = size,
                pool_max = self.pool_max,
                utilization_pct = (size as f64 / self.pool_max as f64 * 100.0) as u32,
                "connection pool utilization exceeds 80% — consider increasing max_connections"
            );
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
    /// In direct mode, this is a no-op.
    pub fn try_resize_pool(&self, max: u32, min: u32) -> Result<()> {
        if max < min {
            return Err(PgStoreError::Other(format!(
                "max_connections ({max}) must be >= min_connections ({min})"
            )));
        }
        match &self.mode {
            ConnectionMode::Pooled(_) => {
                info!(
                    "pool resize requested: max={max}, min={min} (not yet supported by sqlx 0.8)"
                );
            }
            ConnectionMode::Direct(_) => {
                info!("pool resize requested in direct mode (no-op)");
            }
        }
        Ok(())
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
