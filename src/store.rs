//! PgStore — the datastore / factory layer that holds a PG connection pool
//! (or direct connection options) and spawns [`PgTransaction`] instances.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

use sqlx::ConnectOptions;
use sqlx::Executor;
use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

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
/// `Pooled` variant clones cheaply (PgPool is Arc-wrapped), while `Direct`
/// clones the boxed PgConnectOptions (one heap allocation). Both are safe;
/// this derive is intentional for PgStore::clone().
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
    /// Barrier flag: set by `shutdown()` to prevent new `begin()` calls.
    shutting_down: Arc<AtomicBool>,
    /// Session SQL applied to every new connection (both modes).
    session_sql: Arc<str>,
    /// TCP keepalive SQL applied to every new connection (both modes).
    keepalive_sql: Arc<str>,
    /// Connect timeout for direct-mode connections (TCP connect phase).
    connect_timeout: std::time::Duration,
    /// Cancellation token for the direct-mode background heartbeat task.
    /// `None` in pool mode (no heartbeat task spawned).
    heartbeat_cancel: Option<CancellationToken>,
    /// JoinHandle for the direct-mode background heartbeat task.
    /// Stored so `shutdown()` can await the task's graceful exit.
    /// `None` in pool mode (no heartbeat task spawned).
    /// Arc<tokio::sync::Mutex<..>> so the field is Clone (PgStore derives Clone)
    /// and the guard is Send (required for async shutdown).
    heartbeat_handle: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Heartbeat interval for direct-mode background keepalive.
    /// Uses `keepalive_idle` value (default 60s) — probes just before
    /// the pooler would drop idle connections.
    heartbeat_interval: std::time::Duration,
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
    "hash_partitions",
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

/// Verify that the actual partition count matches the configured value.
///
/// Hash partition count is immutable in PostgreSQL — once a table is created
/// with N partitions, it cannot be changed without dropping and recreating
/// the table. This function checks the result of `partition_count_sql()`
/// and returns a fatal error if there is a mismatch.
///
/// - `expected == 1` (no partitioning): actual must be 0 (unpartitioned table).
/// - `expected > 1` (hash partitioning): actual must equal `expected`.
fn verify_partition_count(table: &str, expected: u32, actual: i64) -> Result<()> {
    let expected_i = expected as i64;
    if expected <= 1 {
        // No partitioning expected — table should be unpartitioned (0 child partitions).
        if actual > 0 {
            return Err(PgStoreError::Other(format!(
                "hash_partitions mismatch: table '{table}' has {actual} existing hash \
                 partitions but configuration expects no partitioning (hash_partitions=1). \
                 Hash partition count is immutable — to change it, drop and recreate the \
                 table, or update hash_partitions to {actual} to match the existing table."
            )));
        }
        Ok(())
    } else {
        // Hash partitioning expected — actual must match.
        if actual != expected_i {
            if actual == 0 {
                return Err(PgStoreError::Other(format!(
                    "hash_partitions mismatch: table '{table}' is not partitioned \
                     but configuration expects hash_partitions={expected}. \
                     Hash partition count is immutable — to use partitioning, drop \
                     and recreate the table."
                )));
            }
            return Err(PgStoreError::Other(format!(
                "hash_partitions mismatch: table '{table}' has {actual} existing hash \
                 partitions but configuration expects hash_partitions={expected}. \
                 Hash partition count is immutable — to change it, drop and recreate \
                 the table, or update hash_partitions to {actual} to match the existing table."
            )));
        }
        Ok(())
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

        // F4: Defense-in-depth — validate table_name after all config
        // merging is done. `merge_url_params` validates on input, but
        // `merge_env` could theoretically override it (currently it doesn't
        // set table_name, but future code might). This assert catches any
        // path that could produce an invalid identifier.
        if let Err(e) = PgConfig::validate_identifier(&config.table_name) {
            return Err(PgStoreError::Other(format!(
                "table_name '{name}' is invalid after config merge: {e}",
                name = config.table_name
            )));
        }

        // Post-merge cross-validation: min_connections must not exceed max_connections.
        if let (Some(min), Some(max)) = (config.min_connections, config.max_connections)
            && min > max
        {
            warn!("min_connections={min} > max_connections={max}, capping min to max");
            config.min_connections = Some(max);
        }

        let tune = PgTuneConfig::from_env();

        // URL param `hash_partitions` overrides the env var
        // `PG_TUNED_TABLE_HASH_PARTITIONS`. This is consistent with how
        // pool sizing params work (URL > env > default).
        let mut tune = tune;
        if let Some(hp) = config.hash_partitions {
            if hp == 0 {
                warn!("hash_partitions=0 from URL is invalid, using env/default");
            } else if hp > 1024 {
                warn!(
                    "hash_partitions={hp} from URL is unreasonably high (>1024), using env/default"
                );
            } else {
                if tune.hash_partitions != hp {
                    info!(
                        env_value = tune.hash_partitions,
                        url_value = hp,
                        "hash_partitions overridden by URL parameter"
                    );
                }
                tune.hash_partitions = hp;
            }
        }

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
        // In pool mode this is unused (Duration::ZERO placeholder).
        let connect_timeout = if config.pooler {
            config.connect_timeout.unwrap_or_else(|| {
                std::cmp::max(
                    tune.pool_acquire_timeout,
                    std::time::Duration::from_secs(30),
                )
            })
        } else {
            // Pool mode: connect_timeout is not used — acquire_timeout
            // governs connection acquisition (including establishment).
            std::time::Duration::ZERO
        };

        let vacuum_sql: Arc<str> = format!("VACUUM ANALYZE {}", config.table_name).into();
        let sql: Arc<Sql> = Arc::new(Sql::new(&config.table_name));
        let begin_sql: Arc<str> =
            format!("BEGIN ISOLATION LEVEL {}", config.isolation_level.as_sql()).into();

        // pool_max == 0 is invalid only in pool mode (direct mode has no pool).
        // Defense-in-depth: even though tune.from_env() and merge_url_params()
        // both reject 0, we guard here too — PgPoolOptions::max_connections(0)
        // would panic inside sqlx.
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
        // F4: Format keepalive Duration values for PG's tcp_keepalives_*
        // parameters, which only accept integer seconds. Use `as_secs()`
        // which truncates sub-second components, but guard against 0s
        // (sub-second durations like 500ms would truncate to 0 which is
        // invalid — PG would reject or ignore it). Floor at 1s.
        let fmt_keepalive_secs = |d: std::time::Duration| -> String {
            let secs = d.as_secs();
            if secs == 0 && !d.is_zero() {
                // Sub-second value truncated to 0 — use minimum 1s
                "1".to_string()
            } else {
                secs.to_string()
            }
        };
        let keepalive_sql: Arc<str> = format!(
            "SET tcp_keepalives_idle = {idle}; \
             SET tcp_keepalives_interval = {interval}; \
             SET tcp_keepalives_count = {count}",
            idle = fmt_keepalive_secs(tune.keepalive_idle),
            interval = fmt_keepalive_secs(tune.keepalive_interval),
            count = tune.keepalive_count,
        )
        .into();

        // ── Direct-mode heartbeat task ──
        //
        // In pooler (direct) mode, we spawn a background task that maintains
        // a long-lived TCP connection to PG and periodically executes `SELECT 1`.
        // This serves two purposes:
        // 1. Keep the connection path alive — Supabase Pooler typically drops
        //    idle connections after ~60s. The heartbeat probes at the same
        //    cadence as TCP keepalive (keepalive_idle), preventing the pooler
        //    from reclaiming the connection.
        // 2. Early detection of PG reachability issues — if the heartbeat
        //    fails, we log a warning and attempt to reconnect. This gives
        //    operators visibility into connectivity problems before they
        //    cascade to transaction failures.
        let (heartbeat_cancel, heartbeat_handle) = if config.pooler {
            let cancel = CancellationToken::new();
            let interval = tune.keepalive_idle;
            // Clone the parsed PgConnectOptions for the heartbeat task.
            // We must re-parse because `opts` is moved into `ConnectionMode`
            // below. The options are Arc-wrapped internally, so cloning is
            // cheap (no deep copy of the URL string).
            let opts_hb: PgConnectOptions = strip_custom_params(url)
                .parse()
                .map_err(|e: sqlx::Error| PgStoreError::Postgres(format!("invalid URL: {e}")))?;
            let opts_boxed = Box::new(opts_hb);
            let keepalive_sql_hb = Arc::clone(&keepalive_sql);
            let session_sql_hb = Arc::clone(&session_sql);
            let cancel_clone = cancel.clone();

            let heartbeat_handle = tokio::spawn(direct_mode_heartbeat(
                opts_boxed,
                connect_timeout,
                interval,
                keepalive_sql_hb,
                session_sql_hb,
                cancel_clone,
            ));

            info!(
                "direct-mode heartbeat task spawned: interval={:?}, connect_timeout={:?}",
                interval, connect_timeout
            );
            (Some(cancel), Some(heartbeat_handle))
        } else {
            (None, None)
        };
        let heartbeat_interval = tune.keepalive_idle;

        // ── Branch: pooler (direct) vs pooled ──
        let mode = if config.pooler {
            info!(
                "pooler=true: using DIRECT connection mode (bypassing sqlx pool). \
                 Each transaction creates a fresh TCP connection. \
                 connect_timeout={:?}",
                connect_timeout
            );
            ConnectionMode::Direct(Box::new(opts))
        } else {
            // Pool mode: warn if behind pgbouncer in transaction mode without
            // pooler=true — session SQL (statement_timeout, lock_timeout, etc.)
            // set in after_connect will be silently lost when pgbouncer
            // reassigns the connection to a different transaction.
            warn!(
                "pool mode active: if this server is behind pgbouncer (transaction mode), \
                 session SQL (statement_timeout, lock_timeout, keepalive) set in after_connect \
                 will be silently lost on transaction boundaries. Use ?pooler=true to switch \
                 to direct mode where each transaction gets a fresh connection."
            );
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
                        if let Err(e) = Executor::execute(conn, sqlx::raw_sql(&sql)).await {
                            warn!(
                                error = %e,
                                "session SQL failed in after_connect (non-fatal, but \
                                 statement_timeout/lock_timeout will not be set)"
                            );
                            return Err(e);
                        }
                        Ok(())
                    })
                })
                .before_acquire({
                    // Use keepalive_idle as the ping threshold: if a connection
                    // has been idle longer than keepalive_idle (default 60s),
                    // ping it to verify the server is still reachable before
                    // handing it to a caller. This aligns with the direct-mode
                    // heartbeat interval which also uses keepalive_idle.
                    let ping_threshold = tune.keepalive_idle;
                    move |conn, meta| {
                        Box::pin(async move {
                            if meta.idle_for > ping_threshold {
                                sqlx::Connection::ping(conn).await?;
                            }
                            Ok(true)
                        })
                    }
                })
                .connect_with(opts)
                .await
                .map_err(|e| PgStoreError::from_sqlx(None, &e))?;

            info!(
                "connection pool created: max={}, min={}, acquire_timeout={:?}, \
                 idle_timeout={:?}, max_lifetime={:?}, slow_acquire_threshold={:?}",
                pool_max, pool_min, acquire_timeout, idle_timeout, max_lifetime, slow_acquire
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
                    let mut conn = connect_direct_with_session(
                        conn_opts,
                        connect_timeout,
                        &keepalive_sql,
                        &session_sql,
                        "DDL",
                    )
                    .await?;
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

            // ── Partition count verification ──
            //
            // Hash partition count is immutable in PostgreSQL. If the table
            // already existed (CREATE TABLE IF NOT EXISTS was a no-op) with a
            // different partition count, we must detect and report it here.
            //
            // This runs after the DDL block (both pooled and direct modes).
            let check_sql = tune.partition_count_sql(table);
            let expected = tune.hash_partitions;
            match &mode {
                ConnectionMode::Pooled(pool) => {
                    let row = sqlx::query(&check_sql)
                        .bind(table)
                        .fetch_one(pool)
                        .await
                        .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
                    let actual: i64 = row.get("part_cnt");
                    verify_partition_count(table, expected, actual)?;
                }
                ConnectionMode::Direct(conn_opts) => {
                    let mut conn = connect_direct(conn_opts, connect_timeout).await?;
                    let row = sqlx::query(&check_sql)
                        .bind(table)
                        .fetch_one(&mut conn)
                        .await
                        .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
                    let actual: i64 = row.get("part_cnt");
                    verify_partition_count(table, expected, actual)?;
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
            if matches!(
                config.persistent_statements,
                crate::config::PersistentStatements::Enabled
            ) {
                warn!(
                    "pooler mode: persistent-statements=Enabled is not compatible with \
                     pgbouncer transaction mode — forced to false"
                );
            } else {
                info!("pooler mode: persistent-statements forced to false");
            }
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
            shutting_down: Arc::new(AtomicBool::new(false)),
            session_sql,
            keepalive_sql,
            connect_timeout,
            heartbeat_cancel,
            heartbeat_handle: Arc::new(tokio::sync::Mutex::new(heartbeat_handle)),
            heartbeat_interval,
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
        // Barrier: refuse new transactions after shutdown() has been called.
        // R22-F1: Use Acquire ordering — pairs with Release store in shutdown()
        // to ensure the barrier is visible on weak-memory architectures (ARM).
        if self.shutting_down.load(AtomicOrdering::Acquire) {
            return Err(PgStoreError::Other(
                "PgStore is shutting down — new transactions are refused".to_string(),
            ));
        }
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

            // Re-check shutdown barrier on each retry attempt — if shutdown()
            // was called during the backoff sleep, don't waste resources
            // creating a new connection. R22-F1: Acquire ordering pairs with
            // Release store in shutdown() for correct cross-thread visibility.
            if self.shutting_down.load(AtomicOrdering::Acquire) {
                return Err(PgStoreError::Other(
                    "PgStore is shutting down — new transactions are refused".to_string(),
                ));
            }

            // begin_with() requires Cow<'static, str> — we must own the
            // string, so to_string() is unavoidable here (Arc<str> → String).
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
    ///
    /// Unlike the initial implementation, this now includes retry logic
    /// with exponential backoff for transient connection failures. This
    /// mirrors the retry strategy in `begin_pooled()` — behind a pooler
    /// (Supabase Pooler / pgbouncer), transient TCP connect failures are
    /// common (pooler connection limit reached, brief network blips,
    /// pooler restarting, etc.) and retrying typically succeeds.
    async fn begin_direct(&self, opts: &PgConnectOptions, write: bool) -> Result<PgTransaction> {
        const MAX_RETRIES: u32 = 2;
        const BASE_DELAY_MS: u64 = 500;
        const MAX_DELAY_MS: u64 = 5000;

        let mut attempt = 0;
        loop {
            attempt += 1;

            // Re-check shutdown barrier on each retry attempt — if shutdown()
            // was called during the backoff sleep, don't waste resources
            // creating a new connection. R22-F1: Acquire ordering.
            if self.shutting_down.load(AtomicOrdering::Acquire) {
                return Err(PgStoreError::Other(
                    "PgStore is shutting down — new transactions are refused".to_string(),
                ));
            }

            match connect_direct(opts, self.connect_timeout).await {
                Ok(mut conn) => {
                    // Apply keepalive (non-fatal).
                    if let Err(e) =
                        Executor::execute(&mut conn, sqlx::raw_sql(&self.keepalive_sql)).await
                    {
                        warn!(
                            error = %e,
                            "tcp_keepalive SET failed (non-fatal, may not be supported by this PG)"
                        );
                    }
                    // Apply session SQL — fatal on failure (unlike vacuum/health_check
                    // which tolerate missing timeouts). begin_direct is on the hot path;
                    // if session SQL fails, subsequent transactions will also fail.
                    if let Err(e) =
                        Executor::execute(&mut conn, sqlx::raw_sql(&self.session_sql)).await
                    {
                        warn!(
                            error = %e,
                            "session SQL failed on direct-mode connection — \
                             statement_timeout/lock_timeout will not be set for this transaction"
                        );
                        let pg_err = PgStoreError::from_sqlx(None, &e);
                        if pg_err.is_transient() && attempt <= MAX_RETRIES {
                            let delay = (BASE_DELAY_MS * 2u64.pow(attempt - 1)).min(MAX_DELAY_MS);
                            warn!(
                                attempt,
                                max_retries = MAX_RETRIES,
                                delay_ms = delay,
                                "session SQL failed (transient) — retrying"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                            continue;
                        }
                        return Err(pg_err);
                    }

                    // Send BEGIN.
                    match Executor::execute(&mut conn, sqlx::raw_sql(&self.begin_sql)).await {
                        Ok(_) => {}
                        Err(e) => {
                            // Best-effort ROLLBACK: if BEGIN was sent but PG returned
                            // an error, PG may or may not be in a transaction. Send
                            // ROLLBACK to ensure clean state before dropping the
                            // connection (in pooler environments, the server-side
                            // connection may be reused).
                            if let Err(rb_err) =
                                Executor::execute(&mut conn, sqlx::raw_sql("ROLLBACK")).await
                            {
                                debug!(
                                    error = %rb_err,
                                    "best-effort ROLLBACK after BEGIN failure (ignored)"
                                );
                            }
                            let pg_err = PgStoreError::from_sqlx(None, &e);
                            if pg_err.is_transient() && attempt <= MAX_RETRIES {
                                let delay =
                                    (BASE_DELAY_MS * 2u64.pow(attempt - 1)).min(MAX_DELAY_MS);
                                warn!(
                                    attempt,
                                    max_retries = MAX_RETRIES,
                                    delay_ms = delay,
                                    error = %pg_err,
                                    "BEGIN failed (transient) — retrying"
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                                continue;
                            }
                            return Err(pg_err);
                        }
                    }

                    self.tx_started.fetch_add(1, AtomicOrdering::Relaxed);
                    self.tx_active.fetch_add(1, AtomicOrdering::Relaxed);

                    return Ok(PgTransaction::new_direct(
                        conn,
                        write,
                        self.config.isolation_level,
                        self.persistent,
                        Arc::clone(&self.sql),
                        Arc::clone(&self.tx_active),
                    ));
                }
                Err(e) => {
                    if e.is_transient() && attempt <= MAX_RETRIES {
                        let delay = (BASE_DELAY_MS * 2u64.pow(attempt - 1)).min(MAX_DELAY_MS);
                        warn!(
                            attempt,
                            max_retries = MAX_RETRIES,
                            delay_ms = delay,
                            error = %e,
                            "direct-mode TCP connect failed (transient) — retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Shut down gracefully.
    ///
    /// **Pool mode**: closes the sqlx connection pool (waits for all
    /// connections to be returned, then closes them) with a 30s timeout.
    ///
    /// **Direct mode**: sets the shutdown barrier, cancels the heartbeat
    /// task, and gives in-flight transactions a short 3s grace period to
    /// finish naturally. SurrealDB's shutdown sequence calls `shutdown()`
    /// *before* dropping in-flight transactions, so we cannot wait for
    /// `tx_active == 0` (that would deadlock). When transactions are later
    /// dropped by SurrealDB, the `Drop` impl closes each TCP connection
    /// and PG aborts the transaction.
    pub async fn shutdown(&self) {
        // Set barrier to prevent new begin() calls.
        // R22-F1: Use Release ordering — pairs with Acquire loads in begin()
        // to ensure the barrier is visible on weak-memory architectures (ARM).
        self.shutting_down.store(true, AtomicOrdering::Release);

        // Cancel the direct-mode heartbeat task (if any) and await its exit.
        if let Some(cancel) = &self.heartbeat_cancel {
            cancel.cancel();
            info!("direct-mode heartbeat task cancellation requested");
        }
        if let Some(handle) = self.heartbeat_handle.lock().await.take() {
            // The heartbeat task checks cancellation in tokio::select! branches,
            // so it should exit promptly. But if a SELECT 1 or connect is in
            // progress, it may take up to ~10s (HEARTBEAT_TIMEOUT). Give it
            // 15s to finish before giving up.
            match tokio::time::timeout(std::time::Duration::from_secs(15), handle).await {
                Ok(Ok(())) => info!("direct-mode heartbeat task exited cleanly"),
                Ok(Err(e)) => warn!(error = %e, "direct-mode heartbeat task exited with error"),
                Err(_) => warn!("direct-mode heartbeat task did not exit within 15s — proceeding"),
            }
        }

        match &self.mode {
            ConnectionMode::Pooled(pool) => {
                // Close the pool with a 30s timeout. Without this, pool.close()
                // can hang indefinitely if a connection is stuck (e.g. TCP
                // half-open). Direct mode already has a 30s deadline.
                match tokio::time::timeout(std::time::Duration::from_secs(30), pool.close()).await {
                    Ok(()) => {}
                    Err(_) => {
                        warn!("pool.close() timed out after 30s — proceeding with shutdown");
                    }
                }
            }
            ConnectionMode::Direct(_) => {
                // SurrealDB's shutdown sequence calls our shutdown() *before*
                // dropping in-flight transactions. So waiting for tx_active==0
                // is a deadlock: shutdown() waits for the transactions to drain,
                // but they can only drain after shutdown() returns and SurrealDB
                // proceeds to drop them.
                //
                // Instead, give a short grace period (3s) for any transactions
                // that are *truly* in-flight (mid-query) to finish naturally.
                // When transactions are later dropped by SurrealDB, the Drop
                // impl closes the TCP connection and PG aborts the transaction.
                let active = self.tx_active.load(AtomicOrdering::Relaxed);
                if active > 0 {
                    info!(
                        tx_active = active,
                        "shutdown: direct mode with in-flight transactions — \
                         giving 3s grace period before proceeding"
                    );
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
                    loop {
                        let remaining = self.tx_active.load(AtomicOrdering::Relaxed);
                        if remaining == 0 {
                            break;
                        }
                        if std::time::Instant::now() >= deadline {
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

    /// Get the heartbeat interval for direct-mode background keepalive.
    ///
    /// Returns the `keepalive_idle` value (default 60s). In direct mode,
    /// this is the interval at which the background heartbeat task probes
    /// PG with `SELECT 1`. In pool mode, this value is informational.
    #[must_use]
    pub fn heartbeat_interval(&self) -> std::time::Duration {
        self.heartbeat_interval
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
                // F4: VACUUM operates outside a transaction and doesn't need
                // session SQL (statement_timeout/lock_timeout). Using
                // connect_direct instead of connect_direct_with_session
                // saves 2 network round-trips (keepalive + session SQL SETs).
                let mut conn = connect_direct(opts, self.connect_timeout).await?;
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
    ///
    /// In direct mode, includes a single retry on transient connection
    /// failures (e.g. brief network blip) to prevent K8s from restarting
    /// the Pod due to a one-off timeout.
    pub async fn health_check(&self) -> Result<()> {
        match &self.mode {
            ConnectionMode::Pooled(pool) => {
                Executor::execute(pool, sqlx::raw_sql("SELECT 1"))
                    .await
                    .map_err(|e| PgStoreError::from_sqlx(None, &e))?;
            }
            ConnectionMode::Direct(opts) => {
                // F4: health_check uses connect_direct (no session SQL) —
                // keepalive + session SQL SETs add 2 extra network round-trips
                // which is wasteful for a lightweight liveness probe called
                // every ~10s by Kubernetes. A bare TCP connect + SELECT 1 is
                // sufficient to verify PG reachability.
                //
                // R21-F5: Retry once on transient errors — K8s liveness probe
                // failures trigger Pod restart, which is disproportionately
                // expensive compared to a second connect attempt. A single
                // retry with a short delay catches transient network blips
                // without adding significant latency to the steady-state case.
                let mut attempt = 0;
                const MAX_ATTEMPTS: u32 = 2;
                loop {
                    attempt += 1;
                    match connect_direct(opts, self.connect_timeout).await {
                        Ok(mut conn) => {
                            if let Err(e) =
                                Executor::execute(&mut conn, sqlx::raw_sql("SELECT 1")).await
                            {
                                let pg_err = PgStoreError::from_sqlx(None, &e);
                                if pg_err.is_transient() && attempt < MAX_ATTEMPTS {
                                    warn!(
                                        attempt,
                                        max_attempts = MAX_ATTEMPTS,
                                        error = %pg_err,
                                        "health_check SELECT 1 failed (transient) — retrying"
                                    );
                                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                    continue;
                                }
                                return Err(pg_err);
                            }
                            return Ok(());
                        }
                        Err(e) => {
                            if e.is_transient() && attempt < MAX_ATTEMPTS {
                                warn!(
                                    attempt,
                                    max_attempts = MAX_ATTEMPTS,
                                    error = %e,
                                    "health_check connect failed (transient) — retrying"
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                continue;
                            }
                            return Err(e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Return the current pool size info.
    ///
    /// In direct mode, returns `(0, 0)` — there is no pool.
    #[must_use]
    pub fn pool_size(&self) -> (u32, u64) {
        match &self.mode {
            ConnectionMode::Pooled(pool) => (pool.size(), pool.num_idle() as u64),
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

// ─── Direct-mode connection helpers ────────────────

/// Open a direct-mode TCP connection with timeout.
///
/// This is the low-level shared core of all direct-mode operations.
/// Most call sites should prefer [`connect_direct_with_session`] which
/// also applies keepalive + session SQL. Only use this helper directly
/// when you need full control over session setup (e.g. `begin_direct`
/// treats session SQL failure as fatal, partition verification skips it).
///
/// # Errors
///
/// Returns `PgStoreError::Other` if `connect_timeout` is zero (this is
/// a programming error — pool mode sets connect_timeout to ZERO and
/// must never call this function). In debug builds, this also panics.
async fn connect_direct(
    opts: &PgConnectOptions,
    connect_timeout: std::time::Duration,
) -> Result<sqlx::postgres::PgConnection> {
    if connect_timeout.is_zero() {
        const MSG: &str =
            "connect_direct: connect_timeout must be > 0 (pool mode sets it to ZERO — caller bug)";
        debug_assert!(false, "{MSG}");
        return Err(PgStoreError::Other(MSG.to_string()));
    }
    match tokio::time::timeout(connect_timeout, opts.connect()).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(PgStoreError::from_sqlx(None, &e)),
        Err(_) => {
            warn!(
                timeout_secs = connect_timeout.as_secs(),
                "direct-mode TCP connect timed out"
            );
            Err(PgStoreError::ConnectTimeout(connect_timeout))
        }
    }
}

/// Open a direct-mode connection and apply keepalive + session SQL.
///
/// Keeps the connection ready for use — keepalive and session SQL failures
/// are logged as warnings (non-fatal) since the connection itself is
/// functional without them. `label` is used in log messages to identify
/// which operation triggered the connection (e.g. "DDL", "VACUUM",
/// "health-check").
async fn connect_direct_with_session(
    opts: &PgConnectOptions,
    connect_timeout: std::time::Duration,
    keepalive_sql: &str,
    session_sql: &str,
    label: &str,
) -> Result<sqlx::postgres::PgConnection> {
    let mut conn = connect_direct(opts, connect_timeout).await?;
    if let Err(e) = Executor::execute(&mut conn, sqlx::raw_sql(keepalive_sql)).await {
        warn!(
            error = %e,
            "tcp_keepalive SET failed on {label} connection (non-fatal, \
             may not be supported by this PG)"
        );
    }
    if let Err(e) = Executor::execute(&mut conn, sqlx::raw_sql(session_sql)).await {
        warn!(
            error = %e,
            "session SQL failed on {label} connection (non-fatal, but \
             statement_timeout/lock_timeout will not be set)"
        );
    }
    Ok(conn)
}

/// Background heartbeat task for direct-mode connections.
///
/// Maintains a long-lived TCP connection to PG and periodically executes
/// `SELECT 1` to keep the connection path alive. This prevents Supabase
/// Pooler / pgbouncer from dropping idle connections between transactions
/// and provides early detection of PG reachability issues.
///
/// On heartbeat failure, the connection is dropped and re-established.
/// Continuous failures are logged with exponential backoff to avoid log
/// flooding.
async fn direct_mode_heartbeat(
    opts: Box<PgConnectOptions>,
    connect_timeout: std::time::Duration,
    interval: std::time::Duration,
    keepalive_sql: Arc<str>,
    session_sql: Arc<str>,
    cancel: CancellationToken,
) {
    let mut conn: Option<sqlx::postgres::PgConnection> = None;
    let mut consecutive_failures: u32 = 0;

    loop {
        // Wait for the next heartbeat interval (or cancellation).
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("direct-mode heartbeat task shutting down");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }

        // If cancellation was requested during the sleep, exit.
        if cancel.is_cancelled() {
            info!("direct-mode heartbeat task shutting down");
            return;
        }

        // Try to establish a connection if we don't have one.
        // Use tokio::select! with cancel so shutdown doesn't have to wait
        // for a potentially 30s connect_timeout to elapse.
        if conn.is_none() {
            let connect_fut = connect_direct_with_session(
                &opts,
                connect_timeout,
                &keepalive_sql,
                &session_sql,
                "heartbeat",
            );
            tokio::pin!(connect_fut);
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("direct-mode heartbeat task shutting down during connect");
                    return;
                }
                result = &mut connect_fut => {
                    match result {
                        Ok(c) => {
                            conn = Some(c);
                            if consecutive_failures > 0 {
                                info!(
                                    consecutive_failures,
                                    "direct-mode heartbeat: connection re-established after failures"
                                );
                            }
                            consecutive_failures = 0;
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            warn!(
                                error = %e,
                                consecutive_failures,
                                "direct-mode heartbeat: failed to connect — PG may be unreachable"
                            );
                            // Exponential backoff on repeated failures: min(consecutive^2 * 5s, 5min).
                            // Use saturating arithmetic to prevent theoretical u64 overflow
                            // at very high failure counts (debug-build panic / release wrap).
                            let backoff_secs = (consecutive_failures as u64)
                                .saturating_pow(2)
                                .saturating_mul(5)
                                .min(300);
                            tokio::select! {
                                _ = cancel.cancelled() => {
                                    info!("direct-mode heartbeat task shutting down during backoff");
                                    return;
                                }
                                _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
                            }
                            continue;
                        }
                    }
                }
            }
        }

        // Execute the heartbeat query with a 10s timeout.
        // Without this, a TCP half-open connection to an unreachable PG could
        // hang indefinitely until the OS TCP keepalive fires (potentially minutes).
        // The tokio::select! with cancel still ensures shutdown doesn't block.
        const HEARTBEAT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

        if let Some(mut c) = conn.take() {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("direct-mode heartbeat task shutting down during SELECT 1");
                    return;
                }
                result = tokio::time::timeout(HEARTBEAT_TIMEOUT, Executor::execute(&mut c, sqlx::raw_sql("SELECT 1"))) => {
                    match result {
                        Ok(Ok(_)) => {
                            if consecutive_failures > 0 {
                                info!("direct-mode heartbeat: SELECT 1 succeeded after failures");
                            }
                            consecutive_failures = 0;
                            conn = Some(c); // Put the healthy connection back.
                        }
                        Ok(Err(e)) => {
                            consecutive_failures += 1;
                            warn!(
                                error = %e,
                                consecutive_failures,
                                "direct-mode heartbeat: SELECT 1 failed — dropping connection"
                            );
                            // c is dropped here — broken connection closed.
                        }
                        Err(_elapsed) => {
                            consecutive_failures += 1;
                            warn!(
                                consecutive_failures,
                                "direct-mode heartbeat: SELECT 1 timed out after 10s — dropping connection"
                            );
                            // c is dropped here — hung connection closed.
                        }
                    }
                }
            }
        }
    }
}

// ─── Tests ──────────────────────────────────────────────

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

    #[test]
    fn strip_hash_partitions() {
        assert_eq!(
            strip_custom_params("postgresql://u:p@h/db?hash_partitions=4&sslmode=require"),
            "postgresql://u:p@h/db?sslmode=require"
        );
    }
}

#[cfg(test)]
mod test_partition {
    use super::*;

    #[test]
    fn test_verify_no_partition_expected_none() {
        // expected=1 (no partitioning), actual=0 → OK
        assert!(verify_partition_count("kv", 1, 0).is_ok());
    }

    #[test]
    fn test_verify_no_partition_expected_none_borderline() {
        // expected=0 is treated like 1 (no partitioning)
        assert!(verify_partition_count("kv", 0, 0).is_ok());
    }

    #[test]
    fn test_verify_mismatch_expected_none_actual_partitioned() {
        // expected=1 but table has 4 partitions → error
        let result = verify_partition_count("kv", 1, 4);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("mismatch"));
        assert!(msg.contains("hash_partitions=1"));
    }

    #[test]
    fn test_verify_partitioned_match() {
        // expected=4, actual=4 → OK
        assert!(verify_partition_count("kv", 4, 4).is_ok());
    }

    #[test]
    fn test_verify_partitioned_mismatch() {
        // expected=4 but actual=8 → error
        let result = verify_partition_count("kv", 4, 8);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("mismatch"));
        assert!(msg.contains("hash_partitions=4"));
        assert!(msg.contains("8"));
    }

    #[test]
    fn test_verify_partitioned_but_table_not_partitioned() {
        // expected=4 but actual=0 (not partitioned) → error
        let result = verify_partition_count("kv", 4, 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not partitioned"));
        assert!(msg.contains("hash_partitions=4"));
    }
}
