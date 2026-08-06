//! PostgreSQL tuning configuration — 5-layer, 26-parameter system.
//!
//! All parameters have sensible defaults and can be overridden via
//! `PG_TUNED_*` environment variables. See `PostgreSQL_KV存储层调优方案.docx`
//! for the full design document.
//!
//! ## Layers
//!
//! | Layer | Params | Env prefix | Applied via |
//! |-------|--------|------------|-------------|
//! | PG server | 8 | `PG_TUNED_SERVER_` | session SET + log hints |
//! | Pool | 5 | `PG_TUNED_POOL_` | `PgPoolOptions` at startup |
//! | Table storage | 4 | `PG_TUNED_TABLE_` | DDL (`ALTER TABLE`) |
//! | Autovacuum | 5 | `PG_TUNED_AUTOVAC_` | DDL (`ALTER TABLE`) |
//! | Query runtime | 4 | `PG_TUNED_QUERY_` | session SET (`after_connect`) |

use std::time::Duration;

use tracing::{info, warn};

/// All tuning parameters for the PostgreSQL KV storage backend.
#[derive(Clone, Debug)]
pub struct PgTuneConfig {
    // ── Connection pool (5) ──
    pub pool_max: u32,
    pub pool_min: u32,
    pub pool_acquire_timeout: Duration,
    pub pool_idle_timeout: Duration,
    pub pool_max_lifetime: Duration,

    // ── TCP keepalive (3) ──
    /// TCP keepalive idle time — seconds before the first keepalive probe.
    /// Default 60s. Supabase Pooler typically drops idle connections after ~60s,
    /// so we probe just before that to keep the connection alive.
    pub keepalive_idle: Duration,
    /// TCP keepalive interval — seconds between successive probes.
    /// Default 10s. After the first probe, re-probe every 10s.
    pub keepalive_interval: Duration,
    /// TCP keepalive count — number of unacknowledged probes before the
    /// connection is considered dead. Default 5. Combined with interval,
    /// a dead connection is detected in idle + interval*count seconds
    /// (e.g. 60 + 10*5 = 110s).
    pub keepalive_count: u32,

    // ── KV table storage (4) ──
    pub fillfactor: i32,
    pub toast_storage: String,
    pub toast_threshold: i32,
    pub use_unlogged: bool,

    // ── Autovacuum (5) ──
    pub autovac_vacuum_scale: f64,
    pub autovac_vacuum_threshold: i32,
    pub autovac_analyze_scale: f64,
    pub autovac_vacuum_cost_limit: i32,
    pub autovac_vacuum_cost_delay: i32,

    // ── Query runtime (4) ──
    pub statement_timeout: Duration,
    pub idle_txn_timeout: Duration,
    pub lock_timeout: Duration,
    pub stats_target: i32,

    // ── PG server (8) — some only log recommendations ──
    pub server_shared_buffers: String,
    pub server_work_mem: String,
    pub server_maintenance_work_mem: String,
    pub server_wal_buffers: String,
    pub server_max_connections: i32,
    pub server_effective_cache_size: String,
    pub server_random_page_cost: f64,
    pub server_checkpoint_target: f64,
}

impl Default for PgTuneConfig {
    fn default() -> Self {
        Self {
            pool_max: 20,
            pool_min: 5,
            pool_acquire_timeout: Duration::from_secs(10),
            pool_idle_timeout: Duration::from_secs(600),
            pool_max_lifetime: Duration::from_secs(1800),

            keepalive_idle: Duration::from_secs(60),
            keepalive_interval: Duration::from_secs(10),
            keepalive_count: 5,

            fillfactor: 90,
            toast_storage: "external".to_string(),
            toast_threshold: 2032,
            use_unlogged: false,

            autovac_vacuum_scale: 0.05,
            autovac_vacuum_threshold: 50,
            autovac_analyze_scale: 0.02,
            autovac_vacuum_cost_limit: 2000,
            autovac_vacuum_cost_delay: 1,

            statement_timeout: Duration::from_secs(30),
            idle_txn_timeout: Duration::from_secs(60),
            lock_timeout: Duration::from_secs(10),
            stats_target: 500,

            server_shared_buffers: "256MB".to_string(),
            server_work_mem: "64MB".to_string(),
            server_maintenance_work_mem: "256MB".to_string(),
            server_wal_buffers: "16MB".to_string(),
            server_max_connections: 100,
            server_effective_cache_size: "1GB".to_string(),
            server_random_page_cost: 1.1,
            server_checkpoint_target: 0.9,
        }
    }
}

impl PgTuneConfig {
    /// Load all parameters from environment variables, falling back to defaults.
    ///
    /// Priority: `PG_TUNED_*` env vars > defaults.
    /// URL query params (handled by `PgConfig`) can override pool params
    /// after this struct is created.
    #[must_use]
    pub fn from_env() -> Self {
        // M-5: pool_max=0 would cause a panic in store.rs's assert. Clamp to
        // at least 1 and warn the user.
        let pool_max = {
            let v = env_u32("PG_TUNED_POOL_MAX_CONNECTIONS", 20);
            if v == 0 {
                warn!(
                    env = "PG_TUNED_POOL_MAX_CONNECTIONS",
                    "pool_max=0 is invalid, using default 20"
                );
                20
            } else {
                v
            }
        };

        // M-5: pool_min must not exceed pool_max (checked here for env-only
        // path; store.rs handles the URL+env cross-validation).
        let pool_min = {
            let v = env_u32("PG_TUNED_POOL_MIN_CONNECTIONS", 5);
            if v > pool_max {
                warn!(
                    env = "PG_TUNED_POOL_MIN_CONNECTIONS",
                    value = v,
                    pool_max,
                    "pool_min > pool_max, clamping to pool_max"
                );
                pool_max
            } else {
                v
            }
        };

        // M-4: fillfactor must be in [1, 100] (PG requirement).
        let fillfactor = {
            let v = env_i32("PG_TUNED_TABLE_FILLFACTOR", 90);
            if !(1..=100).contains(&v) {
                warn!(
                    env = "PG_TUNED_TABLE_FILLFACTOR",
                    value = v,
                    "out of range [1, 100], using default 90"
                );
                90
            } else {
                v
            }
        };

        Self {
            // Pool
            pool_max,
            pool_min,
            pool_acquire_timeout: env_duration_nonzero("PG_TUNED_POOL_ACQUIRE_TIMEOUT", 10),
            pool_idle_timeout: env_duration_nonzero("PG_TUNED_POOL_IDLE_TIMEOUT", 600),
            pool_max_lifetime: env_duration_nonzero("PG_TUNED_POOL_MAX_LIFETIME", 1800),

            // TCP keepalive
            keepalive_idle: env_duration_nonzero("PG_TUNED_KEEPALIVE_IDLE", 60),
            keepalive_interval: env_duration_nonzero("PG_TUNED_KEEPALIVE_INTERVAL", 10),
            keepalive_count: {
                let v = env_u32("PG_TUNED_KEEPALIVE_COUNT", 5);
                if v > 100 {
                    warn!(
                        env = "PG_TUNED_KEEPALIVE_COUNT",
                        value = v,
                        "unreasonably high keepalive count (>100), using default 5"
                    );
                    5
                } else {
                    v
                }
            },

            // Table
            fillfactor,
            toast_storage: env_str_validated(
                "PG_TUNED_TABLE_TOAST_STORAGE",
                "external",
                validate_toast_storage,
            ),
            toast_threshold: {
                let v = env_i32("PG_TUNED_TABLE_TOAST_THRESHOLD", 2032);
                // PG's toast_tuple_target range is [128, 8160] for the
                // default 8KB block size (TOAST_TUPLE_TARGET_MAX).
                if v < 128 {
                    warn!(
                        env = "PG_TUNED_TABLE_TOAST_THRESHOLD",
                        value = v,
                        "toast_tuple_target minimum is 128, using default 2032"
                    );
                    2032
                } else if v > 8160 {
                    warn!(
                        env = "PG_TUNED_TABLE_TOAST_THRESHOLD",
                        value = v,
                        "toast_tuple_target maximum is 8160 (8KB block), using default 2032"
                    );
                    2032
                } else {
                    v
                }
            },
            use_unlogged: env_bool("PG_TUNED_TABLE_UNLOGGED", false),

            // Autovacuum
            autovac_vacuum_scale: {
                let v = env_f64("PG_TUNED_AUTOVAC_VACUUM_SCALE", 0.05);
                if !v.is_finite() {
                    warn!(env = "PG_TUNED_AUTOVAC_VACUUM_SCALE", value = %v, "NaN/Infinity not allowed, using default 0.05");
                    0.05
                } else if !(0.0..=1.0).contains(&v) {
                    warn!(
                        env = "PG_TUNED_AUTOVAC_VACUUM_SCALE",
                        value = v,
                        "out of range [0.0, 1.0], using default 0.05"
                    );
                    0.05
                } else {
                    v
                }
            },
            autovac_vacuum_threshold: {
                let v = env_i32("PG_TUNED_AUTOVAC_VACUUM_THRESHOLD", 50);
                if v < 0 {
                    warn!(
                        env = "PG_TUNED_AUTOVAC_VACUUM_THRESHOLD",
                        value = v,
                        "must be >= 0, using default 50"
                    );
                    50
                } else {
                    v
                }
            },
            autovac_analyze_scale: {
                let v = env_f64("PG_TUNED_AUTOVAC_ANALYZE_SCALE", 0.02);
                if !v.is_finite() {
                    warn!(env = "PG_TUNED_AUTOVAC_ANALYZE_SCALE", value = %v, "NaN/Infinity not allowed, using default 0.02");
                    0.02
                } else if !(0.0..=1.0).contains(&v) {
                    warn!(
                        env = "PG_TUNED_AUTOVAC_ANALYZE_SCALE",
                        value = v,
                        "out of range [0.0, 1.0], using default 0.02"
                    );
                    0.02
                } else {
                    v
                }
            },
            autovac_vacuum_cost_limit: {
                let v = env_i32("PG_TUNED_AUTOVAC_VACUUM_COST_LIMIT", 2000);
                if v < 0 {
                    warn!(
                        env = "PG_TUNED_AUTOVAC_VACUUM_COST_LIMIT",
                        value = v,
                        "must be >= 0, using default 2000"
                    );
                    2000
                } else {
                    v
                }
            },
            autovac_vacuum_cost_delay: {
                let v = env_i32("PG_TUNED_AUTOVAC_VACUUM_COST_DELAY", 1);
                if v < 0 {
                    warn!(
                        env = "PG_TUNED_AUTOVAC_VACUUM_COST_DELAY",
                        value = v,
                        "must be >= 0, using default 1"
                    );
                    1
                } else {
                    v
                }
            },

            // Query runtime
            statement_timeout: env_duration_nonzero("PG_TUNED_QUERY_STATEMENT_TIMEOUT", 30),
            idle_txn_timeout: env_duration_nonzero("PG_TUNED_QUERY_IDLE_TXN_TIMEOUT", 60),
            lock_timeout: env_duration_nonzero("PG_TUNED_QUERY_LOCK_TIMEOUT", 10),
            stats_target: {
                let v = env_i32("PG_TUNED_QUERY_STATS_TARGET", 500);
                if !(-1..=10000).contains(&v) {
                    warn!(
                        env = "PG_TUNED_QUERY_STATS_TARGET",
                        value = v,
                        "out of range [-1, 10000], using default 500"
                    );
                    500
                } else {
                    v
                }
            },

            // PG server
            server_shared_buffers: env_str_validated(
                "PG_TUNED_SERVER_SHARED_BUFFERS",
                "256MB",
                validate_pg_memory_size,
            ),
            server_work_mem: env_str_validated(
                "PG_TUNED_SERVER_WORK_MEM",
                "64MB",
                validate_pg_memory_size,
            ),
            server_maintenance_work_mem: env_str_validated(
                "PG_TUNED_SERVER_MAINTENANCE_WORK_MEM",
                "256MB",
                validate_pg_memory_size,
            ),
            server_wal_buffers: env_str_validated(
                "PG_TUNED_SERVER_WAL_BUFFERS",
                "16MB",
                validate_pg_memory_size,
            ),
            server_max_connections: {
                let v = env_i32("PG_TUNED_SERVER_MAX_CONNECTIONS", 100);
                if v <= 0 {
                    warn!(
                        env = "PG_TUNED_SERVER_MAX_CONNECTIONS",
                        value = v,
                        "must be > 0, using default 100"
                    );
                    100
                } else if v > 10_000 {
                    warn!(
                        env = "PG_TUNED_SERVER_MAX_CONNECTIONS",
                        value = v,
                        "unreasonably large (>10000), using default 100"
                    );
                    100
                } else {
                    v
                }
            },
            server_effective_cache_size: env_str_validated(
                "PG_TUNED_SERVER_EFFECTIVE_CACHE_SIZE",
                "1GB",
                validate_pg_memory_size,
            ),
            server_random_page_cost: {
                let v = env_f64("PG_TUNED_SERVER_RANDOM_PAGE_COST", 1.1);
                if !v.is_finite() {
                    warn!(env = "PG_TUNED_SERVER_RANDOM_PAGE_COST", value = %v, "NaN/Infinity not allowed, using default 1.1");
                    1.1
                } else {
                    v
                }
            },
            server_checkpoint_target: {
                let v = env_f64("PG_TUNED_SERVER_CHECKPOINT_TARGET", 0.9);
                // B2: PG requires checkpoint_completion_target ∈ [0.0, 1.0].
                // Clamp to valid range and warn if the user-supplied value
                // was out of bounds.
                if !(0.0..=1.0).contains(&v) {
                    warn!(
                        env = "PG_TUNED_SERVER_CHECKPOINT_TARGET",
                        value = v,
                        "out of range [0.0, 1.0], clamping"
                    );
                }
                v.clamp(0.0, 1.0)
            },
        }
    }

    /// Generate the `CREATE TABLE` DDL.
    ///
    /// This should be executed **once** after pool creation. Failure is fatal.
    ///
    /// # Panics
    ///
    /// Panics if `table` is not a valid SQL identifier (only `[a-zA-Z0-9_]`).
    #[must_use]
    pub fn create_table_sql(&self, table: &str) -> String {
        crate::config::PgConfig::validate_identifier(table)
            .expect("table name must be a valid SQL identifier");
        let kw = if self.use_unlogged { "UNLOGGED " } else { "" };
        format!(
            "CREATE {kw}TABLE IF NOT EXISTS {table} \
             (key BYTEA PRIMARY KEY, val BYTEA NOT NULL)"
        )
    }

    /// Generate table tuning DDL: fillfactor, TOAST storage, autovacuum.
    ///
    /// This should be executed **once** after `create_table_sql`. Failure is
    /// non-fatal (logged as warning) — the table still works without tuning.
    ///
    /// Note: `UNLOGGED` is handled by `create_table_sql` via
    /// `CREATE UNLOGGED TABLE`, so no ALTER SET UNLOGGED is needed here
    /// (the redundant ALTER was removed — it was a no-op since the table
    /// is already UNLOGGED from creation).
    ///
    /// # Panics
    ///
    /// Panics if `table` is not a valid SQL identifier (only `[a-zA-Z0-9_]`),
    /// or if `toast_storage` / `fillfactor` fail defense-in-depth validation
    /// (these are validated in `from_env()`, but direct struct construction
    /// with malicious values is possible since all fields are `pub`).
    #[must_use]
    pub fn tune_table_sql(&self, table: &str) -> String {
        crate::config::PgConfig::validate_identifier(table)
            .expect("table name must be a valid SQL identifier");
        // H-1: Defense-in-depth — validate toast_storage and fillfactor
        // here, not just in from_env(). PgTuneConfig fields are all pub,
        // so a caller could construct it directly with malicious values.
        // session_sql() already does this for memory-size strings; we do
        // the same for toast_storage, fillfactor, and f64 finiteness here.
        assert!(
            validate_toast_storage(&self.toast_storage),
            "toast_storage failed validation: {}",
            self.toast_storage
        );
        assert!(
            (1..=100).contains(&self.fillfactor),
            "fillfactor must be in [1, 100], got {}",
            self.fillfactor
        );
        assert!(
            self.autovac_vacuum_scale.is_finite(),
            "autovac_vacuum_scale must be finite, got {}",
            self.autovac_vacuum_scale
        );
        assert!(
            self.autovac_analyze_scale.is_finite(),
            "autovac_analyze_scale must be finite, got {}",
            self.autovac_analyze_scale
        );
        // Defense-in-depth: autovac cost_limit/cost_delay must be non-negative.
        // These are validated in from_env(), but pub fields allow direct construction.
        assert!(
            self.autovac_vacuum_cost_limit >= 0,
            "autovac_vacuum_cost_limit must be >= 0, got {}",
            self.autovac_vacuum_cost_limit
        );
        assert!(
            self.autovac_vacuum_cost_delay >= 0,
            "autovac_vacuum_cost_delay must be >= 0, got {}",
            self.autovac_vacuum_cost_delay
        );
        assert!(
            self.autovac_vacuum_threshold >= 0,
            "autovac_vacuum_threshold must be >= 0, got {}",
            self.autovac_vacuum_threshold
        );
        assert!(
            (128..=8160).contains(&self.toast_threshold),
            "toast_threshold must be in [128, 8160], got {}",
            self.toast_threshold
        );
        format!(
            r#"
-- Table storage tuning
ALTER TABLE {table} SET (
    fillfactor = {fillfactor},
    toast_tuple_target = {toast_threshold}
);
ALTER TABLE {table} ALTER COLUMN val SET STORAGE {toast};
-- Autovacuum tuning
ALTER TABLE {table} SET (
    autovacuum_vacuum_scale_factor = {vscale},
    autovacuum_vacuum_threshold = {vthresh},
    autovacuum_analyze_scale_factor = {ascale},
    autovacuum_vacuum_cost_limit = {vclimit},
    autovacuum_vacuum_cost_delay = {vcdelay}
);"#,
            fillfactor = self.fillfactor,
            toast = self.toast_storage,
            toast_threshold = self.toast_threshold,
            vscale = self.autovac_vacuum_scale,
            vthresh = self.autovac_vacuum_threshold,
            ascale = self.autovac_analyze_scale,
            vclimit = self.autovac_vacuum_cost_limit,
            vcdelay = self.autovac_vacuum_cost_delay,
        )
    }

    /// Generate session-level `SET` statements.
    ///
    /// Executed via `after_connect` on every new pool connection. These set
    /// query-runtime and PG-server parameters that are session-settable.
    ///
    /// **Note**: behind pgbouncer/Supavisor transaction-mode poolers, session
    /// `SET` may not persist across transactions. For guaranteed effect behind
    /// a pooler, set these at the database or role level
    /// (`ALTER DATABASE … SET …`).
    ///
    /// # Safety
    ///
    /// Memory size strings (`work_mem`, etc.) are validated by
    /// `validate_pg_memory_size` during `from_env()`. Direct construction of
    /// `PgTuneConfig` with unvalidated strings could inject SQL into the
    /// `SET` statements. For defense-in-depth, we assert here too.
    #[must_use]
    pub fn session_sql(&self) -> String {
        // Defense-in-depth: validate all memory size strings before
        // embedding them in SQL. If someone constructed PgTuneConfig
        // directly with malicious values, we catch it here.
        assert!(
            validate_pg_memory_size(&self.server_work_mem),
            "server_work_mem failed validation: {}",
            self.server_work_mem
        );
        assert!(
            validate_pg_memory_size(&self.server_maintenance_work_mem),
            "server_maintenance_work_mem failed validation: {}",
            self.server_maintenance_work_mem
        );
        assert!(
            validate_pg_memory_size(&self.server_effective_cache_size),
            "server_effective_cache_size failed validation: {}",
            self.server_effective_cache_size
        );
        // B5: Guard against NaN/Infinity in random_page_cost — these would
        // produce invalid SQL (`SET random_page_cost = inf` / `nan`).
        assert!(
            self.server_random_page_cost.is_finite(),
            "server_random_page_cost must be finite, got {}",
            self.server_random_page_cost
        );
        format!(
            r#"SET statement_timeout = '{st}s';
SET idle_in_transaction_session_timeout = '{it}s';
SET lock_timeout = '{lt}s';
SET default_statistics_target = {st_target};
SET work_mem = '{wm}';
SET maintenance_work_mem = '{mwm}';
SET random_page_cost = {rpc};
SET effective_cache_size = '{ecs}';"#,
            st = self.statement_timeout.as_secs(),
            it = self.idle_txn_timeout.as_secs(),
            lt = self.lock_timeout.as_secs(),
            st_target = self.stats_target,
            wm = self.server_work_mem,
            mwm = self.server_maintenance_work_mem,
            rpc = self.server_random_page_cost,
            ecs = self.server_effective_cache_size,
        )
    }

    /// Log recommendations for PG server parameters that cannot be set via
    /// `SET` and require `postgresql.conf` changes (with restart).
    ///
    /// Note: `shared_buffers` and `wal_buffers` are only logged here as
    /// recommendations — they are NOT embedded into SQL statements (unlike
    /// `work_mem`/`maintenance_work_mem`/`effective_cache_size` which are
    /// set via `session_sql()`), so they don't need defense-in-depth
    /// validation in `session_sql()`.
    pub fn log_server_hints(&self) {
        info!(
            "PG server params (require postgresql.conf + restart): \
             shared_buffers={}, wal_buffers={}, max_connections={}, \
             checkpoint_completion_target={}",
            self.server_shared_buffers,
            self.server_wal_buffers,
            self.server_max_connections,
            self.server_checkpoint_target,
        );
    }
}

// ─── Env helpers ─────────────────────────────────────────

fn env_u32(key: &str, default: u32) -> u32 {
    match std::env::var(key) {
        Ok(ref v) => match v.parse() {
            Ok(val) => val,
            Err(_) => {
                warn!(env = key, value = %v, "failed to parse as u32, using default {default}");
                default
            }
        },
        Err(_) => default,
    }
}

fn env_i32(key: &str, default: i32) -> i32 {
    match std::env::var(key) {
        Ok(ref v) => match v.parse() {
            Ok(val) => val,
            Err(_) => {
                warn!(env = key, value = %v, "failed to parse as i32, using default {default}");
                default
            }
        },
        Err(_) => default,
    }
}

fn env_f64(key: &str, default: f64) -> f64 {
    match std::env::var(key) {
        Ok(ref v) => match v.parse() {
            Ok(val) => val,
            Err(_) => {
                warn!(env = key, value = %v, "failed to parse as f64, using default {default}");
                default
            }
        },
        Err(_) => default,
    }
}

/// Like `env_str`, but validates the env value with a predicate.
/// Falls back to the default if validation fails (with a warning).
fn env_str_validated(key: &str, default: &str, validate: fn(&str) -> bool) -> String {
    match std::env::var(key) {
        Ok(ref v) if validate(v) => v.clone(),
        Ok(ref v) => {
            warn!(
                env = key,
                value = %v,
                "invalid value, falling back to default '{default}'"
            );
            default.to_string()
        }
        Err(_) => default.to_string(),
    }
}

/// Validate a PG memory size string (e.g. `64MB`, `1GB`, `256kB`).
/// Must match `^[0-9]+(kB|MB|GB|TB)$` or plain integer (bytes).
fn validate_pg_memory_size(v: &str) -> bool {
    let v = v.trim();
    if v.is_empty() {
        return false;
    }
    // Plain integer (bytes)
    if v.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    // Strip known suffixes and check remaining is digits
    let suffixes = ["TB", "GB", "MB", "kB"];
    for suffix in suffixes {
        if let Some(prefix) = v.strip_suffix(suffix) {
            return !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_digit());
        }
    }
    false
}

/// Validate TOAST storage strategy: must be one of the four PG-allowed values.
fn validate_toast_storage(v: &str) -> bool {
    matches!(v, "external" | "extended" | "main" | "plain")
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => {
            let lower = v.to_ascii_lowercase();
            if matches!(lower.as_str(), "true" | "1" | "yes" | "on") {
                true
            } else if matches!(lower.as_str(), "false" | "0" | "no" | "off") {
                false
            } else {
                warn!(
                    env = key,
                    value = %v,
                    "unrecognized boolean value, falling back to default '{default}'"
                );
                default
            }
        }
        Err(_) => default,
    }
}

/// Read a duration from an environment variable, rejecting zero durations.
///
/// A zero timeout would cause immediate expiry (e.g. `acquire_timeout=0`
/// means "fail instantly"), which is almost certainly a misconfiguration.
/// Falls back to the default and warns.
fn env_duration_nonzero(key: &str, default_secs: u64) -> Duration {
    match std::env::var(key) {
        Ok(ref v) => match parse_duration(v) {
            Some(Duration::ZERO) => {
                warn!(
                    env = key,
                    value = %v,
                    "zero duration is invalid (would cause immediate timeout), \
                     using default {default_secs}s"
                );
                Duration::from_secs(default_secs)
            }
            Some(d) => d,
            None => {
                warn!(env = key, value = %v, "failed to parse duration, using default {default_secs}s");
                Duration::from_secs(default_secs)
            }
        },
        Err(_) => Duration::from_secs(default_secs),
    }
}

/// Parse a duration string: `500ms`, `10s`, `5m`, `2h`, or bare secs.
///
/// R2-M2: Validates that the prefix before the suffix is a valid integer.
/// `strip_suffix('m')` alone would match `5km` or `5am`, leading to
/// incorrect parsing. By checking `prefix.parse::<u64>()`, we ensure
/// only pure-numeric prefixes are accepted.
fn parse_duration(v: &str) -> Option<Duration> {
    let v = v.trim();
    if let Some(n) = v.strip_suffix("ms") {
        n.parse().ok().map(Duration::from_millis)
    } else if let Some(n) = v.strip_suffix('s') {
        n.parse().ok().map(Duration::from_secs)
    } else if let Some(n) = v.strip_suffix('m') {
        n.parse::<u64>()
            .ok()
            .and_then(|n| n.checked_mul(60))
            .map(Duration::from_secs)
    } else if let Some(n) = v.strip_suffix('h') {
        n.parse::<u64>()
            .ok()
            .and_then(|n| n.checked_mul(3600))
            .map(Duration::from_secs)
    } else {
        v.parse().ok().map(Duration::from_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let c = PgTuneConfig::default();
        assert_eq!(c.pool_max, 20);
        assert_eq!(c.fillfactor, 90);
        assert!(!c.use_unlogged);
        assert_eq!(c.statement_timeout, Duration::from_secs(30));
        assert_eq!(c.keepalive_idle, Duration::from_secs(60));
        assert_eq!(c.keepalive_interval, Duration::from_secs(10));
        assert_eq!(c.keepalive_count, 5);
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("10s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("45"), Some(Duration::from_secs(45)));
        assert_eq!(parse_duration("abc"), None);
    }

    #[test]
    fn test_session_sql_contains_all_params() {
        let c = PgTuneConfig::default();
        let sql = c.session_sql();
        assert!(sql.contains("statement_timeout"));
        assert!(sql.contains("idle_in_transaction_session_timeout"));
        assert!(sql.contains("lock_timeout"));
        assert!(sql.contains("default_statistics_target"));
        assert!(sql.contains("work_mem"));
        assert!(sql.contains("maintenance_work_mem"));
        assert!(sql.contains("random_page_cost"));
        assert!(sql.contains("effective_cache_size"));
    }

    #[test]
    fn test_create_table_sql() {
        let c = PgTuneConfig::default();
        let sql = c.create_table_sql("kv");
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS kv"));
        assert!(!sql.contains("UNLOGGED"));

        let c2 = PgTuneConfig {
            use_unlogged: true,
            ..PgTuneConfig::default()
        };
        let sql2 = c2.create_table_sql("kv_test");
        assert!(sql2.contains("CREATE UNLOGGED TABLE IF NOT EXISTS kv_test"));
    }

    #[test]
    fn test_tune_table_sql() {
        let c = PgTuneConfig::default();
        let sql = c.tune_table_sql("kv");
        assert!(sql.contains("fillfactor = 90"));
        assert!(sql.contains("STORAGE external"));
        assert!(sql.contains("toast_tuple_target = 2032"));
        assert!(sql.contains("autovacuum_vacuum_scale_factor = 0.05"));
        assert!(!sql.contains("SET UNLOGGED"));
    }

    #[test]
    fn test_validate_pg_memory_size() {
        assert!(validate_pg_memory_size("64MB"));
        assert!(validate_pg_memory_size("1GB"));
        assert!(validate_pg_memory_size("256kB"));
        assert!(validate_pg_memory_size("2TB"));
        assert!(validate_pg_memory_size("1024"));
        assert!(!validate_pg_memory_size(""));
        assert!(!validate_pg_memory_size("64MB'; DROP TABLE kv; --"));
        assert!(!validate_pg_memory_size("abc"));
        assert!(!validate_pg_memory_size("64mb")); // lowercase not allowed by PG
    }

    #[test]
    fn test_validate_toast_storage() {
        assert!(validate_toast_storage("external"));
        assert!(validate_toast_storage("extended"));
        assert!(validate_toast_storage("main"));
        assert!(validate_toast_storage("plain"));
        assert!(!validate_toast_storage("evil'; DROP TABLE kv; --"));
        assert!(!validate_toast_storage("EXTERNAL")); // case-sensitive
    }

    #[test]
    fn test_env_bool() {
        // Unset → default
        unsafe { std::env::remove_var("TEST_ENV_BOOL_UNSET") };
        assert!(env_bool("TEST_ENV_BOOL_UNSET", true));
        assert!(!env_bool("TEST_ENV_BOOL_UNSET", false));

        // Truthy values (case-insensitive)
        for val in &["true", "1", "yes", "on", "TRUE", "Yes", "ON"] {
            unsafe { std::env::set_var("TEST_ENV_BOOL_UNSET", val) };
            assert!(env_bool("TEST_ENV_BOOL_UNSET", false), "val={val}");
        }

        // Falsy values (case-insensitive)
        for val in &["false", "0", "no", "off", "FALSE", "No", "OFF"] {
            unsafe { std::env::set_var("TEST_ENV_BOOL_UNSET", val) };
            assert!(!env_bool("TEST_ENV_BOOL_UNSET", true), "val={val}");
        }

        // Unrecognized → default (not silently false)
        unsafe { std::env::set_var("TEST_ENV_BOOL_UNSET", "maybe") };
        assert!(env_bool("TEST_ENV_BOOL_UNSET", true));
        assert!(!env_bool("TEST_ENV_BOOL_UNSET", false));

        unsafe { std::env::remove_var("TEST_ENV_BOOL_UNSET") };
    }

    // B2: checkpoint_target clamping to [0.0, 1.0]
    #[test]
    fn test_checkpoint_target_clamping() {
        // Out-of-range values should be clamped
        unsafe { std::env::set_var("PG_TUNED_SERVER_CHECKPOINT_TARGET", "1.5") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.server_checkpoint_target, 1.0, "should clamp 1.5 to 1.0");

        unsafe { std::env::set_var("PG_TUNED_SERVER_CHECKPOINT_TARGET", "-0.5") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.server_checkpoint_target, 0.0, "should clamp -0.5 to 0.0");

        // In-range value should pass through
        unsafe { std::env::set_var("PG_TUNED_SERVER_CHECKPOINT_TARGET", "0.7") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.server_checkpoint_target, 0.7, "0.7 should pass through");

        // Exact boundaries
        unsafe { std::env::set_var("PG_TUNED_SERVER_CHECKPOINT_TARGET", "0.0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.server_checkpoint_target, 0.0);

        unsafe { std::env::set_var("PG_TUNED_SERVER_CHECKPOINT_TARGET", "1.0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.server_checkpoint_target, 1.0);

        unsafe { std::env::remove_var("PG_TUNED_SERVER_CHECKPOINT_TARGET") };
    }

    // M-5: pool_max=0 is invalid and should fall back to default
    #[test]
    fn test_pool_max_zero_fallback() {
        unsafe { std::env::set_var("PG_TUNED_POOL_MAX_CONNECTIONS", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.pool_max, 20, "pool_max=0 should fall back to 20");
        unsafe { std::env::remove_var("PG_TUNED_POOL_MAX_CONNECTIONS") };
    }

    // M-5: pool_min > pool_max should be clamped
    #[test]
    fn test_pool_min_exceeds_max_clamped() {
        unsafe { std::env::set_var("PG_TUNED_POOL_MAX_CONNECTIONS", "5") };
        unsafe { std::env::set_var("PG_TUNED_POOL_MIN_CONNECTIONS", "10") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.pool_max, 5);
        assert_eq!(c.pool_min, 5, "pool_min should be clamped to pool_max");
        unsafe { std::env::remove_var("PG_TUNED_POOL_MAX_CONNECTIONS") };
        unsafe { std::env::remove_var("PG_TUNED_POOL_MIN_CONNECTIONS") };
    }

    // M-4: fillfactor out of range should fall back to default
    #[test]
    fn test_fillfactor_out_of_range() {
        unsafe { std::env::set_var("PG_TUNED_TABLE_FILLFACTOR", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.fillfactor, 90, "fillfactor=0 should fall back to 90");

        unsafe { std::env::set_var("PG_TUNED_TABLE_FILLFACTOR", "101") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.fillfactor, 90, "fillfactor=101 should fall back to 90");

        unsafe { std::env::set_var("PG_TUNED_TABLE_FILLFACTOR", "-5") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.fillfactor, 90, "fillfactor=-5 should fall back to 90");

        // Valid range
        unsafe { std::env::set_var("PG_TUNED_TABLE_FILLFACTOR", "50") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.fillfactor, 50, "fillfactor=50 should pass through");

        // Boundaries
        unsafe { std::env::set_var("PG_TUNED_TABLE_FILLFACTOR", "1") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.fillfactor, 1);

        unsafe { std::env::set_var("PG_TUNED_TABLE_FILLFACTOR", "100") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.fillfactor, 100);

        unsafe { std::env::remove_var("PG_TUNED_TABLE_FILLFACTOR") };
    }

    // H-1: tune_table_sql defense-in-depth validation
    #[test]
    #[should_panic(expected = "toast_storage failed validation")]
    fn test_tune_table_sql_rejects_bad_toast() {
        let c = PgTuneConfig {
            toast_storage: "evil'; DROP TABLE kv; --".to_string(),
            ..PgTuneConfig::default()
        };
        let _ = c.tune_table_sql("kv");
    }

    #[test]
    #[should_panic(expected = "fillfactor must be in")]
    fn test_tune_table_sql_rejects_bad_fillfactor() {
        let c = PgTuneConfig {
            fillfactor: 0,
            ..PgTuneConfig::default()
        };
        let _ = c.tune_table_sql("kv");
    }

    // R4: f64 NaN/Infinity should fall back to defaults
    #[test]
    fn test_f64_nan_infinity_fallback() {
        // autovac_vacuum_scale: NaN → default
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_SCALE", "nan") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.autovac_vacuum_scale, 0.05, "NaN should fall back to 0.05");

        // autovac_vacuum_scale: inf → default
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_SCALE", "inf") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.autovac_vacuum_scale, 0.05, "inf should fall back to 0.05");

        // autovac_analyze_scale: NaN → default
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_ANALYZE_SCALE", "nan") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.autovac_analyze_scale, 0.02,
            "NaN should fall back to 0.02"
        );

        // server_random_page_cost: inf → default
        unsafe { std::env::set_var("PG_TUNED_SERVER_RANDOM_PAGE_COST", "inf") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.server_random_page_cost, 1.1,
            "inf should fall back to 1.1"
        );

        // Valid value should pass through
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_SCALE", "0.1") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.autovac_vacuum_scale, 0.1);

        unsafe {
            std::env::remove_var("PG_TUNED_AUTOVAC_VACUUM_SCALE");
            std::env::remove_var("PG_TUNED_AUTOVAC_ANALYZE_SCALE");
            std::env::remove_var("PG_TUNED_SERVER_RANDOM_PAGE_COST");
        }
    }

    // R4: toast_threshold < 128 should fall back to default
    #[test]
    fn test_toast_threshold_min_value() {
        unsafe { std::env::set_var("PG_TUNED_TABLE_TOAST_THRESHOLD", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.toast_threshold, 2032,
            "toast_threshold=0 should fall back to 2032"
        );

        unsafe { std::env::set_var("PG_TUNED_TABLE_TOAST_THRESHOLD", "127") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.toast_threshold, 2032,
            "toast_threshold=127 should fall back to 2032"
        );

        // Boundary: 128 is the minimum valid value
        unsafe { std::env::set_var("PG_TUNED_TABLE_TOAST_THRESHOLD", "128") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.toast_threshold, 128,
            "toast_threshold=128 should pass through"
        );

        // Normal value
        unsafe { std::env::set_var("PG_TUNED_TABLE_TOAST_THRESHOLD", "2032") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.toast_threshold, 2032);

        unsafe { std::env::remove_var("PG_TUNED_TABLE_TOAST_THRESHOLD") };
    }

    // R4: autovac cost_limit/cost_delay negative should fall back to defaults
    #[test]
    fn test_autovac_nonneg_validation() {
        // cost_limit: negative → default
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_COST_LIMIT", "-1") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.autovac_vacuum_cost_limit, 2000,
            "negative cost_limit should fall back to 2000"
        );

        // cost_delay: negative → default
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_COST_DELAY", "-5") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.autovac_vacuum_cost_delay, 1,
            "negative cost_delay should fall back to 1"
        );

        // Zero is valid
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_COST_LIMIT", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.autovac_vacuum_cost_limit, 0,
            "cost_limit=0 should pass through"
        );

        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_COST_DELAY", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.autovac_vacuum_cost_delay, 0,
            "cost_delay=0 should pass through"
        );

        unsafe {
            std::env::remove_var("PG_TUNED_AUTOVAC_VACUUM_COST_LIMIT");
            std::env::remove_var("PG_TUNED_AUTOVAC_VACUUM_COST_DELAY");
        }
    }

    // stats_target out of range should fall back to default
    #[test]
    fn test_stats_target_range_validation() {
        // Below minimum (-2)
        unsafe { std::env::set_var("PG_TUNED_QUERY_STATS_TARGET", "-2") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.stats_target, 500,
            "stats_target=-2 should fall back to 500"
        );

        // Above maximum (10001)
        unsafe { std::env::set_var("PG_TUNED_QUERY_STATS_TARGET", "10001") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.stats_target, 500,
            "stats_target=10001 should fall back to 500"
        );

        // Boundary: -1 is valid (PG uses -1 to disable statistics collection)
        unsafe { std::env::set_var("PG_TUNED_QUERY_STATS_TARGET", "-1") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.stats_target, -1, "stats_target=-1 should pass through");

        // Boundary: 10000 is valid
        unsafe { std::env::set_var("PG_TUNED_QUERY_STATS_TARGET", "10000") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.stats_target, 10000,
            "stats_target=10000 should pass through"
        );

        // Normal value
        unsafe { std::env::set_var("PG_TUNED_QUERY_STATS_TARGET", "500") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.stats_target, 500);

        unsafe { std::env::remove_var("PG_TUNED_QUERY_STATS_TARGET") };
    }

    // autovac_vacuum_threshold negative should fall back to default
    #[test]
    fn test_autovac_vacuum_threshold_nonneg() {
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_THRESHOLD", "-1") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.autovac_vacuum_threshold, 50,
            "negative autovac_vacuum_threshold should fall back to 50"
        );

        // Zero is valid
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_THRESHOLD", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.autovac_vacuum_threshold, 0,
            "autovac_vacuum_threshold=0 should pass through"
        );

        // Normal value
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_THRESHOLD", "100") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.autovac_vacuum_threshold, 100);

        unsafe { std::env::remove_var("PG_TUNED_AUTOVAC_VACUUM_THRESHOLD") };
    }

    // tune_table_sql defense-in-depth: f64 finiteness
    #[test]
    #[should_panic(expected = "autovac_vacuum_scale must be finite")]
    fn test_tune_table_sql_rejects_nan_vacuum_scale() {
        let c = PgTuneConfig {
            autovac_vacuum_scale: f64::NAN,
            ..PgTuneConfig::default()
        };
        let _ = c.tune_table_sql("kv");
    }

    #[test]
    #[should_panic(expected = "autovac_analyze_scale must be finite")]
    fn test_tune_table_sql_rejects_nan_analyze_scale() {
        let c = PgTuneConfig {
            autovac_analyze_scale: f64::NAN,
            ..PgTuneConfig::default()
        };
        let _ = c.tune_table_sql("kv");
    }

    // R6: autovac scale factors should be in [0.0, 1.0]
    #[test]
    fn test_autovac_scale_range_validation() {
        // vacuum_scale > 1.0 → default
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_SCALE", "1.5") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.autovac_vacuum_scale, 0.05, "1.5 should fall back to 0.05");

        // vacuum_scale < 0.0 → default
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_SCALE", "-0.1") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.autovac_vacuum_scale, 0.05,
            "-0.1 should fall back to 0.05"
        );

        // analyze_scale > 1.0 → default
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_ANALYZE_SCALE", "2.0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.autovac_analyze_scale, 0.02,
            "2.0 should fall back to 0.02"
        );

        // Boundaries: 0.0 and 1.0 are valid
        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_SCALE", "0.0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.autovac_vacuum_scale, 0.0, "0.0 should pass through");

        unsafe { std::env::set_var("PG_TUNED_AUTOVAC_VACUUM_SCALE", "1.0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.autovac_vacuum_scale, 1.0, "1.0 should pass through");

        unsafe {
            std::env::remove_var("PG_TUNED_AUTOVAC_VACUUM_SCALE");
            std::env::remove_var("PG_TUNED_AUTOVAC_ANALYZE_SCALE");
        }
    }

    // R6: keepalive_count > 100 is unreasonable
    #[test]
    fn test_keepalive_count_upper_bound() {
        unsafe { std::env::set_var("PG_TUNED_KEEPALIVE_COUNT", "200") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.keepalive_count, 5, "200 should fall back to 5");

        // Normal value
        unsafe { std::env::set_var("PG_TUNED_KEEPALIVE_COUNT", "10") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.keepalive_count, 10, "10 should pass through");

        // Boundary: 100 is valid
        unsafe { std::env::set_var("PG_TUNED_KEEPALIVE_COUNT", "100") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.keepalive_count, 100, "100 should pass through");

        unsafe { std::env::remove_var("PG_TUNED_KEEPALIVE_COUNT") };
    }

    // R7: Duration=0 should be rejected for timeout/lifetime parameters
    #[test]
    fn test_duration_zero_rejected() {
        // pool_acquire_timeout=0 → fallback to default 10s
        unsafe { std::env::set_var("PG_TUNED_POOL_ACQUIRE_TIMEOUT", "0s") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.pool_acquire_timeout,
            Duration::from_secs(10),
            "pool_acquire_timeout=0s should fall back to 10s"
        );

        // pool_idle_timeout=0 → fallback to default 600s
        unsafe { std::env::set_var("PG_TUNED_POOL_IDLE_TIMEOUT", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.pool_idle_timeout,
            Duration::from_secs(600),
            "pool_idle_timeout=0 should fall back to 600s"
        );

        // statement_timeout=0 → fallback to default 30s
        unsafe { std::env::set_var("PG_TUNED_QUERY_STATEMENT_TIMEOUT", "0s") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.statement_timeout,
            Duration::from_secs(30),
            "statement_timeout=0s should fall back to 30s"
        );

        // keepalive_idle=0 → fallback to default 60s
        unsafe { std::env::set_var("PG_TUNED_KEEPALIVE_IDLE", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.keepalive_idle,
            Duration::from_secs(60),
            "keepalive_idle=0 should fall back to 60s"
        );

        // Valid nonzero value should pass through
        unsafe { std::env::set_var("PG_TUNED_POOL_ACQUIRE_TIMEOUT", "30s") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.pool_acquire_timeout,
            Duration::from_secs(30),
            "pool_acquire_timeout=30s should pass through"
        );

        unsafe {
            std::env::remove_var("PG_TUNED_POOL_ACQUIRE_TIMEOUT");
            std::env::remove_var("PG_TUNED_POOL_IDLE_TIMEOUT");
            std::env::remove_var("PG_TUNED_QUERY_STATEMENT_TIMEOUT");
            std::env::remove_var("PG_TUNED_KEEPALIVE_IDLE");
        }
    }

    // R7: server_max_connections range validation
    #[test]
    fn test_server_max_connections_range() {
        // Negative → default
        unsafe { std::env::set_var("PG_TUNED_SERVER_MAX_CONNECTIONS", "-1") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.server_max_connections, 100,
            "negative should fall back to 100"
        );

        // Zero → default
        unsafe { std::env::set_var("PG_TUNED_SERVER_MAX_CONNECTIONS", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.server_max_connections, 100,
            "zero should fall back to 100"
        );

        // Unreasonably large → default
        unsafe { std::env::set_var("PG_TUNED_SERVER_MAX_CONNECTIONS", "50000") };
        let c = PgTuneConfig::from_env();
        assert_eq!(
            c.server_max_connections, 100,
            "50000 should fall back to 100"
        );

        // Valid value should pass through
        unsafe { std::env::set_var("PG_TUNED_SERVER_MAX_CONNECTIONS", "200") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.server_max_connections, 200, "200 should pass through");

        // Boundary: 10000 is valid
        unsafe { std::env::set_var("PG_TUNED_SERVER_MAX_CONNECTIONS", "10000") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.server_max_connections, 10000, "10000 should pass through");

        unsafe { std::env::remove_var("PG_TUNED_SERVER_MAX_CONNECTIONS") };
    }

    // R6: tune_table_sql defense-in-depth for autovac non-negative
    #[test]
    #[should_panic(expected = "autovac_vacuum_cost_limit must be >= 0")]
    fn test_tune_table_sql_rejects_negative_cost_limit() {
        let c = PgTuneConfig {
            autovac_vacuum_cost_limit: -1,
            ..PgTuneConfig::default()
        };
        let _ = c.tune_table_sql("kv");
    }

    #[test]
    #[should_panic(expected = "autovac_vacuum_cost_delay must be >= 0")]
    fn test_tune_table_sql_rejects_negative_cost_delay() {
        let c = PgTuneConfig {
            autovac_vacuum_cost_delay: -1,
            ..PgTuneConfig::default()
        };
        let _ = c.tune_table_sql("kv");
    }

    #[test]
    #[should_panic(expected = "autovac_vacuum_threshold must be >= 0")]
    fn test_tune_table_sql_rejects_negative_vacuum_threshold() {
        let c = PgTuneConfig {
            autovac_vacuum_threshold: -1,
            ..PgTuneConfig::default()
        };
        let _ = c.tune_table_sql("kv");
    }

    // R9: toast_threshold upper bound validation
    #[test]
    fn test_toast_threshold_upper_bound() {
        unsafe { std::env::set_var("PG_TUNED_TABLE_TOAST_THRESHOLD", "8160") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.toast_threshold, 8160, "8160 should pass through");
        unsafe { std::env::set_var("PG_TUNED_TABLE_TOAST_THRESHOLD", "8161") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.toast_threshold, 2032, "8161 should fall back to default");
        unsafe { std::env::remove_var("PG_TUNED_TABLE_TOAST_THRESHOLD") };
    }

    #[test]
    #[should_panic(expected = "toast_threshold must be in [128, 8160]")]
    fn test_tune_table_sql_rejects_oversized_toast_threshold() {
        let c = PgTuneConfig {
            toast_threshold: 10000,
            ..PgTuneConfig::default()
        };
        let _ = c.tune_table_sql("kv");
    }
}
