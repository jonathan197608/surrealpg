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
        Self {
            // Pool
            pool_max: env_u32("PG_TUNED_POOL_MAX_CONNECTIONS", 20),
            pool_min: env_u32("PG_TUNED_POOL_MIN_CONNECTIONS", 5),
            pool_acquire_timeout: env_duration("PG_TUNED_POOL_ACQUIRE_TIMEOUT", 10),
            pool_idle_timeout: env_duration("PG_TUNED_POOL_IDLE_TIMEOUT", 600),
            pool_max_lifetime: env_duration("PG_TUNED_POOL_MAX_LIFETIME", 1800),

            // Table
            fillfactor: env_i32("PG_TUNED_TABLE_FILLFACTOR", 90),
            toast_storage: env_str_validated(
                "PG_TUNED_TABLE_TOAST_STORAGE",
                "external",
                validate_toast_storage,
            ),
            toast_threshold: env_i32("PG_TUNED_TABLE_TOAST_THRESHOLD", 2032),
            use_unlogged: env_bool("PG_TUNED_TABLE_UNLOGGED", false),

            // Autovacuum
            autovac_vacuum_scale: env_f64("PG_TUNED_AUTOVAC_VACUUM_SCALE", 0.05),
            autovac_vacuum_threshold: env_i32("PG_TUNED_AUTOVAC_VACUUM_THRESHOLD", 50),
            autovac_analyze_scale: env_f64("PG_TUNED_AUTOVAC_ANALYZE_SCALE", 0.02),
            autovac_vacuum_cost_limit: env_i32("PG_TUNED_AUTOVAC_VACUUM_COST_LIMIT", 2000),
            autovac_vacuum_cost_delay: env_i32("PG_TUNED_AUTOVAC_VACUUM_COST_DELAY", 1),

            // Query runtime
            statement_timeout: env_duration("PG_TUNED_QUERY_STATEMENT_TIMEOUT", 30),
            idle_txn_timeout: env_duration("PG_TUNED_QUERY_IDLE_TXN_TIMEOUT", 60),
            lock_timeout: env_duration("PG_TUNED_QUERY_LOCK_TIMEOUT", 10),
            stats_target: env_i32("PG_TUNED_QUERY_STATS_TARGET", 500),

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
            server_max_connections: env_i32("PG_TUNED_SERVER_MAX_CONNECTIONS", 100),
            server_effective_cache_size: env_str_validated(
                "PG_TUNED_SERVER_EFFECTIVE_CACHE_SIZE",
                "1GB",
                validate_pg_memory_size,
            ),
            server_random_page_cost: env_f64("PG_TUNED_SERVER_RANDOM_PAGE_COST", 1.1),
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
    /// Panics if `table` is not a valid SQL identifier (only `[a-zA-Z0-9_]`).
    #[must_use]
    pub fn tune_table_sql(&self, table: &str) -> String {
        crate::config::PgConfig::validate_identifier(table)
            .expect("table name must be a valid SQL identifier");
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

fn env_duration(key: &str, default_secs: u64) -> Duration {
    match std::env::var(key) {
        Ok(ref v) => match parse_duration(v) {
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
}
