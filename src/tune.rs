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

use tracing::info;

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
            toast_storage: env_str("PG_TUNED_TABLE_TOAST_STORAGE", "external"),
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
            server_shared_buffers: env_str("PG_TUNED_SERVER_SHARED_BUFFERS", "256MB"),
            server_work_mem: env_str("PG_TUNED_SERVER_WORK_MEM", "64MB"),
            server_maintenance_work_mem: env_str("PG_TUNED_SERVER_MAINTENANCE_WORK_MEM", "256MB"),
            server_wal_buffers: env_str("PG_TUNED_SERVER_WAL_BUFFERS", "16MB"),
            server_max_connections: env_i32("PG_TUNED_SERVER_MAX_CONNECTIONS", 100),
            server_effective_cache_size: env_str("PG_TUNED_SERVER_EFFECTIVE_CACHE_SIZE", "1GB"),
            server_random_page_cost: env_f64("PG_TUNED_SERVER_RANDOM_PAGE_COST", 1.1),
            server_checkpoint_target: env_f64("PG_TUNED_SERVER_CHECKPOINT_TARGET", 0.9),
        }
    }

    /// Generate the `CREATE TABLE` DDL.
    ///
    /// This should be executed **once** after pool creation. Failure is fatal.
    #[must_use]
    pub fn create_table_sql(&self, table: &str) -> String {
        let kw = if self.use_unlogged { "UNLOGGED " } else { "" };
        format!(
            "CREATE {kw}TABLE IF NOT EXISTS {table} \
             (key BYTEA PRIMARY KEY, val BYTEA NOT NULL)"
        )
    }

    /// Generate table tuning DDL: fillfactor, TOAST storage, autovacuum, UNLOGGED.
    ///
    /// This should be executed **once** after `create_table_sql`. Failure is
    /// non-fatal (logged as warning) — the table still works without tuning.
    #[must_use]
    pub fn tune_table_sql(&self, table: &str) -> String {
        let unlogged_alter = if self.use_unlogged {
            format!("ALTER TABLE {table} SET UNLOGGED;")
        } else {
            String::new()
        };
        format!(
            r#"
-- Table storage tuning
ALTER TABLE {table} SET (fillfactor = {fillfactor});
ALTER TABLE {table} ALTER COLUMN val SET STORAGE {toast};
ALTER TABLE {table} ALTER COLUMN val SET (toast_tuple_target = {toast_threshold});
-- Autovacuum tuning
ALTER TABLE {table} SET (
    autovacuum_vacuum_scale_factor = {vscale},
    autovacuum_vacuum_threshold = {vthresh},
    autovacuum_analyze_scale_factor = {ascale},
    autovacuum_vacuum_cost_limit = {vclimit},
    autovacuum_vacuum_cost_delay = {vcdelay}
);
{unlogged_alter}"#,
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
    #[must_use]
    pub fn session_sql(&self) -> String {
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
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_i32(key: &str, default: i32) -> i32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "on"))
        .unwrap_or(default)
}

fn env_duration(key: &str, default_secs: u64) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|v| parse_duration(&v))
        .unwrap_or_else(|| Duration::from_secs(default_secs))
}

/// Parse a duration string: `500ms`, `10s`, `5m`, `2h`, or bare secs.
fn parse_duration(v: &str) -> Option<Duration> {
    let v = v.trim();
    if let Some(n) = v.strip_suffix("ms") {
        n.parse().ok().map(Duration::from_millis)
    } else if let Some(n) = v.strip_suffix('s') {
        n.parse().ok().map(Duration::from_secs)
    } else if let Some(n) = v.strip_suffix('m') {
        n.parse::<u64>().ok().map(|n| Duration::from_secs(n * 60))
    } else if let Some(n) = v.strip_suffix('h') {
        n.parse::<u64>().ok().map(|n| Duration::from_secs(n * 3600))
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
}
