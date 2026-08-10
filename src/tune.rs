//! PostgreSQL tuning configuration — 6-layer, 30-parameter system.
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
//! | Table storage | 5 | `PG_TUNED_TABLE_` | DDL (`ALTER TABLE`) |
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

    // ── KV table storage (5) ──
    pub fillfactor: i32,
    pub toast_storage: String,
    pub toast_threshold: i32,
    pub use_unlogged: bool,
    /// Number of hash partitions for the KV table.
    ///
    /// - 1 (default): single unpartitioned table.
    /// - >1: `CREATE TABLE ... PARTITION BY HASH (key)` with N partitions.
    ///
    /// **Hash partition count is immutable** in PostgreSQL — once a table
    /// is created with N partitions, it cannot be changed. If the table
    /// already exists with a different partition count, startup will fail
    /// with a clear error message.
    pub hash_partitions: u32,

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
            hash_partitions: 1,

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
    /// Validate that partition child names (`{table}_p{i}`) do not exceed
    /// PG's 63-byte identifier limit (NAMEDATALEN − 1).
    ///
    /// When `hash_partitions > 1`, the last child partition name is
    /// `{table}_p{hash_partitions - 1}` (the longest because the suffix
    /// has the most digits). If this exceeds 63 bytes, PG silently truncates
    /// the name, which can cause identifier collisions or DDL failures.
    fn validate_partition_names(&self, table: &str) {
        if self.hash_partitions <= 1 {
            return;
        }
        // The last partition has the longest suffix `_p{N-1}`.
        let last_idx = self.hash_partitions - 1;
        let suffix_len = 2 + format!("{last_idx}").len(); // "_p" + digits
        let name_len = table.len() + suffix_len;
        assert!(
            name_len <= 63,
            "partition name '{table}_p{last_idx}' is {name_len} bytes, \
             exceeding PG's 63-byte identifier limit (NAMEDATALEN). \
             Use a shorter table name or fewer partitions."
        );
    }

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
            hash_partitions: {
                let v = env_u32("PG_TUNED_TABLE_HASH_PARTITIONS", 1);
                if v == 0 {
                    warn!(
                        env = "PG_TUNED_TABLE_HASH_PARTITIONS",
                        value = v,
                        "must be >= 1, using default 1"
                    );
                    1
                } else if v > 1024 {
                    warn!(
                        env = "PG_TUNED_TABLE_HASH_PARTITIONS",
                        value = v,
                        "unreasonably high (>1024), using default 1"
                    );
                    1
                } else {
                    v
                }
            },

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
    /// When `hash_partitions > 1`, the table is created as a hash-partitioned
    /// table with N child partitions. Hash partition count is **immutable** in
    /// PostgreSQL — once created, the number of partitions cannot be changed
    /// without dropping and recreating the table.
    ///
    /// # Panics
    ///
    /// Panics if `table` is not a valid SQL identifier (only `[a-zA-Z0-9_]`),
    /// or if partition names (`{table}_p{i}`) would exceed PG's 63-byte
    /// identifier limit (NAMEDATALEN − 1).
    #[must_use]
    pub fn create_table_sql(&self, table: &str) -> String {
        crate::config::PgConfig::validate_identifier(table)
            .expect("table name must be a valid SQL identifier");
        self.validate_partition_names(table);
        let kw = if self.use_unlogged { "UNLOGGED " } else { "" };
        if self.hash_partitions <= 1 {
            format!(
                "CREATE {kw}TABLE IF NOT EXISTS {table} \
                 (key BYTEA PRIMARY KEY, val BYTEA NOT NULL);"
            )
        } else {
            // Hash-partitioned table: parent + N child partitions.
            //
            // PostgreSQL's hash partitioning uses MODULUS/REMAINDER:
            //   PARTITION p0 FOR VALUES WITH (MODULUS N, REMAINDER 0)
            //   PARTITION p1 FOR VALUES WITH (MODULUS N, REMAINDER 1)
            //   ...
            //
            // The modulus must equal the total number of partitions.
            // Each row is assigned to partition (hash(key) % N).
            //
            // Note: PRIMARY KEY on a partitioned table must include the
            // partition key. Since we partition by `key`, and our PK is
            // already `key`, this is satisfied automatically.
            //
            // Each statement must end with `;` because `raw_sql()` sends
            // the entire string as a single multi-statement command — PG
            // requires `;` between statements.
            let n = self.hash_partitions;
            let mut sql = format!(
                "CREATE {kw}TABLE IF NOT EXISTS {table} \
                 (key BYTEA PRIMARY KEY, val BYTEA NOT NULL) \
                 PARTITION BY HASH (key);\n"
            );
            for i in 0..n {
                sql.push_str(&format!(
                    "CREATE TABLE IF NOT EXISTS {table}_p{i} \
                     PARTITION OF {table} FOR VALUES WITH (MODULUS {n}, REMAINDER {i});\n"
                ));
            }
            sql
        }
    }

    /// Generate SQL to query the actual partition count of a table.
    ///
    /// Returns a query that yields a single row with the partition count
    /// (0 if the table is not partitioned, >0 if it is). The table name
    /// is passed as a `$1` parameter rather than interpolated into the
    /// SQL string, preventing SQL injection even if `validate_identifier`
    /// were bypassed.
    #[must_use]
    pub fn partition_count_sql(&self, _table: &str) -> String {
        // F3: Table name is no longer interpolated — it is bound via $1
        // at the call site (sqlx::query().bind(table_name)). We keep the
        // `table` parameter for API compatibility and validation only.
        crate::config::PgConfig::validate_identifier(_table)
            .expect("table name must be a valid SQL identifier");
        String::from(
            "SELECT count(*) AS part_cnt FROM pg_partitioned_table pt \
             JOIN pg_class c ON c.oid = pt.partrelid \
             JOIN pg_inherits i ON i.inhparent = c.oid \
             WHERE c.relname = $1",
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
    /// Panics if `table` is not a valid SQL identifier, or if partition
    /// names (`{table}_p{i}`) would exceed PG's 63-byte identifier limit.
    #[must_use]
    pub fn tune_table_sql(&self, table: &str) -> String {
        crate::config::PgConfig::validate_identifier(table)
            .expect("table name must be a valid SQL identifier");
        self.validate_partition_names(table);
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
            (0.0..=1.0).contains(&self.autovac_vacuum_scale),
            "autovac_vacuum_scale must be in [0.0, 1.0], got {}",
            self.autovac_vacuum_scale
        );
        assert!(
            self.autovac_analyze_scale.is_finite(),
            "autovac_analyze_scale must be finite, got {}",
            self.autovac_analyze_scale
        );
        assert!(
            (0.0..=1.0).contains(&self.autovac_analyze_scale),
            "autovac_analyze_scale must be in [0.0, 1.0], got {}",
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
        // For non-partitioned tables: generate the full storage SQL (fillfactor +
        // toast_tuple_target + SET STORAGE) + autovacuum SQL.
        //
        // For hash-partitioned tables: PG does NOT allow ANY storage parameters
        // (fillfactor, toast_tuple_target, autovacuum_*) on a partitioned parent
        // table — attempting `ALTER TABLE parent SET (...)` raises
        // "cannot specify storage parameters for a partitioned table" in PG 13.
        // (PG 14+ allows autovacuum reloptions on partitioned tables, but we
        // omit them for cross-version compatibility.) Only `ALTER COLUMN SET
        // STORAGE` is legal on the parent (inherited by children). Each child
        // partition is a regular table and receives the full storage +
        // autovacuum ALTER.
        if self.hash_partitions <= 1 {
            // Non-partitioned: full storage SQL + autovacuum SQL
            let storage_sql = format!(
                r#"
-- Table storage tuning
ALTER TABLE {table} SET (
    fillfactor = {fillfactor},
    toast_tuple_target = {toast_threshold}
);
ALTER TABLE {table} ALTER COLUMN val SET STORAGE {toast};"#,
                fillfactor = self.fillfactor,
                toast = self.toast_storage,
                toast_threshold = self.toast_threshold,
            );
            let autovac_sql = format!(
                r#"
-- Autovacuum tuning
ALTER TABLE {table} SET (
    autovacuum_vacuum_scale_factor = {vscale},
    autovacuum_vacuum_threshold = {vthresh},
    autovacuum_analyze_scale_factor = {ascale},
    autovacuum_vacuum_cost_limit = {vclimit},
    autovacuum_vacuum_cost_delay = {vcdelay}
);"#,
                vscale = self.autovac_vacuum_scale,
                vthresh = self.autovac_vacuum_threshold,
                ascale = self.autovac_analyze_scale,
                vclimit = self.autovac_vacuum_cost_limit,
                vcdelay = self.autovac_vacuum_cost_delay,
            );
            format!("{storage_sql}{autovac_sql}")
        } else {
            // Hash-partitioned: parent table cannot have ANY storage
            // parameters — neither fillfactor/toast_tuple_target nor
            // autovacuum_* are allowed on a partitioned table in PG 13.
            // (PG 14+ allows autovacuum on partitioned tables, but we
            // omit it for cross-version compatibility.) Only ALTER COLUMN
            // SET STORAGE is legal on the parent (it's inherited by
            // children). Child partitions are regular tables, so all
            // storage + autovacuum ALTERs are applied to each child.
            let mut sql = format!(
                r#"
-- Parent table: SET STORAGE only (all SET (...) storage parameters are
-- illegal on partitioned tables in PG 13; child partitions get the full
-- storage + autovacuum tuning below).
ALTER TABLE {table} ALTER COLUMN val SET STORAGE {toast};"#,
                table = table,
                toast = self.toast_storage,
            );
            // F1: For hash-partitioned tables, ALTER TABLE SET (fillfactor=...)
            // on the parent does NOT propagate to child partitions. PG's ALTER
            // TABLE on a partitioned parent only sets the parent's own storage
            // parameters — child partitions inherit their own defaults at creation
            // time. We must explicitly apply the same settings to each child
            // partition to ensure consistent behavior across all partitions.
            for i in 0..self.hash_partitions {
                let part = format!("{table}_p{i}");
                sql.push_str(&format!(
                    r#"
-- Partition {part} storage tuning
ALTER TABLE {part} SET (
    fillfactor = {fillfactor},
    toast_tuple_target = {toast_threshold}
);
ALTER TABLE {part} ALTER COLUMN val SET STORAGE {toast};
-- Partition {part} autovacuum tuning
ALTER TABLE {part} SET (
    autovacuum_vacuum_scale_factor = {vscale},
    autovacuum_vacuum_threshold = {vthresh},
    autovacuum_analyze_scale_factor = {ascale},
    autovacuum_vacuum_cost_limit = {vclimit},
    autovacuum_vacuum_cost_delay = {vcdelay}
);"#,
                    part = part,
                    fillfactor = self.fillfactor,
                    toast = self.toast_storage,
                    toast_threshold = self.toast_threshold,
                    vscale = self.autovac_vacuum_scale,
                    vthresh = self.autovac_vacuum_threshold,
                    ascale = self.autovac_analyze_scale,
                    vclimit = self.autovac_vacuum_cost_limit,
                    vcdelay = self.autovac_vacuum_cost_delay,
                ));
            }
            sql
        }
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
    /// # Security
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
        // F5: Defense-in-depth — stats_target must be in [-1, 10000].
        // from_env() validates this, but pub fields allow direct construction
        // with out-of-range values that would produce invalid SQL.
        assert!(
            (-1..=10000).contains(&self.stats_target),
            "stats_target must be in [-1, 10000], got {}",
            self.stats_target
        );
        // F2: Use Duration formatting that preserves sub-second precision.
        // `as_secs()` truncates sub-second values (e.g. 500ms → 0s), which
        // would disable the timeout. PG accepts `'500ms'` syntax, so we
        // format as seconds when the Duration is an exact number of seconds,
        // and as milliseconds otherwise.
        let fmt_dur = |d: Duration| -> String {
            let sub_ms = d.subsec_millis();
            if sub_ms == 0 {
                // Exact seconds — use 'Ns' format (PG standard)
                format!("{}s", d.as_secs())
            } else {
                // Has sub-second component — use 'Nms' format
                format!("{}ms", d.as_millis())
            }
        };
        format!(
            r#"SET statement_timeout = '{st}';
SET idle_in_transaction_session_timeout = '{it}';
SET lock_timeout = '{lt}';
SET default_statistics_target = {st_target};
SET work_mem = '{wm}';
SET maintenance_work_mem = '{mwm}';
SET random_page_cost = {rpc};
SET effective_cache_size = '{ecs}';"#,
            st = fmt_dur(self.statement_timeout),
            it = fmt_dur(self.idle_txn_timeout),
            lt = fmt_dur(self.lock_timeout),
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
/// F3: Trims whitespace before validation and stores the trimmed value,
/// so `v = " 64MB "` is treated identically to `"64MB"`.
fn env_str_validated(key: &str, default: &str, validate: fn(&str) -> bool) -> String {
    match std::env::var(key) {
        Ok(ref raw) => {
            let v = raw.trim();
            if validate(v) {
                v.to_string()
            } else {
                warn!(
                    env = key,
                    value = %raw,
                    "invalid value, falling back to default '{default}'"
                );
                default.to_string()
            }
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

    // ── hash_partitions tests ──

    #[test]
    fn test_hash_partitions_default() {
        let c = PgTuneConfig::default();
        assert_eq!(
            c.hash_partitions, 1,
            "default should be 1 (no partitioning)"
        );
    }

    #[test]
    fn test_hash_partitions_env() {
        // Valid value
        unsafe { std::env::set_var("PG_TUNED_TABLE_HASH_PARTITIONS", "4") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.hash_partitions, 4);

        // 0 is invalid
        unsafe { std::env::set_var("PG_TUNED_TABLE_HASH_PARTITIONS", "0") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.hash_partitions, 1, "0 should fall back to 1");

        // >1024 is unreasonable
        unsafe { std::env::set_var("PG_TUNED_TABLE_HASH_PARTITIONS", "2000") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.hash_partitions, 1, "2000 should fall back to 1");

        // Boundary: 1024 is valid
        unsafe { std::env::set_var("PG_TUNED_TABLE_HASH_PARTITIONS", "1024") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.hash_partitions, 1024);

        // Boundary: 1 is valid (no partitioning)
        unsafe { std::env::set_var("PG_TUNED_TABLE_HASH_PARTITIONS", "1") };
        let c = PgTuneConfig::from_env();
        assert_eq!(c.hash_partitions, 1);

        unsafe { std::env::remove_var("PG_TUNED_TABLE_HASH_PARTITIONS") };
    }

    #[test]
    fn test_create_table_sql_no_partition() {
        let c = PgTuneConfig::default();
        let sql = c.create_table_sql("kv");
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS kv"));
        assert!(!sql.contains("PARTITION BY"));
        // Non-partitioned: single statement must end with `;`
        assert!(
            sql.trim().ends_with(';'),
            "non-partitioned DDL must end with semicolon: {sql}"
        );
    }

    #[test]
    fn test_create_table_sql_with_partitions() {
        let c = PgTuneConfig {
            hash_partitions: 4,
            ..PgTuneConfig::default()
        };
        let sql = c.create_table_sql("kv");
        assert!(sql.contains("PARTITION BY HASH (key)"));
        assert!(sql.contains("PARTITION OF kv FOR VALUES WITH (MODULUS 4, REMAINDER 0)"));
        assert!(sql.contains("PARTITION OF kv FOR VALUES WITH (MODULUS 4, REMAINDER 1)"));
        assert!(sql.contains("PARTITION OF kv FOR VALUES WITH (MODULUS 4, REMAINDER 2)"));
        assert!(sql.contains("PARTITION OF kv FOR VALUES WITH (MODULUS 4, REMAINDER 3)"));
        // Should not have remainder 4
        assert!(!sql.contains("REMAINDER 4"));
        // Partitioned: multi-statement DDL — every statement must end with `;`
        // because raw_sql() sends the entire string and PG requires `;` between
        // statements.
        let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        assert_eq!(
            stmts.len(),
            5,
            "4 partitions → 5 statements (1 parent + 4 children), got {}: {sql}",
            stmts.len()
        );
        assert!(
            sql.contains("PARTITION BY HASH (key);"),
            "parent statement must end with `;` before child partitions: {sql}"
        );
    }

    #[test]
    fn test_create_table_sql_unlogged_with_partitions() {
        let c = PgTuneConfig {
            hash_partitions: 2,
            use_unlogged: true,
            ..PgTuneConfig::default()
        };
        let sql = c.create_table_sql("kv");
        assert!(sql.contains("CREATE UNLOGGED TABLE IF NOT EXISTS kv"));
        assert!(sql.contains("PARTITION BY HASH (key)"));
    }

    #[test]
    fn test_partition_count_sql() {
        let c = PgTuneConfig::default();
        let sql = c.partition_count_sql("kv");
        assert!(sql.contains("pg_partitioned_table"));
        assert!(sql.contains("pg_class"));
        assert!(sql.contains("pg_inherits"));
        // F3: table name is now a $1 parameter, not interpolated
        assert!(sql.contains("relname = $1"));
    }

    // ── verify_partition_count tests (in store.rs test module) ──

    #[test]
    fn test_tune_table_sql_on_partitioned_table() {
        // tune_table_sql on partitioned tables: parent table must NOT
        // contain ANY SET (...) clause (fillfactor, toast_tuple_target,
        // and autovacuum_* are all illegal on partitioned tables in PG 13).
        // Only ALTER COLUMN SET STORAGE is allowed on the parent. Child
        // partitions get the full storage + autovacuum ALTER.
        let c = PgTuneConfig {
            hash_partitions: 4,
            ..PgTuneConfig::default()
        };
        let sql = c.tune_table_sql("kv");

        // Parent table: should have SET STORAGE, but NO ALTER TABLE kv SET (...)
        assert!(
            sql.contains("ALTER TABLE kv ALTER COLUMN val SET STORAGE external"),
            "parent should have SET STORAGE: {sql}"
        );

        // Parent table must NOT contain any "ALTER TABLE kv SET" clause
        // (all SET (...) storage parameters are illegal on partitioned tables)
        let parent_set_matches: Vec<_> = sql
            .lines()
            .filter(|l| l.contains("ALTER TABLE kv SET"))
            .collect();
        assert!(
            parent_set_matches.is_empty(),
            "parent should have NO ALTER TABLE kv SET (...) clause, but found: {parent_set_matches:?}"
        );

        // Child partitions: should have fillfactor (full storage ALTER)
        assert!(sql.contains("ALTER TABLE kv_p0 SET"));
        assert!(sql.contains("ALTER TABLE kv_p3 SET"));
        assert!(!sql.contains("ALTER TABLE kv_p4 SET"));

        // Child partitions should contain fillfactor
        assert!(
            sql.contains("fillfactor = 90"),
            "child partitions should have fillfactor: {sql}"
        );

        // Child partitions should contain autovacuum
        assert!(
            sql.contains("autovacuum_vacuum_scale_factor"),
            "child partitions should have autovacuum: {sql}"
        );
    }

    // F1: Partition names exceeding PG's 63-byte limit should panic
    #[test]
    #[should_panic(expected = "exceeding PG's 63-byte identifier limit")]
    fn test_create_table_sql_partition_name_too_long() {
        let c = PgTuneConfig {
            hash_partitions: 4,
            ..PgTuneConfig::default()
        };
        // 60 chars + "_p3" = 63 bytes — just at the limit
        let name60 = "a".repeat(60);
        let _ = c.create_table_sql(&name60); // should NOT panic

        // 61 chars + "_p3" = 64 bytes — exceeds limit
        let name61 = "a".repeat(61);
        let _ = c.create_table_sql(&name61); // SHOULD panic
    }

    // F1: Partition name boundary — exactly 63 bytes should be OK
    #[test]
    fn test_create_table_sql_partition_name_at_limit() {
        let c = PgTuneConfig {
            hash_partitions: 4,
            ..PgTuneConfig::default()
        };
        // 60 chars + "_p3" = 63 bytes — exactly at the limit
        let name60 = "a".repeat(60);
        let sql = c.create_table_sql(&name60);
        assert!(sql.contains("PARTITION BY HASH"));
    }

    // F2: session_sql should use ms suffix for sub-second durations
    #[test]
    fn test_session_sql_subsecond_duration() {
        let c = PgTuneConfig {
            statement_timeout: Duration::from_millis(500),
            idle_txn_timeout: Duration::from_secs(10), // exact seconds
            lock_timeout: Duration::from_secs(10),
            ..PgTuneConfig::default()
        };
        let sql = c.session_sql();
        // 500ms has sub-second component → "500ms"
        assert!(
            sql.contains("statement_timeout = '500ms'"),
            "sub-second should use ms: {sql}"
        );
        // 10s is exact seconds → "10s"
        assert!(
            sql.contains("idle_in_transaction_session_timeout = '10s'"),
            "exact seconds should use s: {sql}"
        );
        assert!(
            sql.contains("lock_timeout = '10s'"),
            "whole seconds should use s: {sql}"
        );
    }

    // F2: Default config should produce second-based timeout strings
    #[test]
    fn test_session_sql_default_uses_seconds() {
        let c = PgTuneConfig::default();
        let sql = c.session_sql();
        assert!(sql.contains("statement_timeout = '30s'"));
        assert!(sql.contains("idle_in_transaction_session_timeout = '60s'"));
        assert!(sql.contains("lock_timeout = '10s'"));
    }
}
