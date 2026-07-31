//! PostgreSQL storage engine configuration

use std::time::Duration;

/// PostgreSQL storage engine configuration
#[derive(Clone, Debug)]
pub struct PgConfig {
    /// Maximum connection pool size
    pub max_connections: u32,
    /// Minimum idle connections
    pub min_connections: u32,
    /// Connection acquisition timeout
    pub connect_timeout: Duration,
    /// Idle connection timeout (None = no timeout)
    pub idle_timeout: Option<Duration>,
    /// Maximum connection lifetime (None = no limit)
    pub max_lifetime: Option<Duration>,
    /// SQL statement timeout (None = no limit; superseded by PgTuneConfig when tuning is active)
    pub statement_timeout: Option<Duration>,
    /// Automatically create the table on startup
    pub auto_create_table: bool,
    /// Table name (default `kv`; use `kv_test` for tests)
    pub table_name: String,
    /// Default transaction isolation level
    pub isolation_level: PgIsolation,
    /// Use `BEGIN READ ONLY` for read-only transactions
    pub read_only_optimization: bool,
    /// Persistent prepared statement policy (default: auto-detect pgbouncer)
    pub persistent_statements: PersistentStatements,
}

/// Policy for persistent prepared statements.
///
/// - `Auto` (default): probe at startup to detect whether the server is
///   behind pgbouncer/Supabase Pooler (transaction mode). If direct PG, enable
///   persistent statements for performance; if pgbouncer, disable them.
/// - `Enabled`: force persistent prepared statements (best for direct PG).
/// - `Disabled`: force non-persistent (unnamed) statements (required for
///   pgbouncer transaction mode without `max_prepared_statements`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PersistentStatements {
    /// Automatically detect at startup via a probe query.
    #[default]
    Auto,
    /// Force persistent prepared statements.
    Enabled,
    /// Force non-persistent (unnamed) statements.
    Disabled,
}

impl PersistentStatements {
    /// Resolve to a concrete `bool` after optional auto-detection.
    ///
    /// `Auto` returns the provided detected value; explicit variants return
    /// their own value regardless.
    #[must_use]
    pub fn resolve(self, detected: bool) -> bool {
        match self {
            Self::Auto => detected,
            Self::Enabled => true,
            Self::Disabled => false,
        }
    }

    /// Parse from a URL query parameter value.
    /// Accepts `true`/`false` (case-insensitive) and `auto`.
    /// Returns `None` for unrecognized values (caller keeps default).
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(Self::Enabled),
            "false" | "0" | "no" | "off" => Some(Self::Disabled),
            "auto" => Some(Self::Auto),
            _ => None,
        }
    }
}

impl std::fmt::Display for PersistentStatements {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Enabled => write!(f, "enabled"),
            Self::Disabled => write!(f, "disabled"),
        }
    }
}

/// PostgreSQL transaction isolation level
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PgIsolation {
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl PgIsolation {
    /// Return the SQL fragment for `BEGIN ISOLATION LEVEL ...`
    #[must_use]
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::ReadCommitted => "READ COMMITTED",
            Self::RepeatableRead => "REPEATABLE READ",
            Self::Serializable => "SERIALIZABLE",
        }
    }
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            max_connections: 20,
            min_connections: 5,
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Some(Duration::from_secs(600)),
            max_lifetime: Some(Duration::from_secs(1800)),
            statement_timeout: Some(Duration::from_secs(30)),
            auto_create_table: true,
            table_name: "kv".to_string(),
            isolation_level: PgIsolation::default(),
            read_only_optimization: true,
            persistent_statements: PersistentStatements::Auto,
        }
    }
}

impl PgConfig {
    /// Apply configuration from environment variables.
    ///
    /// This is called **after** [`merge_url_params`](Self::merge_url_params),
    /// so environment variables take precedence over URL query parameters.
    ///
    /// Supported variables:
    /// - `PG_PERSISTENT_STATEMENTS`: `auto` | `true`/`1`/`on` | `false`/`0`/`off`
    ///   — override the persistent-statements policy when auto-detection fails.
    pub fn merge_env(&mut self) {
        if let Ok(val) = std::env::var("PG_PERSISTENT_STATEMENTS") {
            if let Some(v) = PersistentStatements::parse(&val) {
                tracing::info!(
                    env = "PG_PERSISTENT_STATEMENTS",
                    value = %val,
                    "overriding persistent_statements from environment"
                );
                self.persistent_statements = v;
            } else {
                tracing::warn!(
                    env = "PG_PERSISTENT_STATEMENTS",
                    value = %val,
                    "unrecognized value, ignoring (expected: auto|true|false|on|off|1|0|yes|no)"
                );
            }
        }
    }

    /// Parse configuration overrides from query parameters in a PG URL.
    ///
    /// Supported params: `max_connections`, `min_connections`,
    /// `statement_timeout` (seconds), `auto_create_table` (bool),
    /// `isolation_level` (read_committed|repeatable_read|serializable),
    /// `persistent_statements` (auto|true|false).
    pub fn merge_url_params(&mut self, url: &str) {
        // Parse the query string manually to avoid adding a URL-parsing dep.
        if let Some(query) = url.split('?').nth(1) {
            for pair in query.split('&') {
                if let Some((key, value)) = pair.split_once('=') {
                    match key {
                        "max_connections" => {
                            if let Ok(v) = value.parse() {
                                self.max_connections = v;
                            }
                        }
                        "min_connections" => {
                            if let Ok(v) = value.parse() {
                                self.min_connections = v;
                            }
                        }
                        "statement_timeout" => {
                            if let Ok(secs) = value.parse() {
                                self.statement_timeout = Some(Duration::from_secs(secs));
                            }
                        }
                        "max_lifetime" => {
                            if let Ok(secs) = value.parse() {
                                self.max_lifetime = Some(Duration::from_secs(secs));
                            }
                        }
                        "auto_create_table" => {
                            if let Ok(v) = value.parse::<bool>() {
                                self.auto_create_table = v;
                            }
                        }
                        "table_name" => {
                            self.table_name = value.to_string();
                        }
                        "isolation_level" => {
                            self.isolation_level = match value {
                                "repeatable_read" => PgIsolation::RepeatableRead,
                                "serializable" => PgIsolation::Serializable,
                                _ => PgIsolation::ReadCommitted,
                            };
                        }
                        "persistent_statements" => {
                            if let Some(v) = PersistentStatements::parse(value) {
                                self.persistent_statements = v;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
