//! PostgreSQL storage engine configuration

use std::time::Duration;

/// PostgreSQL storage engine configuration
#[derive(Clone, Debug)]
pub struct PgConfig {
    /// Maximum connection pool size (None = defer to PgTuneConfig)
    pub max_connections: Option<u32>,
    /// Minimum idle connections (None = defer to PgTuneConfig)
    pub min_connections: Option<u32>,
    /// Connection acquisition timeout (None = defer to PgTuneConfig)
    pub connect_timeout: Option<Duration>,
    /// Idle connection timeout (None = defer to PgTuneConfig)
    pub idle_timeout: Option<Duration>,
    /// Maximum connection lifetime (None = defer to PgTuneConfig)
    pub max_lifetime: Option<Duration>,
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
            max_connections: None,
            min_connections: None,
            connect_timeout: None,
            idle_timeout: None,
            max_lifetime: None,
            auto_create_table: true,
            table_name: "kv".to_string(),
            isolation_level: PgIsolation::default(),
            read_only_optimization: true,
            persistent_statements: PersistentStatements::Auto,
        }
    }
}

impl PgConfig {
    /// Validate that a SQL identifier (table name) contains only safe characters.
    ///
    /// Prevents SQL injection through the `table_name` URL parameter.
    /// Only alphanumeric characters and underscores are allowed.
    fn validate_identifier(name: &str) -> Result<(), String> {
        if !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            Ok(())
        } else {
            Err(format!(
                "invalid table name '{name}': must be non-empty and contain only [a-zA-Z0-9_]"
            ))
        }
    }

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
    /// `connect_timeout` (seconds), `idle_timeout` (seconds),
    /// `max_lifetime` (seconds), `auto_create_table` (bool),
    /// `table_name` (identifier), `isolation_level`,
    /// `read_only_optimization` (bool), `persistent_statements`.
    ///
    /// Returns `Err` if the `table_name` parameter contains invalid characters,
    /// allowing the caller to fail gracefully instead of panicking.
    pub fn merge_url_params(&mut self, url: &str) -> Result<(), String> {
        // Parse the query string manually to avoid adding a URL-parsing dep.
        if let Some(query) = url.split('?').nth(1) {
            for pair in query.split('&') {
                if let Some((key, value)) = pair.split_once('=') {
                    match key {
                        "max_connections" => {
                            match value.parse::<u32>() {
                                Ok(0) => {
                                    tracing::warn!("max_connections=0 is invalid, ignoring");
                                }
                                Ok(v) => self.max_connections = Some(v),
                                Err(_) => tracing::warn!(
                                    "max_connections='{value}' is not a valid u32, ignoring"
                                ),
                            }
                        }
                        "min_connections" => {
                            match value.parse::<u32>() {
                                Ok(v) => {
                                    if let Some(max) = self.max_connections
                                        && v > max
                                    {
                                        tracing::warn!(
                                            "min_connections={v} > max_connections={max}, ignoring"
                                        );
                                    } else {
                                        self.min_connections = Some(v);
                                    }
                                }
                                Err(_) => tracing::warn!(
                                    "min_connections='{value}' is not a valid u32, ignoring"
                                ),
                            }
                        }
                        "max_lifetime" => {
                            match value.parse::<u64>() {
                                Ok(secs) => self.max_lifetime = Some(Duration::from_secs(secs)),
                                Err(_) => tracing::warn!(
                                    "max_lifetime='{value}' is not a valid number, ignoring"
                                ),
                            }
                        }
                        "auto_create_table" => {
                            match value.parse::<bool>() {
                                Ok(v) => self.auto_create_table = v,
                                Err(_) => tracing::warn!(
                                    "auto_create_table='{value}' is not a valid bool, ignoring"
                                ),
                            }
                        }
                        "table_name" => {
                            Self::validate_identifier(value)?;
                            self.table_name = value.to_string();
                        }
                        "isolation_level" => {
                            self.isolation_level = match value.to_ascii_lowercase().as_str() {
                                "repeatable_read" => PgIsolation::RepeatableRead,
                                "serializable" => PgIsolation::Serializable,
                                "read_committed" | "read committed" => PgIsolation::ReadCommitted,
                                _ => {
                                    tracing::warn!(
                                        "isolation_level='{value}' is not recognized, defaulting to ReadCommitted"
                                    );
                                    PgIsolation::ReadCommitted
                                }
                            };
                        }
                        "persistent_statements" => {
                            match PersistentStatements::parse(value) {
                                Some(v) => self.persistent_statements = v,
                                None => tracing::warn!(
                                    "persistent_statements='{value}' is not recognized, ignoring"
                                ),
                            }
                        }
                        "connect_timeout" => {
                            match value.parse::<u64>() {
                                Ok(secs) => self.connect_timeout = Some(Duration::from_secs(secs)),
                                Err(_) => tracing::warn!(
                                    "connect_timeout='{value}' is not a valid number, ignoring"
                                ),
                            }
                        }
                        "idle_timeout" => {
                            match value.parse::<u64>() {
                                Ok(secs) => self.idle_timeout = Some(Duration::from_secs(secs)),
                                Err(_) => tracing::warn!(
                                    "idle_timeout='{value}' is not a valid number, ignoring"
                                ),
                            }
                        }
                        "read_only_optimization" => {
                            match value.parse::<bool>() {
                                Ok(v) => self.read_only_optimization = v,
                                Err(_) => tracing::warn!(
                                    "read_only_optimization='{value}' is not a valid bool, ignoring"
                                ),
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }
}
