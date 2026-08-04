//! PostgreSQL storage engine configuration

use std::time::Duration;

/// Percent-decode a URL-encoded string.
///
/// Handles `%XX` sequences (e.g. `%20` → space, `%2F` → `/`) and
/// `+` → space (for `application/x-www-form-urlencoded` compatibility).
/// Returns the decoded string on success, or the original string
/// if decoding fails (graceful degradation for malformed input).
fn percent_decode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut bytes = input.bytes();
    while let Some(b) = bytes.next() {
        if b == b'+' {
            // B2: Handle + → space (application/x-www-form-urlencoded)
            result.push(' ');
        } else if b == b'%' {
            let hi = bytes.next();
            let lo = bytes.next();
            match (hi, lo) {
                (Some(h), Some(l)) => {
                    let hi_val = hex_digit(h);
                    let lo_val = hex_digit(l);
                    if let (Some(hv), Some(lv)) = (hi_val, lo_val) {
                        result.push(char::from(hv * 16 + lv));
                    } else {
                        // Invalid hex sequence — keep as-is
                        result.push('%');
                        result.push(char::from(h));
                        result.push(char::from(l));
                    }
                }
                _ => {
                    // Incomplete %XX — keep as-is
                    result.push('%');
                    if let Some(h) = hi {
                        result.push(char::from(h));
                    }
                }
            }
        } else {
            result.push(char::from(b));
        }
    }
    result
}

/// Convert a hex byte to its numeric value (0–15), or None if not a hex digit.
fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

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
    /// Validate that a SQL identifier (table name) contains only safe characters,
    /// starts with a letter or underscore, and is not a SQL reserved word.
    ///
    /// Prevents SQL injection through the `table_name` URL parameter.
    /// Only alphanumeric characters and underscores are allowed; the first
    /// character must be an ASCII letter or underscore (not a digit), matching
    /// PostgreSQL's unquoted-identifier rules.
    /// Common SQL reserved words (e.g. `SELECT`, `TABLE`) are rejected
    /// because they would cause syntax errors when interpolated into DDL/DML.
    pub(crate) fn validate_identifier(name: &str) -> Result<(), String> {
        if name.is_empty() {
            return Err(
                "invalid table name '': must be non-empty and contain only [a-zA-Z0-9_]"
                    .to_string(),
            );
        }
        // PostgreSQL requires unquoted identifiers to start with a letter or
        // underscore; a leading digit (e.g. "123table") is a syntax error.
        let first = name.chars().next().expect("non-empty (checked above)");
        if !first.is_ascii_alphabetic() && first != '_' {
            return Err(format!(
                "invalid table name '{name}': first character must be a letter or underscore"
            ));
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "invalid table name '{name}': must contain only [a-zA-Z0-9_]"
            ));
        }
        // Reject common SQL reserved words that would cause syntax errors.
        // This is not exhaustive (PG has ~500 reserved words) but covers the
        // most likely accidental misconfigurations.
        if Self::is_sql_reserved(name) {
            return Err(format!("invalid table name '{name}': SQL reserved word"));
        }
        Ok(())
    }

    /// Check if a name is a SQL reserved word (case-insensitive).
    ///
    /// B5: Uses a sorted array + binary search for O(log n) lookups
    /// instead of linear scan. The list covers PostgreSQL's reserved words
    /// that would cause immediate syntax errors when used as unquoted
    /// identifiers in DDL/DML.
    ///
    /// P1: Avoids `to_ascii_uppercase()` allocation when `name` is already
    /// all-ASCII-uppercase (the common case for accidental reserved-word
    /// usage). Falls back to `to_ascii_uppercase()` only when needed.
    fn is_sql_reserved(name: &str) -> bool {
        // Sorted alphabetically for binary search. Covers the most common
        // PG reserved words that would cause syntax errors as identifiers.
        const RESERVED: &[&str] = &[
            "ALL",
            "AND",
            "ANY",
            "AS",
            "ASC",
            "BETWEEN",
            "BY",
            "CASE",
            "CHECK",
            "CONSTRAINT",
            "CREATE",
            "CROSS",
            "CURRENT",
            "DEFAULT",
            "DELETE",
            "DESC",
            "DISTINCT",
            "DROP",
            "ELSE",
            "EXCEPT",
            "EXISTS",
            "EXPLAIN",
            "FALSE",
            "FETCH",
            "FOR",
            "FOREIGN",
            "FROM",
            "FULL",
            "GRANT",
            "GROUP",
            "HAVING",
            "ILIKE",
            "IN",
            "INDEX",
            "INNER",
            "INSERT",
            "INTERSECT",
            "INTO",
            "IS",
            "JOIN",
            "KEY",
            "LEFT",
            "LIKE",
            "LIMIT",
            "NATURAL",
            "NOT",
            "NULL",
            "NULLS",
            "OFFSET",
            "ON",
            "OR",
            "ORDER",
            "OUTER",
            "OVER",
            "PRIMARY",
            "REFERENCES",
            "RETURNING",
            "RIGHT",
            "ROLLBACK",
            "SELECT",
            "SET",
            "SIMILAR",
            "SOME",
            "SAVEPOINT",
            "TABLE",
            "THEN",
            "TRUE",
            "TRUNCATE",
            "UNION",
            "UNIQUE",
            "USING",
            "VALUES",
            "VIEW",
            "WHEN",
            "WHERE",
            "WITH",
        ];
        // Fast path: if name is already all-ASCII-uppercase (the typical
        // case for "SELECT", "TABLE", etc.), use it directly — zero alloc.
        if name.bytes().all(|b| b.is_ascii_uppercase() || b == b'_') {
            return RESERVED.binary_search(&name).is_ok();
        }
        // Slow path: allocate uppercase version for mixed/lowercase input.
        let upper = name.to_ascii_uppercase();
        RESERVED.binary_search(&upper.as_str()).is_ok()
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
            // B6: Track seen parameter names to detect duplicates.
            let mut seen = std::collections::HashSet::<&str>::new();
            // Known parameter names that we actually process.
            const KNOWN_PARAMS: &[&str] = &[
                "max_connections",
                "min_connections",
                "max_lifetime",
                "auto_create_table",
                "table_name",
                "isolation_level",
                "persistent_statements",
                "connect_timeout",
                "idle_timeout",
                "read_only_optimization",
            ];
            for pair in query.split('&') {
                if let Some((key, value)) = pair.split_once('=') {
                    // B6: Warn on duplicate known parameters.
                    if KNOWN_PARAMS.contains(&key) && !seen.insert(key) {
                        tracing::warn!(
                            param = key,
                            "duplicate URL parameter '{key}': last occurrence wins"
                        );
                    }
                    // B4: Percent-decode the value — URL parameters may contain
                    // %XX sequences (e.g. passwords with special chars, or
                    // table names with non-ASCII characters). We decode only
                    // the value, not the key, because our key names are ASCII
                    // and don't need decoding.
                    let value = percent_decode(value);
                    match key {
                        "max_connections" => match value.parse::<u32>() {
                            Ok(0) => {
                                tracing::warn!("max_connections=0 is invalid, ignoring");
                            }
                            Ok(v) => self.max_connections = Some(v),
                            Err(_) => tracing::warn!(
                                "max_connections='{value}' is not a valid u32, ignoring"
                            ),
                        },
                        "min_connections" => match value.parse::<u32>() {
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
                        },
                        "max_lifetime" => match value.parse::<u64>() {
                            Ok(secs) => self.max_lifetime = Some(Duration::from_secs(secs)),
                            Err(_) => tracing::warn!(
                                "max_lifetime='{value}' is not a valid number, ignoring"
                            ),
                        },
                        "auto_create_table" => match value.parse::<bool>() {
                            Ok(v) => self.auto_create_table = v,
                            Err(_) => tracing::warn!(
                                "auto_create_table='{value}' is not a valid bool, ignoring"
                            ),
                        },
                        "table_name" => {
                            Self::validate_identifier(&value)?;
                            self.table_name = value;
                        }
                        "isolation_level" => {
                            self.isolation_level = match value.to_ascii_lowercase().as_str() {
                                "repeatable_read" | "repeatable read" => {
                                    PgIsolation::RepeatableRead
                                }
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
                        "persistent_statements" => match PersistentStatements::parse(&value) {
                            Some(v) => self.persistent_statements = v,
                            None => tracing::warn!(
                                "persistent_statements='{value}' is not recognized, ignoring"
                            ),
                        },
                        "connect_timeout" => match value.parse::<u64>() {
                            Ok(secs) => self.connect_timeout = Some(Duration::from_secs(secs)),
                            Err(_) => tracing::warn!(
                                "connect_timeout='{value}' is not a valid number, ignoring"
                            ),
                        },
                        "idle_timeout" => match value.parse::<u64>() {
                            Ok(secs) => self.idle_timeout = Some(Duration::from_secs(secs)),
                            Err(_) => tracing::warn!(
                                "idle_timeout='{value}' is not a valid number, ignoring"
                            ),
                        },
                        "read_only_optimization" => match value.parse::<bool>() {
                            Ok(v) => self.read_only_optimization = v,
                            Err(_) => tracing::warn!(
                                "read_only_optimization='{value}' is not a valid bool, ignoring"
                            ),
                        },
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_valid() {
        assert!(PgConfig::validate_identifier("kv").is_ok());
        assert!(PgConfig::validate_identifier("kv_test").is_ok());
        assert!(PgConfig::validate_identifier("_kv").is_ok());
        assert!(PgConfig::validate_identifier("a").is_ok());
        assert!(PgConfig::validate_identifier("Mixed_Case123").is_ok());
    }

    #[test]
    fn test_validate_identifier_empty() {
        assert!(PgConfig::validate_identifier("").is_err());
    }

    #[test]
    fn test_validate_identifier_leading_digit() {
        // PostgreSQL rejects unquoted identifiers starting with a digit.
        assert!(PgConfig::validate_identifier("123table").is_err());
        assert!(PgConfig::validate_identifier("1").is_err());
        assert!(PgConfig::validate_identifier("9kv").is_err());
    }

    #[test]
    fn test_validate_identifier_invalid_chars() {
        assert!(PgConfig::validate_identifier("kv-table").is_err());
        assert!(PgConfig::validate_identifier("kv.table").is_err());
        assert!(PgConfig::validate_identifier("kv table").is_err());
        assert!(PgConfig::validate_identifier("kv;DROP").is_err());
        assert!(PgConfig::validate_identifier("kv'").is_err());
    }

    #[test]
    fn test_validate_identifier_reserved() {
        assert!(PgConfig::validate_identifier("SELECT").is_err());
        assert!(PgConfig::validate_identifier("select").is_err());
        assert!(PgConfig::validate_identifier("TABLE").is_err());
        assert!(PgConfig::validate_identifier("table").is_err());
        assert!(PgConfig::validate_identifier("DROP").is_err());
    }

    // P1: Verify is_sql_reserved works with mixed case (zero-alloc path).
    #[test]
    fn test_is_sql_reserved_case_insensitive() {
        // Exact match
        assert!(PgConfig::is_sql_reserved("SELECT"));
        // Lowercase
        assert!(PgConfig::is_sql_reserved("select"));
        // Mixed case
        assert!(PgConfig::is_sql_reserved("Select"));
        assert!(PgConfig::is_sql_reserved("TaBLe"));
        // Non-reserved
        assert!(!PgConfig::is_sql_reserved("kv"));
        assert!(!PgConfig::is_sql_reserved("my_table"));
        assert!(!PgConfig::is_sql_reserved("data"));
        // Different length than any reserved word — fast reject
        assert!(!PgConfig::is_sql_reserved("x"));
        assert!(!PgConfig::is_sql_reserved("verylongname"));
    }

    // B2: Verify checkpoint_target clamping in tune config.
    // (Placed here because config.rs has the test module, but the actual
    // clamping logic is in tune.rs — tested in tune.rs own module.)

    // ── F7: percent_decode & hex_digit tests ──

    #[test]
    fn test_percent_decode_normal() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("a%2Fb%3Fc"), "a/b?c");
    }

    #[test]
    fn test_percent_decode_consecutive() {
        // Consecutive %XX sequences
        assert_eq!(percent_decode("%2F%2F"), "//");
        assert_eq!(percent_decode("%41%42%43"), "ABC");
    }

    #[test]
    fn test_percent_decode_invalid() {
        // Invalid hex in %XX — kept as-is
        assert_eq!(percent_decode("%GG"), "%GG");
        assert_eq!(percent_decode("%2Z"), "%2Z");
    }

    #[test]
    fn test_percent_decode_trailing_percent() {
        // Trailing % without two hex digits
        assert_eq!(percent_decode("hello%"), "hello%");
        assert_eq!(percent_decode("hello%2"), "hello%2");
    }

    #[test]
    fn test_percent_decode_empty() {
        assert_eq!(percent_decode(""), "");
    }

    #[test]
    fn test_percent_decode_plus() {
        // B2: + should decode to space
        assert_eq!(percent_decode("hello+world"), "hello world");
        assert_eq!(percent_decode("a+b%3Fc"), "a b?c");
        assert_eq!(percent_decode("++"), "  ");
    }

    #[test]
    fn test_hex_digit() {
        assert_eq!(hex_digit(b'0'), Some(0));
        assert_eq!(hex_digit(b'9'), Some(9));
        assert_eq!(hex_digit(b'a'), Some(10));
        assert_eq!(hex_digit(b'f'), Some(15));
        assert_eq!(hex_digit(b'A'), Some(10));
        assert_eq!(hex_digit(b'F'), Some(15));
        assert_eq!(hex_digit(b'g'), None);
        assert_eq!(hex_digit(b' '), None);
    }
}
