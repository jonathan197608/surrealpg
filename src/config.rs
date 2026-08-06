//! PostgreSQL storage engine configuration

use std::time::Duration;

/// Percent-decode a URL-encoded string.
///
/// Handles `%XX` sequences (e.g. `%20` → space, `%2F` → `/`) and
/// `+` → space (for `application/x-www-form-urlencoded` compatibility).
/// Returns the decoded string, using lossy conversion for invalid UTF-8
/// (malformed byte sequences are replaced with U+FFFD).
fn percent_decode(input: &str) -> String {
    // M-1: Use Vec<u8> as a byte buffer, then convert to String at the end.
    // The previous implementation used char::from(b) which treated each byte
    // as a Latin-1 character, corrupting multi-byte UTF-8 sequences (e.g.
    // a UTF-8 encoded "é" = 0xC3 0xA9 would become "Ã©" instead of "é").
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                // B2: Handle + → space (application/x-www-form-urlencoded)
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_digit(bytes[i + 1]);
                let lo = hex_digit(bytes[i + 2]);
                match (hi, lo) {
                    (Some(hv), Some(lv)) => {
                        out.push(hv * 16 + lv);
                        i += 3;
                    }
                    _ => {
                        // Invalid hex sequence — keep as-is
                        out.push(b'%');
                        out.push(bytes[i + 1]);
                        out.push(bytes[i + 2]);
                        i += 3;
                    }
                }
            }
            b'%' if i + 1 < bytes.len() => {
                // Incomplete %XX (one digit after %) — keep as-is
                out.push(b'%');
                out.push(bytes[i + 1]);
                i += 2;
            }
            b'%' => {
                // Trailing % — keep as-is
                out.push(b'%');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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

/// Parse a boolean-like URL parameter value.
///
/// Accepts the same synonyms as [`PersistentStatements::parse`]:
/// `true`/`1`/`yes`/`on`/`false`/`0`/`no`/`off` (case-insensitive).
/// Returns `None` for unrecognized values.
fn parse_bool_param(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
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
    /// Slow-acquire warning threshold (None = defer to sqlx default 2s)
    pub slow_acquire_threshold_secs: Option<Duration>,
    /// Slow-statement warning threshold (None = defer to sqlx default 1s)
    pub slow_statements_threshold_secs: Option<Duration>,
    /// Automatically create the table on startup
    pub auto_create_table: bool,
    /// Table name (default `kv`; use `kv_test` for tests)
    pub table_name: String,
    /// Default transaction isolation level
    pub isolation_level: PgIsolation,
    /// Persistent prepared statement policy (default: auto-detect pgbouncer)
    pub persistent_statements: PersistentStatements,
    /// Whether the server is behind a connection pooler (e.g. Supabase Pooler,
    /// pgbouncer in transaction mode). When `true`, the store uses direct
    /// connections (bypassing sqlx's connection pool) to avoid the "zombie pool"
    /// problem where the pooler silently reclaims idle connections.
    ///
    /// Set via the `pooler=true` URL query parameter.
    pub pooler: bool,
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
    /// Accepts `true`/`false`/`enabled`/`disabled` (case-insensitive) and `auto`.
    /// Returns `None` for unrecognized values (caller keeps default).
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" | "enabled" => Some(Self::Enabled),
            "false" | "0" | "no" | "off" | "disabled" => Some(Self::Disabled),
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
            slow_acquire_threshold_secs: None,
            slow_statements_threshold_secs: None,
            auto_create_table: true,
            table_name: "kv".to_string(),
            isolation_level: PgIsolation::default(),
            persistent_statements: PersistentStatements::Auto,
            pooler: false,
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
            "SAVEPOINT",
            "SELECT",
            "SET",
            "SIMILAR",
            "SOME",
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
        // debug_assert: RESERVED must be sorted lexicographically for
        // binary_search to work correctly. This catches accidental
        // reordering during maintenance. Checked in debug builds only —
        // zero cost in release.
        debug_assert!(
            RESERVED.windows(2).all(|w| w[0] < w[1]),
            "RESERVED array must be sorted lexicographically"
        );
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
    /// `persistent_statements`.
    ///
    /// Returns `Err` if the `table_name` parameter contains invalid characters,
    /// allowing the caller to fail gracefully instead of panicking.
    pub fn merge_url_params(&mut self, url: &str) -> Result<(), String> {
        // Parse the query string manually to avoid adding a URL-parsing dep.
        if let Some(query) = url.split('?').nth(1) {
            // M-2: Strip URL fragment (#...) from the query string.
            // Without this, `?table_name=kv#frag` would set table_name to "kv#frag".
            let query = query.split('#').next().unwrap_or(query);
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
                "slow_acquire_threshold_secs",
                "slow_statements_threshold_secs",
                "pooler",
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
                        "auto_create_table" => match parse_bool_param(&value) {
                            Some(v) => self.auto_create_table = v,
                            None => tracing::warn!(
                                "auto_create_table='{value}' is not a valid bool (expected true/false/yes/no/on/off/1/0), ignoring"
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
                        "slow_acquire_threshold_secs" => match value.parse::<u64>() {
                            Ok(secs) => {
                                self.slow_acquire_threshold_secs = Some(Duration::from_secs(secs))
                            }
                            Err(_) => tracing::warn!(
                                "slow_acquire_threshold_secs='{value}' is not a valid number, ignoring"
                            ),
                        },
                        "slow_statements_threshold_secs" => match value.parse::<u64>() {
                            Ok(secs) => {
                                self.slow_statements_threshold_secs =
                                    Some(Duration::from_secs(secs))
                            }
                            Err(_) => tracing::warn!(
                                "slow_statements_threshold_secs='{value}' is not a valid number, ignoring"
                            ),
                        },
                        "pooler" => match parse_bool_param(&value) {
                            Some(v) => self.pooler = v,
                            None => tracing::warn!(
                                "pooler='{value}' is not a valid bool (expected true/false/yes/no/on/off/1/0), ignoring"
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
        // SAVEPOINT was previously misplaced in the RESERVED array (after
        // SOME instead of before SELECT), causing binary_search to miss it.
        assert!(PgConfig::validate_identifier("SAVEPOINT").is_err());
        assert!(PgConfig::validate_identifier("savepoint").is_err());
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
        // SAVEPOINT — regression: was misplaced in array, binary_search missed it
        assert!(PgConfig::is_sql_reserved("SAVEPOINT"));
        assert!(PgConfig::is_sql_reserved("savepoint"));
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

    // M-1: Multi-byte UTF-8 percent-decoding. The old implementation used
    // char::from(b) which treated each byte as Latin-1, corrupting UTF-8.
    #[test]
    fn test_percent_decode_multibyte_utf8() {
        // "é" = U+00E9 = UTF-8: 0xC3 0xA9 = %C3%A9
        assert_eq!(percent_decode("caf%C3%A9"), "café");
        // "日本" = U+65E5 U+672C = UTF-8: %E6%97%A5%E6%9C%AC
        assert_eq!(percent_decode("%E6%97%A5%E6%9C%AC"), "日本");
        // Mixed ASCII + multi-byte
        assert_eq!(percent_decode("hello%20world%21"), "hello world!");
        // '€' = U+20AC = UTF-8: %E2%82%AC
        assert_eq!(percent_decode("100%E2%82%AC"), "100€");
    }

    // M-3: PersistentStatements::parse accepts "enabled"/"disabled" synonyms
    #[test]
    fn test_persistent_statements_parse_synonyms() {
        assert_eq!(
            PersistentStatements::parse("enabled"),
            Some(PersistentStatements::Enabled)
        );
        assert_eq!(
            PersistentStatements::parse("disabled"),
            Some(PersistentStatements::Disabled)
        );
        assert_eq!(
            PersistentStatements::parse("ENABLED"),
            Some(PersistentStatements::Enabled)
        );
        assert_eq!(
            PersistentStatements::parse("Disabled"),
            Some(PersistentStatements::Disabled)
        );
        // Still accepts old values
        assert_eq!(
            PersistentStatements::parse("true"),
            Some(PersistentStatements::Enabled)
        );
        assert_eq!(
            PersistentStatements::parse("auto"),
            Some(PersistentStatements::Auto)
        );
        assert_eq!(PersistentStatements::parse("bogus"), None);
    }

    // M-2: merge_url_params should strip URL fragments
    #[test]
    fn test_merge_url_params_fragment() {
        let mut config = PgConfig::default();
        config
            .merge_url_params("postgresql://u:p@h/db?table_name=kv#fragment")
            .unwrap();
        assert_eq!(config.table_name, "kv");

        // Fragment after multiple params
        let mut config2 = PgConfig::default();
        config2
            .merge_url_params("postgresql://u:p@h/db?min_connections=3&table_name=my_table#x")
            .unwrap();
        assert_eq!(config2.table_name, "my_table");
        assert_eq!(config2.min_connections, Some(3));
    }

    // Audit-3: parse_bool_param accepts same synonyms as PersistentStatements
    #[test]
    fn test_parse_bool_param_synonyms() {
        assert_eq!(parse_bool_param("true"), Some(true));
        assert_eq!(parse_bool_param("1"), Some(true));
        assert_eq!(parse_bool_param("yes"), Some(true));
        assert_eq!(parse_bool_param("on"), Some(true));
        assert_eq!(parse_bool_param("false"), Some(false));
        assert_eq!(parse_bool_param("0"), Some(false));
        assert_eq!(parse_bool_param("no"), Some(false));
        assert_eq!(parse_bool_param("off"), Some(false));
        // Case-insensitive
        assert_eq!(parse_bool_param("True"), Some(true));
        assert_eq!(parse_bool_param("ON"), Some(true));
        assert_eq!(parse_bool_param("No"), Some(false));
        assert_eq!(parse_bool_param("OFF"), Some(false));
        // Invalid
        assert_eq!(parse_bool_param("maybe"), None);
        assert_eq!(parse_bool_param("2"), None);
    }

    // Audit-3: merge_url_params should parse pooler with synonyms
    #[test]
    fn test_merge_url_params_pooler_synonyms() {
        let mut config = PgConfig::default();
        config
            .merge_url_params("postgresql://u:p@h/db?pooler=on")
            .unwrap();
        assert!(config.pooler);

        let mut config2 = PgConfig::default();
        config2
            .merge_url_params("postgresql://u:p@h/db?pooler=0")
            .unwrap();
        assert!(!config2.pooler);

        let mut config3 = PgConfig::default();
        config3
            .merge_url_params("postgresql://u:p@h/db?pooler=yes")
            .unwrap();
        assert!(config3.pooler);
    }
}
