//! PostgresComposer — delegation-pattern composer for PostgreSQL storage.
//!
//! Wraps [`CommunityComposer`] and intercepts only `postgresql://` / `postgres://`
//! connection paths. All other schemes fall through to the community composer.
//!
//! # Traits implemented
//!
//! | Trait | Strategy |
//! |-------|----------|
//! | `TransactionBuilderFactory` | **Custom** — intercepts `postgresql://` |
//! | `RouterFactory` | Delegates to `CommunityComposer` |
//! | `ConfigCheck` | Delegates to `CommunityComposer` |
//! | `BucketStoreProvider` | Delegates to `CommunityComposer` |
//! | `ObservabilityProvider` | Delegates to `CommunityComposer` |

use std::fmt;
use std::sync::Arc;

use surrealdb_core::CommunityComposer;
use surrealdb_core::buc::{BucketStoreProvider, BucketStoreProviderRequirements};
use surrealdb_core::cnf::ConfigMap;
use surrealdb_core::kvs::{
    TransactionBuilder, TransactionBuilderFactory, TransactionBuilderFactoryRequirements,
    TransactionBuilderParts, TransactionBuilderRequirements,
};
use surrealdb_core::observe::ExecutionObserver;
use surrealdb_server::observe::ObservabilityProvider;
use surrealdb_server::{ConfigCheck, ConfigCheckRequirements, RouterFactory, RpcState};

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::store::PgStore;

/// Redact userinfo (username:password) from a PostgreSQL connection URL.
///
/// Turns `postgresql://user:pass@host:5432/db` into `postgresql://***:***@host:5432/db`.
/// If the URL cannot be parsed or has no userinfo, returns the original string
/// with a `(redaction failed)` suffix so the operator knows something is wrong.
fn redact_url(url: &str) -> String {
    // Find the scheme separator "://"
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after_scheme = &url[scheme_end + 3..];

    // Find the end of the authority (start of path '/', query '?', or fragment '#')
    let authority_end = after_scheme
        .find('/')
        .or_else(|| after_scheme.find('?'))
        .or_else(|| after_scheme.find('#'))
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..authority_end];
    let rest = &after_scheme[authority_end..];

    // Authority may be `[ipv6]:port` — still needs userinfo redaction if
    // it contains '@', e.g. `user:pass@[::1]:5432`.
    if let Some(at_pos) = authority.find('@') {
        let host_port = &authority[at_pos + 1..];
        let scheme = &url[..scheme_end + 3]; // includes "://"
        format!("{scheme}***:***@{host_port}{rest}")
    } else {
        url.to_string()
    }
}

/// A composer that wraps [`CommunityComposer`] and adds PostgreSQL backend
/// support.
///
/// Only `TransactionBuilderFactory` has custom logic: paths starting with
/// `postgres://` or `postgresql://` are routed to [`PgStore`]; all other
/// paths fall through to the community composer.
pub struct PostgresComposer {
    inner: CommunityComposer,
}

impl PostgresComposer {
    /// Create a new `PostgresComposer` wrapping the given [`CommunityComposer`].
    #[must_use]
    pub fn new(inner: CommunityComposer) -> Self {
        Self { inner }
    }

    /// Check whether a path should be handled by the PostgreSQL backend.
    fn is_pg_path(path: &str) -> bool {
        path.starts_with("postgres:") || path.starts_with("postgresql:")
    }
}

impl Default for PostgresComposer {
    fn default() -> Self {
        Self::new(CommunityComposer::default())
    }
}

// ─── Display ───────────────────────────────────────────

impl fmt::Display for PostgresComposer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgresComposer")
    }
}

// ─── Requirement marker traits ─────────────────────────
// All of these are `Send + Sync + 'static` auto-trait bounds; PostgresComposer
// is a struct containing only CommunityComposer (which is Send + Sync + 'static).

impl TransactionBuilderFactoryRequirements for PostgresComposer {}
impl TransactionBuilderRequirements for PostgresComposer {}
impl BucketStoreProviderRequirements for PostgresComposer {}
impl ConfigCheckRequirements for PostgresComposer {}
impl surrealdb_core::observe::requirements::ObservabilityProviderRequirements for PostgresComposer {}

// ─── TransactionBuilderFactory (CUSTOM) ────────────────

impl TransactionBuilderFactory for PostgresComposer {
    type RouterState = ();

    async fn new_transaction_builder(
        &self,
        path: &str,
        canceller: CancellationToken,
        config: ConfigMap,
    ) -> anyhow::Result<TransactionBuilderParts<Self::RouterState>> {
        if Self::is_pg_path(path) {
            info!("Starting PostgreSQL kvs store at {}", redact_url(path));

            // F2: ConfigMap is not used by the PG backend — connection
            // configuration comes from URL query params and environment
            // variables (see PgConfig::merge_url_params / merge_env).
            // Explicitly ignore to make the intent clear.
            let _ = &config;

            let store = PgStore::new(path).await?;

            info!("Started PostgreSQL kvs store");

            let store_boxed: Box<dyn TransactionBuilder> = Box::new((*store).clone());

            Ok(TransactionBuilderParts::without_router_state(store_boxed))
        } else {
            // Delegate to the community composer for all other backends
            self.inner
                .new_transaction_builder(path, canceller, config)
                .await
                .map(|parts| TransactionBuilderParts::without_router_state(parts.builder))
        }
    }

    fn path_valid(v: &str) -> anyhow::Result<String> {
        if Self::is_pg_path(v) {
            Ok(v.to_string())
        } else {
            // Fall through to community composer's validation
            CommunityComposer::path_valid(v)
        }
    }
}

// ─── RouterFactory (DELEGATE) ──────────────────────────

impl RouterFactory for PostgresComposer {
    fn configure_router(router_state: Self::RouterState) -> axum::Router<Arc<RpcState>> {
        CommunityComposer::configure_router(router_state)
    }
}

// ─── ConfigCheck (DELEGATE) ────────────────────────────

#[async_trait::async_trait]
impl ConfigCheck for PostgresComposer {
    async fn check_config(&mut self, cfg: &surrealdb_server::Config) -> anyhow::Result<()> {
        self.inner.check_config(cfg).await
    }
}

// ─── BucketStoreProvider (DELEGATE) ────────────────────

impl BucketStoreProvider for PostgresComposer {
    fn connect<'a>(
        &self,
        url: &'a str,
        global: bool,
        readonly: bool,
        config: surrealdb_core::buc::Config,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = anyhow::Result<Arc<dyn surrealdb_core::buc::store::ObjectStore>>>
                + Send
                + Sync
                + 'a,
        >,
    > {
        self.inner.connect(url, global, readonly, config)
    }
}

// ─── ObservabilityProvider (DELEGATE) ──────────────────

// Core trait (required by server's supertrait)
impl surrealdb_core::observe::ObservabilityProvider for PostgresComposer {
    fn create_observer(&self) -> Arc<dyn ExecutionObserver> {
        self.inner.create_observer()
    }
}

// Server-side supertrait (all methods have defaults that delegate to core)
impl ObservabilityProvider for PostgresComposer {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_url_basic() {
        assert_eq!(
            redact_url("postgresql://user:pass@host:5432/db"),
            "postgresql://***:***@host:5432/db"
        );
    }

    #[test]
    fn test_redact_url_no_userinfo() {
        // No userinfo → returned as-is
        assert_eq!(
            redact_url("postgresql://host:5432/db"),
            "postgresql://host:5432/db"
        );
    }

    #[test]
    fn test_redact_url_no_scheme() {
        // No :// → returned as-is
        assert_eq!(redact_url("just-a-string"), "just-a-string");
    }

    #[test]
    fn test_redact_url_ipv6_with_userinfo() {
        // R10-F1: IPv6 addresses with userinfo must be redacted
        assert_eq!(
            redact_url("postgresql://user:pass@[::1]:5432/db"),
            "postgresql://***:***@[::1]:5432/db"
        );
    }

    #[test]
    fn test_redact_url_ipv6_no_userinfo() {
        // IPv6 without userinfo → returned as-is
        assert_eq!(
            redact_url("postgresql://[::1]:5432/db"),
            "postgresql://[::1]:5432/db"
        );
    }

    #[test]
    fn test_redact_url_with_query() {
        assert_eq!(
            redact_url("postgresql://user:pass@host:5432/db?sslmode=require"),
            "postgresql://***:***@host:5432/db?sslmode=require"
        );
    }

    #[test]
    fn test_redact_url_ipv6_with_query() {
        assert_eq!(
            redact_url("postgresql://user:pass@[2001:db8::1]:5432/db?sslmode=require"),
            "postgresql://***:***@[2001:db8::1]:5432/db?sslmode=require"
        );
    }
}
