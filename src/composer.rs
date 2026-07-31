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
        path.starts_with("postgres://")
            || path.starts_with("postgresql://")
            || path.starts_with("postgres:")
            || path.starts_with("postgresql:")
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
            info!("Starting PostgreSQL kvs store at {path}");

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
