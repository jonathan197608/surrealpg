//! surreal-pg — SurrealDB server with a PostgreSQL storage backend.
//!
//! This crate provides a library containing the PostgreSQL storage backend
//! and composer, plus a binary entry point that delegates to
//! [`surrealdb_server::init`].
//!
//! # Usage
//!
//! ```text
//! surreal-pg start --user root --pass secret postgresql://user:pass@host:5432/db
//! ```
//!
//! The `postgresql://` (or `postgres://`) connection string is intercepted by
//! [`PostgresComposer`]; all other schemes (`memory`, `rocksdb:…`,
//! `tikv:…`, `surrealkv:…`) fall through to the community composer.

#![recursion_limit = "512"]

pub mod composer;
pub mod config;
pub mod error;
pub mod pg_builder;
pub mod pg_tx;
pub mod store;
pub mod transaction;
pub mod tune;
