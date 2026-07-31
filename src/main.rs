//! surreal-pg — SurrealDB server with a PostgreSQL storage backend.
//!
//! This binary delegates entirely to [`init`], providing the
//! full SurrealDB CLI (start / sql / import / export / version / …) over a
//! PostgreSQL-backed key-value store.

#![recursion_limit = "512"]

use std::process::ExitCode;

use surreal_pg::composer::PostgresComposer;
use surrealdb_server::core::CommunityComposer;
use surrealdb_server::init;

fn main() -> ExitCode {
    init(PostgresComposer::new(CommunityComposer()))
}
