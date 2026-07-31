//! SurrealQL-level KV test suite.
//!
//! Tests the PG backend through the full SurrealDB engine (Datastore API),
//! verifying that SurrealQL statements work correctly end-to-end.
//!
//! These tests run when `PG_TEST_URL` is set and are skipped otherwise.

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Builder, Datastore};

/// Build a SurrealDB Datastore backed by PostgreSQL.
async fn build_ds(url: &str) -> anyhow::Result<Datastore> {
    let ds = Builder::new()
        .build_with_factory_path(url, surreal_pg::composer::PostgresComposer::default())
        .await?;
    Ok(ds)
}

/// Build the PG test URL with table_name param.
fn test_url() -> Option<String> {
    let raw_url = std::env::var("PG_TEST_URL").ok()?;
    if raw_url.contains("table_name=") {
        Some(raw_url)
    } else if raw_url.contains('?') {
        Some(format!("{raw_url}&table_name=kv_test"))
    } else {
        Some(format!("{raw_url}?table_name=kv_test"))
    }
}

#[tokio::test]
async fn surreal_kv_basic_crud() {
    let Some(url) = test_url() else {
        eprintln!("skipped (PG_TEST_URL not set)");
        return;
    };

    let ds = build_ds(&url).await.expect("failed to build datastore");
    let sess = Session::owner().with_ns("crud_test").with_db("crud_test");

    // Ensure namespace, database, and table exist
    ds.execute("DEFINE NAMESPACE crud_test", &sess, None)
        .await
        .ok();
    ds.execute("DEFINE DATABASE crud_test", &sess, None)
        .await
        .ok();
    ds.execute("DEFINE TABLE person SCHEMAFULL", &sess, None)
        .await
        .ok();
    ds.execute("DEFINE FIELD name ON person TYPE string", &sess, None)
        .await
        .ok();
    ds.execute("DEFINE FIELD age ON person TYPE int", &sess, None)
        .await
        .ok();

    // Clean up any stale data from previous runs
    ds.execute("DELETE FROM person", &sess, None).await.ok();

    // CREATE
    let result = ds
        .execute("CREATE person SET name = 'Alice', age = 30", &sess, None)
        .await;
    assert!(result.is_ok(), "CREATE should succeed: {:?}", result.err());

    // SELECT
    let mut response = ds
        .execute("SELECT * FROM person WHERE name = 'Alice'", &sess, None)
        .await
        .expect("SELECT failed");
    let records = response.remove(0).result.expect("query result");
    assert!(!records.is_empty(), "should find Alice");

    // UPDATE
    let result = ds
        .execute(
            "UPDATE person SET age = 31 WHERE name = 'Alice'",
            &sess,
            None,
        )
        .await;
    assert!(result.is_ok(), "UPDATE should succeed: {:?}", result.err());

    // Verify update
    let mut response = ds
        .execute("SELECT age FROM person WHERE name = 'Alice'", &sess, None)
        .await
        .expect("SELECT after update failed");
    let records = response.remove(0).result.expect("query result");
    assert!(!records.is_empty(), "should find Alice after update");

    // DELETE
    let result = ds
        .execute("DELETE FROM person WHERE name = 'Alice'", &sess, None)
        .await;
    assert!(result.is_ok(), "DELETE should succeed: {:?}", result.err());

    // Verify deletion
    let mut response = ds
        .execute("SELECT * FROM person WHERE name = 'Alice'", &sess, None)
        .await
        .expect("SELECT after delete failed");
    let records = response.remove(0).result.expect("query result");
    assert!(records.is_empty(), "Alice should be gone");

    ds.shutdown().await.ok();
}

#[tokio::test]
async fn surreal_kv_transaction_rollback() {
    let Some(url) = test_url() else {
        eprintln!("skipped (PG_TEST_URL not set)");
        return;
    };

    let ds = build_ds(&url).await.expect("failed to build datastore");
    let sess = Session::owner()
        .with_ns("rollback_test")
        .with_db("rollback_test");

    // Ensure namespace, database, and table exist
    ds.execute("DEFINE NAMESPACE rollback_test", &sess, None)
        .await
        .ok();
    ds.execute("DEFINE DATABASE rollback_test", &sess, None)
        .await
        .ok();
    ds.execute("DEFINE TABLE tx_test SCHEMAFULL", &sess, None)
        .await
        .ok();
    ds.execute("DEFINE FIELD val ON tx_test TYPE int", &sess, None)
        .await
        .ok();

    // Clean up any existing data
    ds.execute("DELETE FROM tx_test", &sess, None).await.ok();

    // Begin transaction, create data, then rollback
    let query = "BEGIN; CREATE tx_test SET val = 1; CANCEL;";
    let result = ds.execute(query, &sess, None).await;
    assert!(
        result.is_ok(),
        "transaction should execute: {:?}",
        result.err()
    );

    // Verify data was rolled back
    let mut response = ds
        .execute("SELECT * FROM tx_test", &sess, None)
        .await
        .expect("SELECT after rollback failed");
    let records = response.remove(0).result.expect("query result");
    assert!(records.is_empty(), "tx_test should be empty after rollback");

    ds.shutdown().await.ok();
}
