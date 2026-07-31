//! Integration tests for the PostgreSQL storage backend.
//!
//! These tests exercise the raw KV layer (PgStore / PgTransaction) directly,
//! without going through the SurrealDB engine. They run automatically when
//! `PG_TEST_URL` is set, and are skipped otherwise.

#![allow(clippy::unwrap_used)]
//!
//! Run against local PG:
//!   PG_TEST_URL='postgres://...' cargo test
//!
//! Run against pgbouncer / Supabase Pooler (transaction mode):
//!   PG_TEST_URL='postgresql://...' cargo test -- --test-threads=1

use std::sync::Arc;

use surreal_pg::store::PgStore;

/// Type alias for test case functions to avoid clippy type_complexity warning.
type TestCase = fn(&Arc<PgStore>) -> futures::future::BoxFuture<'_, Result<(), String>>;

// ─── Public entry point ──────────────────────────────────

/// Run the full integration test suite.
///
/// Returns `(passed, failed)` counts.  Each failure is printed to stderr.
pub async fn run(store: &Arc<PgStore>) -> (u32, u32) {
    let cases: &[(&str, TestCase)] = &[
        ("basic CRUD", test_basic_crud),
        ("put (insert-if-absent)", test_put),
        ("range scan + delete", test_range_scan_and_delete),
        ("savepoint rollback", test_savepoint_rollback),
        ("CAS (putc)", test_putc),
        ("namespace isolation (key prefix)", test_namespace_isolation),
        ("exists + getm", test_exists_and_getm),
        ("delc (compare-and-delete)", test_delc),
        ("keys / keysr direction", test_keys_direction),
        ("read-only tx rejects writes", test_read_only_rejects_writes),
    ];

    let mut passed = 0u32;
    let mut failed = 0u32;

    for (name, case_fn) in cases {
        match case_fn(store).await {
            Ok(()) => {
                println!("[PASS] {name}");
                passed += 1;
            }
            Err(e) => {
                eprintln!("[FAIL] {name} — {e}");
                failed += 1;
            }
        }
    }

    // Write a marker record so the user can verify data reached PG.
    let mut tx = match store.begin(true).await {
        Ok(tx) => tx,
        Err(e) => {
            eprintln!("[WARN] failed to write marker: {e}");
            return (passed, failed);
        }
    };
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let marker_val = format!("surreal-pg test suite — {passed} passed, {failed} failed @ {now}");
    if let Err(e) = tx
        .set(b"test:marker".to_vec(), marker_val.into_bytes())
        .await
    {
        eprintln!("[WARN] failed to write marker: {e}");
    }
    let _ = tx.commit().await;

    (passed, failed)
}

// ─── Helpers ──────────────────────────────────────────────

async fn set_key(store: &Arc<PgStore>, key: &[u8], val: &[u8]) -> Result<(), String> {
    let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
    tx.set(key.to_vec(), val.to_vec())
        .await
        .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn get_key(store: &Arc<PgStore>, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let mut tx = store.begin(false).await.map_err(|e| e.to_string())?;
    let val = tx.get(key.to_vec()).await.map_err(|e| e.to_string())?;
    tx.cancel().await.map_err(|e| e.to_string())?;
    Ok(val)
}

async fn del_key(store: &Arc<PgStore>, key: &[u8]) -> Result<(), String> {
    let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
    tx.del(key.to_vec()).await.map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn clean_all(store: &Arc<PgStore>) {
    let mut tx = store.begin(true).await.unwrap();
    tx.delr(vec![]..vec![0xFF]).await.ok();
    tx.commit().await.ok();
}

// ─── Test cases ───────────────────────────────────────────

fn test_basic_crud(store: &Arc<PgStore>) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        clean_all(store).await;

        set_key(store, b"key1", b"val1").await?;
        let val = get_key(store, b"key1").await?;
        assert_eq!(val, Some(b"val1".to_vec()), "basic set/get");

        del_key(store, b"key1").await?;
        let val = get_key(store, b"key1").await?;
        assert_eq!(val, None, "key should be gone after del");

        Ok(())
    })
}

fn test_put(store: &Arc<PgStore>) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        clean_all(store).await;

        let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
        tx.put(b"test:unique".to_vec(), b"v1".to_vec())
            .await
            .map_err(|e| e.to_string())?;
        let res = tx.put(b"test:unique".to_vec(), b"v2".to_vec()).await;
        assert!(res.is_err(), "put on existing key should fail");
        tx.commit().await.map_err(|e| e.to_string())?;

        del_key(store, b"test:unique").await?;
        Ok(())
    })
}

fn test_range_scan_and_delete(
    store: &Arc<PgStore>,
) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        clean_all(store).await;

        // Seed
        {
            let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
            tx.set(b"test:range:a".to_vec(), b"1".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.set(b"test:range:b".to_vec(), b"2".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.set(b"test:range:c".to_vec(), b"3".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
        }

        // Scan
        {
            let mut tx = store.begin(false).await.map_err(|e| e.to_string())?;
            let pairs = tx
                .scan(b"test:range:".to_vec()..b"test:range:z".to_vec(), 10, 0)
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(pairs.len(), 3, "scan should return 3 rows");
            let cnt = tx
                .count(b"test:range:".to_vec()..b"test:range:z".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(cnt, 3, "count should be 3");
            tx.cancel().await.map_err(|e| e.to_string())?;
        }

        // Range delete
        {
            let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
            tx.delr(b"test:range:".to_vec()..b"test:range:z".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
        }

        {
            let mut tx = store.begin(false).await.map_err(|e| e.to_string())?;
            let cnt = tx
                .count(b"test:range:".to_vec()..b"test:range:z".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(cnt, 0, "count should be 0 after delr");
            tx.cancel().await.map_err(|e| e.to_string())?;
        }

        Ok(())
    })
}

fn test_savepoint_rollback(
    store: &Arc<PgStore>,
) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        clean_all(store).await;

        let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
        tx.set(b"test:sp".to_vec(), b"v1".to_vec())
            .await
            .map_err(|e| e.to_string())?;
        tx.new_save_point().await.map_err(|e| e.to_string())?;
        tx.set(b"test:sp".to_vec(), b"v2".to_vec())
            .await
            .map_err(|e| e.to_string())?;
        tx.rollback_to_save_point()
            .await
            .map_err(|e| e.to_string())?;
        let val = tx
            .get(b"test:sp".to_vec())
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(val, Some(b"v1".to_vec()), "rollback should restore v1");
        tx.commit().await.map_err(|e| e.to_string())?;

        del_key(store, b"test:sp").await?;
        Ok(())
    })
}

fn test_putc(store: &Arc<PgStore>) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        clean_all(store).await;

        let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
        tx.set(b"test:cas".to_vec(), b"expected".to_vec())
            .await
            .map_err(|e| e.to_string())?;

        // putc with matching check
        tx.putc(
            b"test:cas".to_vec(),
            b"new_val".to_vec(),
            Some(b"expected".to_vec()),
        )
        .await
        .map_err(|e| e.to_string())?;
        let val = tx
            .get(b"test:cas".to_vec())
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(val, Some(b"new_val".to_vec()), "putc should update value");

        // putc with wrong check → should fail
        let res = tx
            .putc(
                b"test:cas".to_vec(),
                b"wrong".to_vec(),
                Some(b"expected".to_vec()),
            )
            .await;
        assert!(res.is_err(), "putc with wrong check should fail");

        tx.commit().await.map_err(|e| e.to_string())?;
        del_key(store, b"test:cas").await?;
        Ok(())
    })
}

fn test_namespace_isolation(
    store: &Arc<PgStore>,
) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        clean_all(store).await;

        let ns_a_key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*users\0*alice".to_vec();
        let ns_b_key = b"/*\x00\x00\x00\x03*\x00\x00\x00\x04*users\0*alice".to_vec();

        {
            let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
            tx.set(ns_a_key.clone(), b"ns_a_data".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.set(ns_b_key.clone(), b"ns_b_data".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
        }

        {
            let mut tx = store.begin(false).await.map_err(|e| e.to_string())?;
            let val_a = tx.get(ns_a_key.clone()).await.map_err(|e| e.to_string())?;
            let val_b = tx.get(ns_b_key.clone()).await.map_err(|e| e.to_string())?;
            assert_eq!(val_a, Some(b"ns_a_data".to_vec()));
            assert_eq!(val_b, Some(b"ns_b_data".to_vec()));
            tx.cancel().await.map_err(|e| e.to_string())?;
        }

        // Range scan by ns prefix
        {
            let mut tx = store.begin(false).await.map_err(|e| e.to_string())?;
            let pairs = tx
                .scan(
                    b"/*\x00\x00\x00\x01".to_vec()..b"/*\x00\x00\x00\x02".to_vec(),
                    100,
                    0,
                )
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(pairs.len(), 1, "ns_a range should have 1 entry");
            tx.cancel().await.map_err(|e| e.to_string())?;
        }

        {
            let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
            tx.del(ns_a_key).await.map_err(|e| e.to_string())?;
            tx.del(ns_b_key).await.map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
        }

        Ok(())
    })
}

fn test_exists_and_getm(
    store: &Arc<PgStore>,
) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        clean_all(store).await;

        // exists on missing key
        {
            let mut tx = store.begin(false).await.map_err(|e| e.to_string())?;
            let exists = tx
                .exists(b"test:nope".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            assert!(!exists, "missing key should not exist");
            tx.cancel().await.map_err(|e| e.to_string())?;
        }

        // set then exists + getm
        {
            let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
            tx.set(b"test:exists1".to_vec(), b"v".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.set(b"test:exists2".to_vec(), b"v".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
        }

        {
            let mut tx = store.begin(false).await.map_err(|e| e.to_string())?;
            let exists = tx
                .exists(b"test:exists1".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            assert!(exists, "existing key should exist");

            let vals = tx
                .getm(vec![
                    b"test:exists1".to_vec(),
                    b"test:exists2".to_vec(),
                    b"test:missing".to_vec(),
                ])
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(vals.len(), 3);
            assert_eq!(vals[0], Some(b"v".to_vec()));
            assert_eq!(vals[1], Some(b"v".to_vec()));
            assert_eq!(vals[2], None);
            tx.cancel().await.map_err(|e| e.to_string())?;
        }

        del_key(store, b"test:exists1").await?;
        del_key(store, b"test:exists2").await?;
        Ok(())
    })
}

fn test_delc(store: &Arc<PgStore>) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        clean_all(store).await;

        let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
        tx.set(b"test:delc".to_vec(), b"v1".to_vec())
            .await
            .map_err(|e| e.to_string())?;

        // delc with wrong check
        let res = tx
            .delc(b"test:delc".to_vec(), Some(b"wrong".to_vec()))
            .await;
        assert!(res.is_err(), "delc with wrong check should fail");

        let val = tx
            .get(b"test:delc".to_vec())
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(val, Some(b"v1".to_vec()), "key should survive wrong delc");

        // delc with correct check
        tx.delc(b"test:delc".to_vec(), Some(b"v1".to_vec()))
            .await
            .map_err(|e| e.to_string())?;
        let val = tx
            .get(b"test:delc".to_vec())
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(val, None, "key should be gone after correct delc");

        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(())
    })
}

fn test_keys_direction(store: &Arc<PgStore>) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        clean_all(store).await;

        // Seed
        {
            let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
            tx.set(b"test:scan:a".to_vec(), b"1".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.set(b"test:scan:b".to_vec(), b"2".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.set(b"test:scan:c".to_vec(), b"3".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
        }

        {
            let mut tx = store.begin(false).await.map_err(|e| e.to_string())?;

            // ASC
            let keys = tx
                .keys(b"test:scan:".to_vec()..b"test:scan:z".to_vec(), 10, 0)
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0], b"test:scan:a");
            assert_eq!(keys[2], b"test:scan:c");

            // DESC
            let keys = tx
                .keysr(b"test:scan:".to_vec()..b"test:scan:z".to_vec(), 10, 0)
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0], b"test:scan:c");
            assert_eq!(keys[2], b"test:scan:a");

            tx.cancel().await.map_err(|e| e.to_string())?;
        }

        {
            let mut tx = store.begin(true).await.map_err(|e| e.to_string())?;
            tx.delr(b"test:scan:".to_vec()..b"test:scan:z".to_vec())
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
        }

        Ok(())
    })
}

fn test_read_only_rejects_writes(
    store: &Arc<PgStore>,
) -> futures::future::BoxFuture<'_, Result<(), String>> {
    Box::pin(async {
        let mut tx = store.begin(false).await.map_err(|e| e.to_string())?;
        let res = tx.set(b"test:ro".to_vec(), b"v".to_vec()).await;
        assert!(res.is_err(), "write on read-only tx should fail");
        tx.cancel().await.map_err(|e| e.to_string())?;
        Ok(())
    })
}

// ─── Test runner ─────────────────────────────────────────

/// Ensure the test URL targets `kv_test` so we never touch production data.
fn ensure_test_table(url: &str) -> String {
    if url.contains("table_name=") {
        url.to_string()
    } else if url.contains('?') {
        format!("{url}&table_name=kv_test")
    } else {
        format!("{url}?table_name=kv_test")
    }
}

#[tokio::test]
async fn integration_test_suite() {
    match std::env::var("PG_TEST_URL") {
        Ok(raw_url) => {
            let url = ensure_test_table(&raw_url);
            let store = PgStore::new(&url).await.unwrap();

            // Verify persistent-statements auto-detection produced a sane result.
            // The test runs against Supabase Pooler (pgbouncer transaction mode
            // on port 6543), so persistent should be resolved to `false`.
            // Against direct PG (port 5432), it should be `true`.
            println!("persistent-statements resolved to: {}", store.persistent());

            let (passed, failed) = run(&store).await;
            store.shutdown().await;
            assert_eq!(failed, 0, "{passed} passed, {failed} failed");
        }
        Err(_) => eprintln!("skipped (PG_TEST_URL not set)"),
    }
}
