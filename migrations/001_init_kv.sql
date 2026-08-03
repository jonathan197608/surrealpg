-- Migration 001: Initialize the kv table for SurrealDB PostgreSQL storage backend
--
-- This migration creates the single key-value table that backs the entire
-- SurrealDB datastore. The `key` column stores the encoded namespace path
-- (e.g. /*{ns_id}*{db_id}*{table_id}*{record_id}), and `val` stores the
-- serialized record value.

CREATE TABLE IF NOT EXISTS kv (
    key BYTEA PRIMARY KEY,
    val BYTEA NOT NULL
);

-- ── Performance tuning (optional but recommended) ─────────────────────────
--
-- If you use this migration script instead of letting surreal-pg auto-create
-- the table (auto_create_table = true), apply the following tuning for
-- optimal write-heavy KV performance. These match PgTuneConfig defaults.
--
-- -- Reduce page fill to leave room for in-place updates ( HOT updates ):
-- ALTER TABLE kv SET (fillfactor = 90);
--
-- -- Store large values out-of-line (reduces TOAST table scan overhead):
-- ALTER TABLE kv ALTER COLUMN val SET STORAGE external;
--
-- -- Autovacuum tuning for high-churn KV workloads (matches PgTuneConfig defaults):
-- ALTER TABLE kv SET (
--     autovacuum_enabled = true,
--     autovacuum_vacuum_scale_factor = 0.05,
--     autovacuum_vacuum_threshold = 50,
--     autovacuum_analyze_scale_factor = 0.02,
--     autovacuum_analyze_threshold = 50
-- );
--
-- See `PgTuneConfig` and the `PG_TUNED_*` environment variables for the full
-- set of tuning parameters and their defaults.
