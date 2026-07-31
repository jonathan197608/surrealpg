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
