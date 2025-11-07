-- ============================================================
-- 00_setup.sql
--
-- Sets up the database for HLL benchmarking.
-- 1. Enables the 'hll' extension.
-- 2. Creates raw 'facts' table.
-- 3. Creates various aggregate HLL tables to test parameters.
-- 4. Creates a table for 'live' point-insert tests.
-- 5. Populates 'facts' with configurable test data.
-- 6. Pre-populates aggregate tables for read tests.
--
-- CORRECTIONS:
-- - Changed `hll(log2m=...)` column type to just `hll`.
--   The HLL object itself stores its parameters.
-- - Corrected `INSERT` into `daily_uniques_high_accuracy`
--   to use `hll_add_agg(hashval, 14, 5)` to set parameters.
-- - Corrected `INSERT` into `live_hll_test` to use
--   `hll_empty(log2m, ...)` instead of casting.
-- ============================================================

-- --- Configuration ---
-- Set these variables for data size.
-- Default is a "fast" run (100k rows, ~30s setup).
-- For a full run, increase total_rows to 10,000,000+.
\set total_rows 100000
\set distinct_users 10000
\set num_days 30
-- --- End Configuration ---

\timing on

\echo '>>> [0/7] Enabling extension...'
CREATE EXTENSION IF NOT EXISTS hll;

-- ============================================================
-- 1. Create Tables
-- ============================================================

\echo '>>> [1/7] Creating tables...'

-- Raw data table
CREATE TABLE facts (
  visit_time  TIMESTAMPTZ,
  user_id     BIGINT
);

-- Aggregate table with default HLL settings
CREATE TABLE daily_uniques_default (
  date_key    DATE PRIMARY KEY,
  users       hll -- HLL object parameters are self-contained
);

-- Aggregate table with high accuracy settings
CREATE TABLE daily_uniques_high_accuracy (
  date_key    DATE PRIMARY KEY,
  users       hll -- HLL object parameters are self-contained
);

-- Table for point-insert (hll_add) tests
CREATE TABLE live_hll_test (
  test_type   TEXT PRIMARY KEY,
  hll_set     hll
);

-- ============================================================
-- 2. Generate Raw Data
-- ============================================================

\echo '>>> [2/7] Generating ' :total_rows ' rows...'
INSERT INTO facts (visit_time, user_id)
SELECT
    '2025-01-01'::timestamptz + (n % :num_days) * '1 day'::interval + (random() * 86400) * '1 second'::interval,
    (random() * :distinct_users)::bigint
FROM generate_series(1, :total_rows) s(n);

\echo '>>> [3/7] Analyzing facts table...'
ANALYZE facts;

-- =V==========================================================
-- 3. Pre-populate Aggregate Tables (for read tests)
-- ============================================================

\echo '>>> [4/7] Pre-populating aggregate table: daily_uniques_default'
INSERT INTO daily_uniques_default(date_key, users)
  SELECT 
    date_trunc('day', visit_time)::date, 
    hll_add_agg(hll_hash_bigint(user_id)) -- Use default parameters
  FROM facts
  GROUP BY 1;
ANALYZE daily_uniques_default;

\echo '>>> [5/7] Pre-populating aggregate table: daily_uniques_high_accuracy'
INSERT INTO daily_uniques_high_accuracy(date_key, users)
  SELECT 
    date_trunc('day', visit_time)::date, 
    hll_add_agg(hll_hash_bigint(user_id), 14, 5) -- Use log2m=14, regwidth=5
  FROM facts
  GROUP BY 1;
ANALYZE daily_uniques_high_accuracy;

-- ============================================================
-- 4. Setup Live Insert Table
-- ============================================================

\echo '>>> [6/7] Initializing live_hll_test table...'
-- We insert hll_empty() with the correct parameters
INSERT INTO live_hll_test (test_type, hll_set) VALUES
    ('default', hll_empty()), -- log2m=11, regwidth=5, expthresh=-1, sparseon=1
    ('high_accuracy_p14', hll_empty(14, 5)), -- log2m=14, regwidth=5
    ('no_explicit', hll_empty(11, 5, 0)), -- log2m=11, regwidth=5, expthresh=0
    ('no_sparse', hll_empty(11, 5, 0, 0)) -- ... expthresh=0, sparseon=0
ON CONFLICT (test_type) DO NOTHING;

ANALYZE live_hll_test;

\echo '>>> [7/7] Setup complete.'
\timing off
