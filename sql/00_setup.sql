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
-- ============================================================

-- --- Configuration ---
-- Set these variables for data size.
-- Default is a "fast" run (100k rows, ~30s setup).
-- For a full run, increase total_rows to 10,000,000+.
-- For hll configs, the parameter order is: hll(log2m, regwidth, expthresh, sparseon)
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
-- hll(log2m=11, regwidth=5, expthresh=-1, sparseon=1)
CREATE TABLE daily_uniques_default (
  date_key    DATE PRIMARY KEY,
  users       hll
);

-- Aggregate table with high accuracy settings
-- hll(log2m=14, regwidth=5)
CREATE TABLE daily_uniques_high_accuracy (
  date_key    DATE PRIMARY KEY,
  users       hll(14, 5)
);

-- Aggregate table, 'explicit' representation disabled
-- hll(log2m=11, regwidth=5, expthresh=0)
CREATE TABLE daily_uniques_no_explicit (
  date_key    DATE PRIMARY KEY,
  users       hll(11, 5, 0)
  -- users       hll(log2m=11, regwidth=5, expthresh=0)
);

-- Aggregate table, 'explicit' and 'sparse' disabled
-- hll(log2m=11, regwidth=5, expthresh=0, sparseon=0)
CREATE TABLE daily_uniques_no_sparse (
  date_key    DATE PRIMARY KEY,
  users       hll(11, 5, 0, 0)
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
    hll_add_agg(hll_hash_bigint(user_id))
  FROM facts
  GROUP BY 1;
ANALYZE daily_uniques_default;

\echo '>>> [5/7] Pre-populating aggregate table: daily_uniques_high_accuracy'
INSERT INTO daily_uniques_high_accuracy(date_key, users)
  SELECT 
    date_trunc('day', visit_time)::date, 
    -- hll_add_agg(hll_hash_bigint(user_id))
	-- hll_union_agg(hll_add(hll_empty()::hll(14, 5), hll_hash_bigint(user_id)))
	hll_union_agg(hll_add(hll_empty()::hll(log2m=14, regwidth=5), hll_hash_bigint(user_id)))
  FROM facts
  GROUP BY 1;
ANALYZE daily_uniques_high_accuracy;

-- Note: We only pre-populate the two main tables for read/union tests.
-- The other types ('no_explicit', 'no_sparse') will be tested
-- in the point-insert and storage tests.

-- ============================================================
-- 4. Setup Live Insert Table
-- ============================================================

\echo '>>> [6/7] Initializing live_hll_test table...'
-- We insert hll_empty() with the correct type casts
INSERT INTO live_hll_test (test_type, hll_set) VALUES
    ('default', hll_empty()),
    ('high_accuracy', hll_empty()::hll(log2m=14, regwidth=5)),
    ('no_explicit', hll_empty()::hll(11, 5, 0)),
    ('no_sparse', hll_empty()::hll(11, 5, 0, 0));

ANALYZE live_hll_test;

\echo '>>> [7/7] Setup complete.'
\timing off
