-- ============================================================
-- 99_cleanup.sql
--
-- Drops all tables, types, and results tables created
-- by the benchmark suite.
--
-- CORRECTIONS:
-- - Removed tables that were dropped from setup
--   (daily_uniques_no_explicit, daily_uniques_no_sparse)
-- ============================================================

\echo '>>> Cleaning up all benchmark tables...'

-- Data tables
DROP TABLE IF EXISTS facts CASCADE;
DROP TABLE IF EXISTS daily_uniques_default CASCADE;
DROP TABLE IF EXISTS daily_uniques_high_accuracy CASCADE;
DROP TABLE IF EXISTS live_hll_test CASCADE;

-- Results tables
DROP TABLE IF EXISTS results_bulk_exact CASCADE;
DROP TABLE IF EXISTS results_bulk_hll CASCADE;
DROP TABLE IF EXISTS results_storage CASCADE;
DROP TABLE IF EXISTS results_hashing CASCADE;

\echo '>>> Cleanup complete.'
