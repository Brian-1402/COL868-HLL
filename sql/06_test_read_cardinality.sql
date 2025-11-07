-- ============================================================
-- 06_test_read_cardinality.sql (pgbench script)
--
-- Tests hll_cardinality() read QPS (Queries Per Second).
--
-- Reads from the pre-populated aggregate tables.
-- ============================================================

-- \set num_days 30 (from setup script)
\set day_id random(0, 29)
\set date_key ('2025-01-01'::date + (:day_id * '1 day'::interval))

-- Test read from default and high-accuracy tables
SELECT hll_cardinality(users) FROM daily_uniques_default WHERE date_key = :date_key;
SELECT hll_cardinality(users) FROM daily_uniques_high_accuracy WHERE date_key = :date_key;
