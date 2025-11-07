-- ============================================================
-- 07_test_read_union.sql (pgbench script)
--
-- Tests hll_union_agg() QPS (Queries Per Second).
-- This simulates the "sliding window" use case.
--
-- Reads from the pre-populated aggregate tables.
--
-- CORRECTIONS:
-- - Fixed pgbench syntax error. `\set` cannot evaluate
--   complex expressions with casts.
-- - Removed the `\set` commands for start_date and end_date.
-- - Moved the date arithmetic directly into the WHERE clause,
--   using the :start_day variable.
-- ============================================================

-- \set num_days 30 (from setup script)
-- We'll query a 7-day window.
-- With 30 days (0-29), the last possible 7-day window
-- starts on day 23 (23, 24, 25, 26, 27, 28, 29).
-- So, random(0, 23).
\set start_day random(0, 23)

-- Test union from default and high-accuracy tables
SELECT hll_cardinality(hll_union_agg(users))
FROM daily_uniques_default
WHERE date_key >= ('2025-01-01'::date + (:start_day * '1 day'::interval))
  AND date_key <  ('2025-01-01'::date + ((:start_day + 7) * '1 day'::interval));

SELECT hll_cardinality(hll_union_agg(users))
FROM daily_uniques_high_accuracy
WHERE date_key >= ('2025-01-01'::date + (:start_day * '1 day'::interval))
  AND date_key <  ('2025-01-01'::date + ((:start_day + 7) * '1 day'::interval));
