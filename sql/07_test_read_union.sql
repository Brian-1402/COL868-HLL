-- ============================================================
-- 07_test_read_union.sql (pgbench script)
--
-- Tests hll_union_agg() QPS (Queries Per Second).
-- This simulates the "sliding window" use case.
--
-- Reads from the pre-populated aggregate tables.
-- ============================================================

-- \set num_days 30 (from setup script)
-- We'll query a 7-day window.
\set start_day random(0, 22) -- (30 - 7 = 23)
\set start_date ('2025-01-01'::date + (:start_day * '1 day'::interval))
\set end_date ('2025-01-01'::date + ((:start_day + 7) * '1 day'::interval))

-- Test union from default and high-accuracy tables
SELECT hll_cardinality(hll_union_agg(users))
FROM daily_uniques_default
WHERE date_key >= :start_date AND date_key < :end_date;

SELECT hll_cardinality(hll_union_agg(users))
FROM daily_uniques_high_accuracy
WHERE date_key >= :start_date AND date_key < :end_date;
