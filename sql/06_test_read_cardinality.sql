-- ============================================================
-- 06_test_read_cardinality.sql (pgbench script)
--
-- Tests hll_cardinality() read QPS (Queries Per Second).
--
-- Reads from the pre-populated aggregate tables.
--
-- CORRECTIONS:
-- - Fixed pgbench syntax error. `\set` cannot evaluate
--   complex expressions with casts.
-- - Removed the second `\set` command for `date_key`.
-- - Moved the date arithmetic directly into the WHERE clause,
--   using the :day_id variable.
-- ============================================================

-- \set num_days 30 (from setup script)
-- pgbench's random() is zero-based and inclusive
\set day_id random(0, 29)

-- Test read from default and high-accuracy tables
-- The date arithmetic is now done by the server.
SELECT hll_cardinality(users) FROM daily_uniques_default
WHERE date_key = ('2025-01-01'::date + (:day_id * '1 day'::interval));

SELECT hll_cardinality(users) FROM daily_uniques_high_accuracy
WHERE date_key = ('2025-01-01'::date + (:day_id * '1 day'::interval));
