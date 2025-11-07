-- ============================================================
-- 05_test_point_insert_high_card.sql (pgbench script)
--
-- Tests hll_add() throughput for HIGH cardinality inserts.
-- This is designed to force the HLLs into the 'FULL'
-- representation and test its performance.
--
-- Runs against the 'live_hll_test' table.
-- ============================================================

\set user_id random(1, 2000000000)

-- We test inserts into all four HLL types created in setup
BEGIN;
    UPDATE live_hll_test SET hll_set = hll_add(hll_set, hll_hash_integer(:user_id)) WHERE test_type = 'default';
    UPDATE live_hll_test SET hll_set = hll_add(hll_set, hll_hash_integer(:user_id)) WHERE test_type = 'high_accuracy_p14';
    UPDATE live_hll_test SET hll_set = hll_add(hll_set, hll_hash_integer(:user_id)) WHERE test_type = 'no_explicit';
    UPDATE live_hll_test SET hll_set = hll_add(hll_set, hll_hash_integer(:user_id)) WHERE test_type = 'no_sparse';
COMMIT;
