-- ============================================================
-- 02_test_storage.sql
--
-- Tests the storage footprint (in bytes) of HLL objects
-- as they transition through internal states:
-- 1. EMPTY (0 items)
-- 2. EXPLICIT (~10 items)
-- 3. SPARSE (~500 items)
-- 4. FULL (~10000 items)
--
-- It tests this against the different HLL type definitions
-- to show how parameters affect storage.
-- ============================================================

\echo '>>> Test 02: Storage Footprint...'

-- Create results table
DROP TABLE IF EXISTS results_storage;
CREATE TABLE results_storage (
    hll_type TEXT,
    item_count BIGINT,
    storage_bytes INTEGER
);

-- Use a temporary table for this test
DROP TABLE IF EXISTS storage_test_table;
CREATE TABLE storage_test_table (
    hll_type TEXT PRIMARY KEY,
    hll_set hll
);

-- Insert empty HLLs of each type
INSERT INTO storage_test_table (hll_type, hll_set) VALUES
    ('default', hll_empty()),
    ('high_accuracy_p14', hll_empty()::hll(log2m=14)),
    ('no_explicit', hll_empty()::hll(expthresh=0)),
    ('no_sparse', hll_empty()::hll(expthresh=0, sparseon=0));

-- --- 1. Test EMPTY state ---
RAISE NOTICE '    Testing EMPTY storage...';
INSERT INTO results_storage
SELECT hll_type, 0, pg_column_size(hll_set) FROM storage_test_table;

-- --- 2. Test EXPLICIT state ---
RAISE NOTICE '    Testing EXPLICIT storage (10 items)...';
-- Add 10 distinct items to each HLL
UPDATE storage_test_table
SET hll_set = (
    SELECT hll_union_agg(hll_add(hll_empty(), hll_hash_integer(s.n)))
    FROM generate_series(1, 10) s(n)
);
INSERT INTO results_storage
SELECT hll_type, 10, pg_column_size(hll_set) FROM storage_test_table;

-- --- 3. Test SPARSE state ---
RAISE NOTICE '    Testing SPARSE storage (500 items)...';
UPDATE storage_test_table
SET hll_set = (
    SELECT hll_union_agg(hll_add(hll_empty(), hll_hash_integer(s.n)))
    FROM generate_series(1, 500) s(n)
);
INSERT INTO results_storage
SELECT hll_type, 500, pg_column_size(hll_set) FROM storage_test_table;

-- --- 4. Test FULL state ---
RAISE NOTICE '    Testing FULL storage (10000 items)...';
UPDATE storage_test_table
SET hll_set = (
    SELECT hll_union_agg(hll_add(hll_empty(), hll_hash_integer(s.n)))
    FROM generate_series(1, 10000) s(n)
);
INSERT INTO results_storage
SELECT hll_type, 10000, pg_column_size(hll_set) FROM storage_test_table;

-- --- 5. Export Results ---
\echo '>>> Test 02: Exporting results...'
\copy results_storage TO '/tmp/hll_bench_outputs/02_results_storage.csv' CSV HEADER;

-- Cleanup
DROP TABLE storage_test_table;

\echo '>>> Test 02: Complete.'
