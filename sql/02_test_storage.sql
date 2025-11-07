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
--
-- CORRECTIONS:
-- - Replaced all `RAISE NOTICE` calls with `\echo`.
--   `RAISE NOTICE` is PL/pgSQL, `\echo` is psql.
-- ============================================================

\echo '>>> Test 02: Storage Footprint...'

-- Create results table
DROP TABLE IF EXISTS results_storage;
CREATE TABLE results_storage (
    hll_type TEXT,
    item_count BIGINT,
    storage_bytes INTEGER
);

-- --- 1. Test EMPTY state ---
\echo '    Testing EMPTY storage...'
INSERT INTO results_storage (hll_type, item_count, storage_bytes) VALUES
    ('default', 0, pg_column_size(hll_empty())),
    ('high_accuracy_p14', 0, pg_column_size(hll_empty(14, 5))),
    ('no_explicit', 0, pg_column_size(hll_empty(11, 5, 0))),
    ('no_sparse', 0, pg_column_size(hll_empty(11, 5, 0, 0)));

-- --- 2. Test EXPLICIT state ---
\echo '    Testing EXPLICIT storage (10 items)...'
INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'default', 10, pg_column_size(hll_add_agg(hll_hash_integer(s.n)))
FROM generate_series(1, 10) s(n);

INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'high_accuracy_p14', 10, pg_column_size(hll_add_agg(hll_hash_integer(s.n), 14, 5))
FROM generate_series(1, 10) s(n);

INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'no_explicit', 10, pg_column_size(hll_add_agg(hll_hash_integer(s.n), 11, 5, 0))
FROM generate_series(1, 10) s(n);

INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'no_sparse', 10, pg_column_size(hll_add_agg(hll_hash_integer(s.n), 11, 5, 0, 0))
FROM generate_series(1, 10) s(n);


-- --- 3. Test SPARSE state ---
\echo '    Testing SPARSE storage (500 items)...'
INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'default', 500, pg_column_size(hll_add_agg(hll_hash_integer(s.n)))
FROM generate_series(1, 500) s(n);

INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'high_accuracy_p14', 500, pg_column_size(hll_add_agg(hll_hash_integer(s.n), 14, 5))
FROM generate_series(1, 500) s(n);

INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'no_explicit', 500, pg_column_size(hll_add_agg(hll_hash_integer(s.n), 11, 5, 0))
FROM generate_series(1, 500) s(n);

INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'no_sparse', 500, pg_column_size(hll_add_agg(hll_hash_integer(s.n), 11, 5, 0, 0))
FROM generate_series(1, 500) s(n);

-- --- 4. Test FULL state ---
\echo '    Testing FULL storage (10000 items)...'
INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'default', 10000, pg_column_size(hll_add_agg(hll_hash_integer(s.n)))
FROM generate_series(1, 10000) s(n);

INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'high_accuracy_p14', 10000, pg_column_size(hll_add_agg(hll_hash_integer(s.n), 14, 5))
FROM generate_series(1, 10000) s(n);

INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'no_explicit', 10000, pg_column_size(hll_add_agg(hll_hash_integer(s.n), 11, 5, 0))
FROM generate_series(1, 10000) s(n);

INSERT INTO results_storage (hll_type, item_count, storage_bytes)
SELECT 'no_sparse', 10000, pg_column_size(hll_add_agg(hll_hash_integer(s.n), 11, 5, 0, 0))
FROM generate_series(1, 10000) s(n);

-- --- 5. Export Results ---
\echo '>>> Test 02: Exporting results...'
\copy results_storage TO '/tmp/hll_bench_outputs/02_results_storage.csv' CSV HEADER;

\echo '>>> Test 02: Complete.'
