-- ============================================================
-- 01_test_bulk_agg.sql
--
-- Tests bulk aggregation performance and accuracy.
-- 1. Compares COUNT(DISTINCT) vs hll_add_agg().
-- 2. Tests hll_add_agg() against different HLL type definitions.
-- 3. Runs 5 times for stability.
-- 4. Exports results to CSV.
--
-- Note: This test reads from the 'facts' table.
--
-- CORRECTIONS:
-- - Fixed the 'cannot cast type hll to bytea' error.
-- - Replaced `octet_length(hll_result::bytea)` with
--   `pg_column_size(hll_result)` to correctly get
--   the storage size of the HLL object.
-- ============================================================

\echo '>>> Test 01: Bulk Aggregation vs. COUNT(DISTINCT)...'

-- Create results tables
DROP TABLE IF EXISTS results_bulk_exact;
CREATE TABLE results_bulk_exact (
    test_name TEXT,
    distinct_count BIGINT,
    duration_ms NUMERIC,
    run_number INTEGER
);

DROP TABLE IF EXISTS results_bulk_hll;
CREATE TABLE results_bulk_hll (
    test_name TEXT,
    hll_estimate NUMERIC,
    exact_count BIGINT,
    relative_error NUMERIC,
    duration_ms NUMERIC,
    storage_bytes INTEGER,
    run_number INTEGER
);

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_ms NUMERIC;
    distinct_cnt BIGINT;
    hll_result hll;
    hll_estimate NUMERIC;
    storage_size INTEGER;
    exact_cnt BIGINT; -- Variable to store the exact count
    i INTEGER;
BEGIN
    -- --- 0. Get Exact Count Baseline ---
    -- Fetch the exact count ONCE inside the PL/pgSQL block
    RAISE NOTICE '    Fetching exact count baseline...';
    SELECT COUNT(DISTINCT user_id) INTO exact_cnt FROM facts;
    RAISE NOTICE '    Exact count is %', exact_cnt;

    -- --- 1. EXACT COUNT BASELINE ---
    RAISE NOTICE '    Running EXACT COUNT (5 runs)...';
    FOR i IN 1..5 LOOP
        start_time := clock_timestamp();
        
        SELECT COUNT(DISTINCT user_id) INTO distinct_cnt FROM facts;
        
        end_time := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        INSERT INTO results_bulk_exact VALUES (
            'exact_count', distinct_cnt, duration_ms, i
        );
    END LOOP;

    -- --- 2. HLL default (p=11) ---
    RAISE NOTICE '    Running HLL default (5 runs)...';
    FOR i IN 1..5 LOOP
        start_time := clock_timestamp();
        
        -- Build the HLL in memory using default parameters
        SELECT hll_add_agg(hll_hash_bigint(user_id)) 
        INTO hll_result
        FROM facts;
        
        end_time := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        hll_estimate := hll_cardinality(hll_result);
        -- Use pg_column_size to get the size of the HLL object
        storage_size := pg_column_size(hll_result);
        
        INSERT INTO results_bulk_hll VALUES (
            'hll_default_p11', hll_estimate, exact_cnt,
            ABS(hll_estimate - exact_cnt) / NULLIF(exact_cnt, 0) * 100,
            duration_ms, storage_size, i
        );
    END LOOP;

    -- --- 3. HLL high accuracy (p=14) ---
    RAISE NOTICE '    Running HLL high accuracy (p=14) (5 runs)...';
    FOR i IN 1..5 LOOP
        start_time := clock_timestamp();
        
        -- Build HLL in memory using log2m=14, regwidth=5
        SELECT hll_add_agg(hll_hash_bigint(user_id), 14, 5) 
        INTO hll_result
        FROM facts;
        
        end_time := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        hll_estimate := hll_cardinality(hll_result);
        -- Use pg_column_size to get the size of the HLL object
        storage_size := pg_column_size(hll_result);
        
        INSERT INTO results_bulk_hll VALUES (
            'hll_high_accuracy_p14', hll_estimate, exact_cnt,
            ABS(hll_estimate - exact_cnt) / NULLIF(exact_cnt, 0) * 100,
            duration_ms, storage_size, i
        );
    END LOOP;
END $$;

-- --- 4. Export Results ---
\echo '>>> Test 01: Exporting results...'
\copy results_bulk_exact TO '/tmp/hll_bench_outputs/01_results_bulk_exact.csv' CSV HEADER;
\copy results_bulk_hll TO '/tmp/hll_bench_outputs/01_results_bulk_hll.csv' CSV HEADER;

\echo '>>> Test 01: Complete.'
