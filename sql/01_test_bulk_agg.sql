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

-- Get exact count once for error comparison
\set exact_count (SELECT COUNT(DISTINCT user_id) FROM facts)

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_ms NUMERIC;
    distinct_cnt BIGINT;
    hll_result hll;
    hll_estimate NUMERIC;
    storage_size INTEGER;
    exact_cnt BIGINT := :exact_count;
    i INTEGER;
BEGIN
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
        
        -- Build the HLL in memory
        SELECT hll_add_agg(hll_hash_bigint(user_id)) 
        INTO hll_result
        FROM facts;
        
        end_time := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        hll_estimate := hll_cardinality(hll_result);
        storage_size := octet_length(hll_result::bytea);
        
        INSERT INTO results_bulk_hll VALUES (
            'hll_default_p11', hll_estimate, exact_cnt,
            ABS(hll_estimate - exact_cnt) / exact_cnt * 100,
            duration_ms, storage_size, i
        );
    END LOOP;

    -- --- 3. HLL high accuracy (p=14) ---
    RAISE NOTICE '    Running HLL high accuracy (p=14) (5 runs)...';
    FOR i IN 1..5 LOOP
        start_time := clock_timestamp();
        
        -- Note: To test aggregation speed into a specific HLL type,
        -- we must cast hll_empty() to that type.
        SELECT hll_add_agg(hll_hash_bigint(user_id), hll_empty()::hll(log2m=14)) 
        INTO hll_result
        FROM facts;
        
        end_time := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        hll_estimate := hll_cardinality(hll_result);
        storage_size := octet_length(hll_result::bytea);
        
        INSERT INTO results_bulk_hll VALUES (
            'hll_high_accuracy_p14', hll_estimate, exact_cnt,
            ABS(hll_estimate - exact_cnt) / exact_cnt * 100,
            duration_ms, storage_size, i
        );
    END LOOP;
END $$;

-- --- 4. Export Results ---
\echo '>>> Test 01: Exporting results...'
\copy results_bulk_exact TO '/tmp/hll_bench_outputs/01_results_bulk_exact.csv' CSV HEADER;
\copy results_bulk_hll TO '/tmp/hll_bench_outputs/01_results_bulk_hll.csv' CSV HEADER;

\echo '>>> Test 01: Complete.'
