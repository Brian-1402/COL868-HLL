-- ============================================================
-- 03_test_hashing.sql
--
-- Tests the performance overhead of different hashing functions.
-- 1. Compares hll_hash_bigint() (type-specific)
-- 2. vs. hll_hash_any() (dynamic dispatch)
--
-- Runs 5 times for stability and exports results.
-- ============================================================

\echo '>>> Test 03: Hashing Function Overhead...'

-- Create results table
DROP TABLE IF EXISTS results_hashing;
CREATE TABLE results_hashing (
    test_name TEXT,
    duration_ms NUMERIC,
    run_number INTEGER
);

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_ms NUMERIC;
    hll_result hll;
    i INTEGER;
BEGIN
    -- --- 1. hll_hash_bigint (type-specific) ---
    RAISE NOTICE '    Running hll_hash_bigint (5 runs)...';
    FOR i IN 1..5 LOOP
        start_time := clock_timestamp();
        
        SELECT hll_add_agg(hll_hash_bigint(user_id)) 
        INTO hll_result
        FROM facts;
        
        end_time := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        INSERT INTO results_hashing VALUES (
            'hash_bigint', duration_ms, i
        );
    END LOOP;

    -- --- 2. hll_hash_any (dynamic) ---
    RAISE NOTICE '    Running hll_hash_any (5 runs)...';
    FOR i IN 1..5 LOOP
        start_time := clock_timestamp();
        
        SELECT hll_add_agg(hll_hash_any(user_id)) 
        INTO hll_result
        FROM facts;
        
        end_time := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        INSERT INTO results_hashing VALUES (
            'hash_any', duration_ms, i
        );
    END LOOP;
END $$;

-- --- 3. Export Results ---
\echo '>>> Test 03: Exporting results...'
\copy results_hashing TO '/tmp/hll_bench_outputs/03_results_hashing.csv' CSV HEADER;

\echo '>>> Test 03: Complete.'
