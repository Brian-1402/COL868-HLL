-- ============================================================
-- COMPREHENSIVE HLL Benchmark - Multi-Scale Analysis
-- Tests: 10K, 100K, 1M, 10M rows
-- Runtime: 10-30 mins total
-- ============================================================

-- Clean start
DROP TABLE IF EXISTS benchmark_data CASCADE;
DROP TABLE IF EXISTS results_exact CASCADE;
DROP TABLE IF EXISTS results_hll CASCADE;
-- DROP TABLE IF EXISTS scaling_results CASCADE;

\timing on

-- ============================================================
-- RESULTS TABLES
-- ============================================================

CREATE TABLE results_exact (
    test_name TEXT,
    dataset_size BIGINT,
    row_count BIGINT,
    distinct_count BIGINT,
    duration_ms NUMERIC,
    run_number INTEGER
);

CREATE TABLE results_hll (
    test_name TEXT,
    dataset_size BIGINT,
    precision INTEGER,
    row_count BIGINT,
    hll_estimate NUMERIC,
    exact_count BIGINT,
    relative_error NUMERIC,
    duration_ms NUMERIC,
    storage_bytes INTEGER,
    run_number INTEGER
);

-- CREATE TABLE scaling_results (
--     dataset_size BIGINT,
--     distinct_count BIGINT,
--     exact_avg_ms NUMERIC,
--     exact_std_ms NUMERIC,
--     hll_p10_avg_ms NUMERIC,
--     hll_p12_avg_ms NUMERIC,
--     hll_p14_avg_ms NUMERIC,
--     hll_p10_error NUMERIC,
--     hll_p12_error NUMERIC,
--     hll_p14_error NUMERIC,
--     hll_p10_storage INTEGER,
--     hll_p12_storage INTEGER,
--     hll_p14_storage INTEGER
-- );

-- ============================================================
-- BENCHMARK FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION run_benchmark(
    data_size BIGINT,
    cardinality_pct NUMERIC DEFAULT 0.10
) RETURNS void AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_ms NUMERIC;
    hll_result hll;
    hll_estimate NUMERIC;
    exact_cnt BIGINT;
    distinct_vals INTEGER;
    prec INTEGER;
    i INTEGER;
    storage_size INTEGER;
    msg TEXT;
BEGIN
    -- Calculate distinct values (10% cardinality by default)
    distinct_vals := FLOOR(data_size * cardinality_pct)::INTEGER;
    
    msg := '============================================================';
    RAISE NOTICE '%', msg;
    msg := 'BENCHMARKING: ' || data_size || ' rows, ~' || distinct_vals || ' distinct values';
    RAISE NOTICE '%', msg;
    msg := '============================================================';
    RAISE NOTICE '%', msg;
    
    -- Drop and recreate benchmark table
    DROP TABLE IF EXISTS benchmark_data;
    CREATE TABLE benchmark_data (
        id SERIAL PRIMARY KEY,
        user_id INTEGER,
        session_id TEXT,
        timestamp TIMESTAMP DEFAULT NOW()
    );
    
    -- Generate data
    msg := '>>> Generating ' || data_size || ' rows...';
    RAISE NOTICE '%', msg;
    
    EXECUTE format('
        INSERT INTO benchmark_data (user_id, session_id)
        SELECT 
            (random() * %s)::INTEGER as user_id,
            md5(random()::text) as session_id
        FROM generate_series(1, %s)',
        distinct_vals, data_size
    );
    
    CREATE INDEX idx_user_id ON benchmark_data(user_id);
    ANALYZE benchmark_data;
    
    msg := '>>> Data generation complete';
    RAISE NOTICE '%', msg;
    
    -- ========================================
    -- EXACT COUNT BENCHMARK
    -- ========================================
    msg := '>>> Running EXACT COUNT (5 runs)...';
    RAISE NOTICE '%', msg;
    
    -- Warmup
    SELECT COUNT(DISTINCT user_id) FROM benchmark_data INTO exact_cnt;
    
    -- Run 5 times
    FOR i IN 1..5 LOOP
        start_time := clock_timestamp();
        SELECT COUNT(DISTINCT user_id) INTO exact_cnt FROM benchmark_data;
        end_time := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        INSERT INTO results_exact VALUES (
            'exact_count',
            data_size,
            (SELECT COUNT(*) FROM benchmark_data),
            exact_cnt,
            duration_ms,
            i
        );
        
        msg := '  Run ' || i || ': Exact = ' || exact_cnt || ', Time = ' || ROUND(duration_ms, 2) || ' ms';
        RAISE NOTICE '%', msg;
    END LOOP;
    
    -- ========================================
    -- HLL BENCHMARK
    -- ========================================
    msg := '>>> Running HLL tests (precisions 10, 12, 14)...';
    RAISE NOTICE '%', msg;
    
    FOREACH prec IN ARRAY ARRAY[10, 12, 14] LOOP
        msg := '  Testing precision ' || prec;
        RAISE NOTICE '%', msg;
        
        -- Warmup
        SELECT hll_add_agg(hll_hash_integer(user_id), prec) 
        FROM benchmark_data INTO hll_result;
        
        FOR i IN 1..5 LOOP
            start_time := clock_timestamp();
            
            SELECT hll_add_agg(hll_hash_integer(user_id), prec) 
            INTO hll_result
            FROM benchmark_data;
            
            end_time := clock_timestamp();
            duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
            
            hll_estimate := hll_cardinality(hll_result);
            storage_size := pg_column_size(hll_result);
            
            INSERT INTO results_hll VALUES (
                'hll_p' || prec,
                data_size,
                prec,
                (SELECT COUNT(*) FROM benchmark_data),
                hll_estimate,
                exact_cnt,
                ABS(hll_estimate - exact_cnt) / exact_cnt * 100,
                duration_ms,
                storage_size,
                i
            );
            
            IF i = 1 THEN
                msg := '    Estimate: ' || ROUND(hll_estimate) || 
                       ', Error: ' || ROUND(ABS(hll_estimate - exact_cnt) / exact_cnt * 100, 3) || 
                       '%, Storage: ' || storage_size || ' bytes';
                RAISE NOTICE '%', msg;
            END IF;
        END LOOP;
    END LOOP;
    
    msg := '>>> Benchmark complete for ' || data_size || ' rows';
    RAISE NOTICE '%', msg;
    RAISE NOTICE '';
    
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- RUN BENCHMARKS AT MULTIPLE SCALES
-- ============================================================

\echo ''
\echo '========================================'
\echo 'MULTI-SCALE BENCHMARK SUITE'
\echo 'This will take 2-3 hours to complete'
\echo '========================================'
\echo ''

-- Scale 1: 10K rows
SELECT run_benchmark(10000);

-- Scale 2: 100K rows
SELECT run_benchmark(100000);

-- Scale 3: 1M rows
SELECT run_benchmark(1000000);

-- Scale 4: 10M rows
SELECT run_benchmark(10000000);

-- ============================================================
-- COMPUTE SCALING SUMMARY
-- ============================================================

-- \echo ''
-- \echo '>>> Computing scaling summary...'

-- INSERT INTO scaling_results
-- SELECT 
--     dataset_size,
--     AVG(exact.distinct_count)::BIGINT as distinct_count,
--     AVG(exact.duration_ms) as exact_avg_ms,
--     STDDEV(exact.duration_ms) as exact_std_ms,
--     AVG(CASE WHEN hll.precision = 10 THEN hll.duration_ms END) as hll_p10_avg_ms,
--     AVG(CASE WHEN hll.precision = 12 THEN hll.duration_ms END) as hll_p12_avg_ms,
--     AVG(CASE WHEN hll.precision = 14 THEN hll.duration_ms END) as hll_p14_avg_ms,
--     AVG(CASE WHEN hll.precision = 10 THEN hll.relative_error END) as hll_p10_error,
--     AVG(CASE WHEN hll.precision = 12 THEN hll.relative_error END) as hll_p12_error,
--     AVG(CASE WHEN hll.precision = 14 THEN hll.relative_error END) as hll_p14_error,
--     AVG(CASE WHEN hll.precision = 10 THEN hll.storage_bytes END)::INTEGER as hll_p10_storage,
--     AVG(CASE WHEN hll.precision = 12 THEN hll.storage_bytes END)::INTEGER as hll_p12_storage,
--     AVG(CASE WHEN hll.precision = 14 THEN hll.storage_bytes END)::INTEGER as hll_p14_storage
-- FROM results_exact exact
-- LEFT JOIN results_hll hll ON exact.dataset_size = hll.dataset_size
-- GROUP BY dataset_size
-- ORDER BY dataset_size;

-- ============================================================
-- FINAL RESULTS
-- ============================================================

-- \echo ''
-- \echo '========================================'
-- \echo 'SCALING ANALYSIS RESULTS'
-- \echo '========================================'

-- \echo ''
-- \echo '>>> Performance Scaling'
-- SELECT 
--     dataset_size as rows,
--     distinct_count as distinct,
--     ROUND(exact_avg_ms, 2) as exact_ms,
--     ROUND(hll_p12_avg_ms, 2) as hll_ms,
--     ROUND(exact_avg_ms / hll_p12_avg_ms, 2) as speedup
-- FROM scaling_results
-- ORDER BY dataset_size;

-- \echo ''
-- \echo '>>> Accuracy by Scale (Precision 12)'
-- SELECT 
--     dataset_size as rows,
--     ROUND(hll_p10_error, 3) as p10_err_pct,
--     ROUND(hll_p12_error, 3) as p12_err_pct,
--     ROUND(hll_p14_error, 3) as p14_err_pct
-- FROM scaling_results
-- ORDER BY dataset_size;

-- \echo ''
-- \echo '>>> Storage Requirements'
-- SELECT 
--     dataset_size as rows,
--     hll_p10_storage as p10_bytes,
--     hll_p12_storage as p12_bytes,
--     hll_p14_storage as p14_bytes
-- FROM scaling_results
-- ORDER BY dataset_size;

-- ============================================================
-- EXPORT RESULTS
-- ============================================================

\echo ''
\echo '>>> Exporting to CSV...'
\copy results_exact TO '/code/results/hll_add_agg_exact.csv' CSV HEADER
\copy results_hll TO '/code/results/hll_add_agg_hll.csv' CSV HEADER
-- \copy scaling_results TO '/code/results/hll_add_agg_scaling_results.csv' CSV HEADER

\echo ''
\echo '========================================'
\echo 'BENCHMARK SUITE COMPLETE!'
\echo '========================================'
\echo 'Results saved to /code/results/'
\echo '  /code/results/hll_add_agg_exact.csv'
\echo '  /code/results/hll_add_agg_hll.csv'
-- \echo '  /code/results/hll_add_agg_scaling_results.csv'
\echo ''
\echo 'Then run: python plot_results.py'
\echo '========================================'
