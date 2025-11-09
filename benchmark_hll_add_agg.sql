-- ============================================================
-- :: BENCHMARK PARAMETERS
-- ============================================================

-- SQL array literal for HLL precisions to test
\set PRECISIONS_ARRAY '{10, 12, 14}'

-- Cardinality percentage (e.g., 0.10 = 10% distinct values)
\set CARDINALITY_PCT '0.10'

-- ============================================================
-- COMPREHENSIVE HLL Benchmark - Multi-Scale Analysis
-- ============================================================

-- Clean start
DROP TABLE IF EXISTS benchmark_data CASCADE;
DROP TABLE IF EXISTS results_exact CASCADE;
DROP TABLE IF EXISTS results_hll CASCADE;

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

-- ============================================================
-- BENCHMARK FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION run_benchmark(
    data_size BIGINT,
    cardinality_pct NUMERIC,
    precisions INTEGER[]
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
    -- Calculate distinct values
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

    EXECUTE 'SELECT setseed(0.5)';
    
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
    msg := '>>> Running HLL tests (precisions ' || precisions::TEXT || ')...';
    RAISE NOTICE '%', msg;
    
    -- Iterate over the precisions array passed as an argument
    FOREACH prec IN ARRAY precisions LOOP
        msg := '  Testing precision ' || prec;
        RAISE NOTICE '%', msg;
        
        -- Warmup
        SELECT hll_add_agg(hll_hash_integer(user_id), prec) 
        FROM benchmark_data INTO hll_result;
        
        FOR i IN 1..5 LOOP
            start_time := clock_timestamp();
            
            -- Adds the hashed user_ids into an HLL and estimates cardinality
            -- creates new HLL for each run
            SELECT hll_add_agg(hll_hash_integer(user_id), prec) 
            INTO hll_result
            FROM benchmark_data;

            hll_estimate := hll_cardinality(hll_result);
            
            end_time := clock_timestamp();
            duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
            
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
\echo 'Using precisions: ' :PRECISIONS_ARRAY
\echo '========================================'
\echo ''

\echo '--- Starting benchmark for 10000 rows ---'
SELECT run_benchmark(10000, :CARDINALITY_PCT, :'PRECISIONS_ARRAY');
\echo '--- Completed benchmark for 10000 rows ---'
\echo ''

\echo '--- Starting benchmark for 100000 rows ---'
SELECT run_benchmark(100000, :CARDINALITY_PCT, :'PRECISIONS_ARRAY');
\echo '--- Completed benchmark for 100000 rows ---'
\echo ''

\echo '--- Starting benchmark for 1000000 rows ---'
SELECT run_benchmark(1000000, :CARDINALITY_PCT, :'PRECISIONS_ARRAY');
\echo '--- Completed benchmark for 1000000 rows ---'
\echo ''

\echo '--- Starting benchmark for 10000000 rows ---'
SELECT run_benchmark(10000000, :CARDINALITY_PCT, :'PRECISIONS_ARRAY');
\echo '--- Completed benchmark for 10000000 rows ---'
\echo ''


-- ============================================================
-- EXPORT RESULTS
-- ============================================================

\echo ''
\echo '>>> Exporting to CSV...'
\! mkdir -p '/code/tables'
\copy results_exact TO '/code/tables/hll_add_agg_exact.csv' CSV HEADER
\copy results_hll TO '/code/tables/hll_add_agg_hll.csv' CSV HEADER

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
