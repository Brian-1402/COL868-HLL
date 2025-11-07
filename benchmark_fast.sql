-- ============================================================
-- FAST HLL Benchmark (30 mins runtime)
-- FULLY CORRECTED VERSION - Final
-- ============================================================

-- Clean start
DROP TABLE IF EXISTS benchmark_data CASCADE;
DROP TABLE IF EXISTS results_exact CASCADE;
DROP TABLE IF EXISTS results_hll CASCADE;

\timing on

-- ============================================================
-- 1. GENERATE DATA (100K rows, ~10K distinct)
-- ============================================================

CREATE TABLE benchmark_data (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    session_id TEXT,
    timestamp TIMESTAMP DEFAULT NOW()
);

\echo '>>> Generating 100K rows...'
INSERT INTO benchmark_data (user_id, session_id)
SELECT 
    (random() * 10000)::INTEGER as user_id,
    md5(random()::text) as session_id
FROM generate_series(1, 100000);

CREATE INDEX idx_user_id ON benchmark_data(user_id);
ANALYZE benchmark_data;

\echo '>>> Data generated successfully'

-- ============================================================
-- 2. EXACT COUNT BASELINE
-- ============================================================

CREATE TABLE results_exact (
    test_name TEXT,
    row_count BIGINT,
    distinct_count BIGINT,
    duration_ms NUMERIC,
    run_number INTEGER
);

\echo '>>> Running EXACT COUNT (5 runs)...'

-- Warmup
SELECT COUNT(DISTINCT user_id) FROM benchmark_data;

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_ms NUMERIC;
    distinct_cnt BIGINT;
    i INTEGER;
    msg TEXT;
BEGIN
    FOR i IN 1..5 LOOP
        start_time := clock_timestamp();
        SELECT COUNT(DISTINCT user_id) INTO distinct_cnt FROM benchmark_data;
        end_time := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        INSERT INTO results_exact VALUES (
            'exact_count',
            (SELECT COUNT(*) FROM benchmark_data),
            distinct_cnt,
            duration_ms,
            i
        );
        
        msg := 'Run ' || i || ': Exact = ' || distinct_cnt || ', Time = ' || ROUND(duration_ms, 2) || ' ms';
        RAISE NOTICE '%', msg;
    END LOOP;
END $$;

-- ============================================================
-- 3. HLL APPROXIMATE COUNT
-- ============================================================

CREATE TABLE results_hll (
    test_name TEXT,
    precision INTEGER,
    row_count BIGINT,
    hll_estimate NUMERIC,
    exact_count BIGINT,
    relative_error NUMERIC,
    duration_ms NUMERIC,
    storage_bytes INTEGER,
    run_number INTEGER
);

\echo '>>> Running HLL tests (precisions 10, 12, 14)...'

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_ms NUMERIC;
    hll_result hll;
    hll_estimate NUMERIC;
    exact_cnt BIGINT;
    prec INTEGER;
    i INTEGER;
    storage_size INTEGER;
    msg TEXT;
BEGIN
    SELECT COUNT(DISTINCT user_id) INTO exact_cnt FROM benchmark_data;
    
    FOREACH prec IN ARRAY ARRAY[10, 12, 14] LOOP
        msg := '>>> Testing precision ' || prec;
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
            -- FIXED: Use pg_column_size instead of bytea cast
            storage_size := pg_column_size(hll_result);
            
            INSERT INTO results_hll VALUES (
                'hll_p' || prec,
                prec,
                (SELECT COUNT(*) FROM benchmark_data),
                hll_estimate,
                exact_cnt,
                ABS(hll_estimate - exact_cnt) / exact_cnt * 100,
                duration_ms,
                storage_size,
                i
            );
            
            msg := 'Run ' || i || ': Est = ' || ROUND(hll_estimate) || 
                   ', Error = ' || ROUND(ABS(hll_estimate - exact_cnt) / exact_cnt * 100, 3) || 
                   '%, Time = ' || ROUND(duration_ms, 2) || ' ms';
            RAISE NOTICE '%', msg;
        END LOOP;
    END LOOP;
END $$;

-- ============================================================
-- 4. RESULTS SUMMARY
-- ============================================================

\echo ''
\echo '========================================'
\echo 'RESULTS SUMMARY'
\echo '========================================'

\echo ''
\echo '>>> EXACT COUNT'
SELECT 
    AVG(distinct_count)::INTEGER as avg_count,
    ROUND(AVG(duration_ms), 2) as avg_ms,
    ROUND(STDDEV(duration_ms), 2) as stddev_ms
FROM results_exact;

\echo ''
\echo '>>> HLL RESULTS BY PRECISION'
SELECT 
    precision as p,
    ROUND(AVG(hll_estimate)) as avg_estimate,
    ROUND(AVG(relative_error), 3) as avg_error_pct,
    ROUND(AVG(duration_ms), 2) as avg_ms,
    ROUND(STDDEV(duration_ms), 2) as stddev_ms,
    ROUND(AVG(storage_bytes)) as storage_bytes
FROM results_hll
GROUP BY precision
ORDER BY precision;

\echo ''
\echo '>>> SPEEDUP FACTORS'
SELECT 
    precision as p,
    ROUND(
        (SELECT AVG(duration_ms) FROM results_exact) / AVG(duration_ms),
        2
    ) as speedup
FROM results_hll
GROUP BY precision
ORDER BY precision;

-- ============================================================
-- 5. EXPORT RESULTS
-- ============================================================

\echo ''
\echo '>>> Exporting to CSV...'
\copy results_exact TO '/tmp/results_exact.csv' CSV HEADER
\copy results_hll TO '/tmp/results_hll.csv' CSV HEADER

\echo ''
\echo '========================================'
\echo 'BENCHMARK COMPLETE!'
\echo '========================================'
\echo 'Export CSV files with:'
\echo '  docker cp pgdb:/tmp/results_exact.csv ./'
\echo '  docker cp pgdb:/tmp/results_hll.csv ./'
\echo ''
\echo 'Then run: python plot_results.py'
\echo '========================================'
