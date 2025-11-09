-- ============================================================================
-- HLL UNION BENCHMARK - Testing hll_union_agg Performance
-- ============================================================================
-- This benchmark tests the performance and accuracy of hll_union_agg by:
-- 1. Creating daily user activity sketches
-- 2. Testing union performance across different time windows
-- 3. Comparing against exact COUNT(DISTINCT) re-aggregation
-- 4. Testing different precisions and data volumes
-- ============================================================================

\timing on

-- Clean up existing tables
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS daily_sketches CASCADE;
DROP TABLE IF EXISTS results_union CASCADE;
DROP TABLE IF EXISTS results_exact_reagg CASCADE;
DROP TABLE IF EXISTS results_comparison CASCADE;

\echo ''
\echo '========================================'
\echo 'SETUP: Generating Test Data'
\echo '========================================'

-- Create main events table
CREATE TABLE events (
    event_id SERIAL,
    timestamp TIMESTAMP,
    user_id INTEGER,
    date DATE
);

\echo '>>> Generating 1M events over 90 days...'
\echo '    (10K-15K events per day, ~50K unique users total)'

-- Generate realistic event data
-- Users have varying activity patterns (some daily, some weekly)
INSERT INTO events (timestamp, user_id, date)
SELECT 
    timestamp,
    -- Weighted user distribution: some users very active, most less active
    CASE 
        WHEN random() < 0.1 THEN floor(random() * 1000)::int         -- 10% users are super active (0-1K)
        WHEN random() < 0.4 THEN floor(random() * 10000)::int        -- 30% users are active (0-10K)
        ELSE floor(random() * 50000)::int                             -- 60% users are casual (0-50K)
    END as user_id,
    timestamp::date as date
FROM generate_series(
    CURRENT_DATE - INTERVAL '90 days',
    CURRENT_DATE - INTERVAL '1 day',
    INTERVAL '6 seconds'  -- ~10K events per day
) as timestamp;

CREATE INDEX idx_events_date ON events(date);
CREATE INDEX idx_events_user_date ON events(user_id, date);
ANALYZE events;

\echo '>>> Data generation complete!'
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT user_id) as total_unique_users,
    COUNT(DISTINCT date) as total_days,
    MIN(date) as start_date,
    MAX(date) as end_date
FROM events;

-- ============================================================================
-- PHASE 1: Pre-compute Daily Sketches at Different Precisions
-- ============================================================================

\echo ''
\echo '========================================'
\echo 'PHASE 1: Pre-computing Daily Sketches'
\echo '========================================'

CREATE TABLE daily_sketches (
    date DATE,
    precision INTEGER,
    user_sketch HLL,
    exact_count INTEGER,
    sketch_size_bytes INTEGER
);

\echo '>>> Creating daily sketches for precision 10, 12, 14...'

-- Generate sketches for each precision level
INSERT INTO daily_sketches (date, precision, user_sketch, exact_count, sketch_size_bytes)
SELECT 
    date,
    p as precision,
    hll_add_agg(hll_hash_integer(user_id), p) as user_sketch,
    COUNT(DISTINCT user_id) as exact_count,
    -- Approximate sketch size (will calculate actual later)
    power(2, p)::int as sketch_size_bytes
FROM events
CROSS JOIN (VALUES (10), (12), (14)) as precisions(p)
GROUP BY date, p;

-- Update actual sketch sizes using pg_column_size which works with HLL type
UPDATE daily_sketches
SET sketch_size_bytes = pg_column_size(user_sketch);

CREATE INDEX idx_daily_sketches_date_prec ON daily_sketches(date, precision);
ANALYZE daily_sketches;

\echo '>>> Daily sketches created!'
SELECT 
    precision,
    COUNT(*) as num_days,
    AVG(exact_count)::int as avg_daily_users,
    AVG(sketch_size_bytes)::int as avg_sketch_bytes,
    SUM(sketch_size_bytes)::bigint as total_storage_bytes
FROM daily_sketches
GROUP BY precision
ORDER BY precision;

-- ============================================================================
-- PHASE 2: Benchmark HLL Union Aggregation
-- ============================================================================

\echo ''
\echo '========================================'
\echo 'PHASE 2: Benchmarking hll_union_agg'
\echo '========================================'

CREATE TABLE results_union (
    test_name VARCHAR(100),
    precision INTEGER,
    num_days INTEGER,
    run INTEGER,
    estimated_count BIGINT,
    query_time_ms NUMERIC,
    total_sketch_size_bytes BIGINT
);

\echo '>>> Testing union performance across different time windows...'
\echo '    Time windows: 7, 14, 30, 60, 90 days'
\echo '    Precisions: 10, 12, 14'
\echo '    Runs per test: 5'

-- Test function for union aggregation
DO $$
DECLARE
    v_start TIMESTAMP;
    v_end TIMESTAMP;
    v_precision INTEGER;
    v_days INTEGER;
    v_run INTEGER;
    v_result BIGINT;
    v_time_ms NUMERIC;
    v_size BIGINT;
BEGIN
    -- Test different time windows and precisions
    FOREACH v_days IN ARRAY ARRAY[7, 14, 30, 60, 90] LOOP
        FOREACH v_precision IN ARRAY ARRAY[10, 12, 14] LOOP
            RAISE NOTICE '>>> Testing: % days, precision %', v_days, v_precision;
            
            -- Run 5 times for each configuration
            FOR v_run IN 1..5 LOOP
                -- Time the union operation
                v_start := clock_timestamp();
                
                SELECT 
                    hll_cardinality(hll_union_agg(user_sketch))::bigint,
                    SUM(sketch_size_bytes)
                INTO v_result, v_size
                FROM daily_sketches
                WHERE precision = v_precision
                  AND date >= CURRENT_DATE - (v_days || ' days')::interval;
                
                v_end := clock_timestamp();
                v_time_ms := EXTRACT(EPOCH FROM (v_end - v_start)) * 1000;
                
                INSERT INTO results_union VALUES (
                    'union_' || v_days || 'd',
                    v_precision,
                    v_days,
                    v_run,
                    v_result,
                    v_time_ms,
                    v_size
                );
                
                RAISE NOTICE '  Run %: Estimated = %, Time = % ms', 
                    v_run, v_result, ROUND(v_time_ms, 2);
            END LOOP;
        END LOOP;
    END LOOP;
END $$;

-- ============================================================================
-- PHASE 3: Benchmark Exact Re-aggregation (Baseline)
-- ============================================================================

\echo ''
\echo '========================================'
\echo 'PHASE 3: Benchmarking Exact Re-aggregation'
\echo '========================================'

CREATE TABLE results_exact_reagg (
    test_name VARCHAR(100),
    num_days INTEGER,
    run INTEGER,
    exact_count BIGINT,
    query_time_ms NUMERIC
);

\echo '>>> Re-scanning raw data for exact counts...'
\echo '    (This is the traditional approach - should be much slower)'

DO $$
DECLARE
    v_start TIMESTAMP;
    v_end TIMESTAMP;
    v_days INTEGER;
    v_run INTEGER;
    v_result BIGINT;
    v_time_ms NUMERIC;
BEGIN
    FOREACH v_days IN ARRAY ARRAY[7, 14, 30, 60, 90] LOOP
        RAISE NOTICE '>>> Testing exact COUNT(DISTINCT) for % days', v_days;
        
        FOR v_run IN 1..5 LOOP
            v_start := clock_timestamp();
            
            SELECT COUNT(DISTINCT user_id)::bigint
            INTO v_result
            FROM events
            WHERE date >= CURRENT_DATE - (v_days || ' days')::interval;
            
            v_end := clock_timestamp();
            v_time_ms := EXTRACT(EPOCH FROM (v_end - v_start)) * 1000;
            
            INSERT INTO results_exact_reagg VALUES (
                'exact_' || v_days || 'd',
                v_days,
                v_run,
                v_result,
                v_time_ms
            );
            
            RAISE NOTICE '  Run %: Count = %, Time = % ms', 
                v_run, v_result, ROUND(v_time_ms, 2);
        END LOOP;
    END LOOP;
END $$;

-- ============================================================================
-- PHASE 4: Results Analysis
-- ============================================================================

\echo ''
\echo '========================================'
\echo 'RESULTS ANALYSIS'
\echo '========================================'

-- Create comparison table
CREATE TABLE results_comparison AS
WITH union_stats AS (
    SELECT 
        precision,
        num_days,
        AVG(estimated_count)::bigint as avg_estimate,
        AVG(query_time_ms) as avg_time_ms,
        STDDEV(query_time_ms) as stddev_time_ms,
        MAX(total_sketch_size_bytes) as total_sketch_bytes
    FROM results_union
    GROUP BY precision, num_days
),
exact_stats AS (
    SELECT 
        num_days,
        AVG(exact_count)::bigint as exact_count,
        AVG(query_time_ms) as avg_time_ms,
        STDDEV(query_time_ms) as stddev_time_ms
    FROM results_exact_reagg
    GROUP BY num_days
)
SELECT 
    u.num_days,
    u.precision,
    e.exact_count,
    u.avg_estimate,
    ABS(u.avg_estimate - e.exact_count) as error_absolute,
    ROUND(ABS(u.avg_estimate - e.exact_count)::numeric / NULLIF(e.exact_count, 0) * 100, 3) as error_pct,
    ROUND(u.avg_time_ms, 2) as union_time_ms,
    ROUND(u.stddev_time_ms, 2) as union_stddev_ms,
    ROUND(e.avg_time_ms, 2) as exact_time_ms,
    ROUND(e.stddev_time_ms, 2) as exact_stddev_ms,
    ROUND(e.avg_time_ms / NULLIF(u.avg_time_ms, 0), 2) as speedup_factor,
    u.total_sketch_bytes,
    ROUND(u.total_sketch_bytes::numeric / 1024, 2) as sketch_size_kb
FROM union_stats u
JOIN exact_stats e ON u.num_days = e.num_days
ORDER BY u.num_days, u.precision;

\echo ''
\echo '>>> PERFORMANCE COMPARISON BY TIME WINDOW'
SELECT 
    num_days || ' days' as time_window,
    precision,
    exact_count,
    avg_estimate,
    error_pct || '%' as error,
    union_time_ms || ' ms' as hll_union_time,
    exact_time_ms || ' ms' as exact_reagg_time,
    speedup_factor || 'x' as speedup
FROM results_comparison
ORDER BY num_days, precision;

\echo ''
\echo '>>> ACCURACY ANALYSIS BY PRECISION'
SELECT 
    precision,
    ROUND(AVG(error_pct), 3) || '%' as avg_error,
    ROUND(MAX(error_pct), 3) || '%' as max_error,
    ROUND(MIN(error_pct), 3) || '%' as min_error,
    ROUND(AVG(union_time_ms), 2) || ' ms' as avg_query_time,
    ROUND(AVG(sketch_size_kb), 2) || ' KB' as avg_total_sketch_size
FROM results_comparison
GROUP BY precision
ORDER BY precision;

\echo ''
\echo '>>> SPEEDUP ANALYSIS BY TIME WINDOW'
SELECT 
    num_days || ' days' as time_window,
    ROUND(AVG(speedup_factor), 2) || 'x' as avg_speedup,
    ROUND(MIN(speedup_factor), 2) || 'x' as min_speedup,
    ROUND(MAX(speedup_factor), 2) || 'x' as max_speedup,
    COUNT(*) as num_sketches_unioned
FROM results_comparison
GROUP BY num_days
ORDER BY num_days;

\echo ''
\echo '>>> STORAGE EFFICIENCY'
WITH storage_calc AS (
    SELECT 
        SUM(pg_column_size(user_id))::bigint as raw_user_id_bytes,
        COUNT(*) as total_events
    FROM events
),
sketch_storage AS (
    SELECT 
        precision,
        SUM(sketch_size_bytes)::bigint as total_sketch_bytes
    FROM daily_sketches
    GROUP BY precision
)
SELECT 
    s.precision,
    ROUND(sc.raw_user_id_bytes::numeric / (1024*1024), 2) || ' MB' as raw_data_size,
    ROUND(s.total_sketch_bytes::numeric / (1024*1024), 2) || ' MB' as sketch_storage,
    ROUND(sc.raw_user_id_bytes::numeric / NULLIF(s.total_sketch_bytes, 0), 1) || 'x' as compression_ratio
FROM sketch_storage s
CROSS JOIN storage_calc sc
ORDER BY s.precision;

-- ============================================================================
-- PHASE 5: Advanced Union Tests
-- ============================================================================

\echo ''
\echo '========================================'
\echo 'PHASE 5: Advanced Union Scenarios'
\echo '========================================'

\echo ''
\echo '>>> Test 1: Union Performance vs Number of Sketches'
\echo '    Hypothesis: Time grows linearly with number of sketches'

DO $$
DECLARE
    v_start TIMESTAMP;
    v_end TIMESTAMP;
    v_num_sketches INTEGER;
    v_result BIGINT;
    v_time_ms NUMERIC;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'Precision 12, varying number of daily sketches:';
    
    FOREACH v_num_sketches IN ARRAY ARRAY[1, 5, 10, 20, 30, 60, 90] LOOP
        v_start := clock_timestamp();
        
        SELECT hll_cardinality(hll_union_agg(user_sketch))::bigint
        INTO v_result
        FROM (
            SELECT user_sketch 
            FROM daily_sketches 
            WHERE precision = 12
            ORDER BY date DESC
            LIMIT v_num_sketches
        ) sub;
        
        v_end := clock_timestamp();
        v_time_ms := EXTRACT(EPOCH FROM (v_end - v_start)) * 1000;
        
        RAISE NOTICE '  % sketches: % ms (estimated users: %)', 
            LPAD(v_num_sketches::text, 3), 
            ROUND(v_time_ms, 2), 
            v_result;
    END LOOP;
END $$;

\echo ''
\echo '>>> Test 2: Nested Union (Union of Unions)'
\echo '    Create weekly sketches from daily, then monthly from weekly'

DO $$
DECLARE
    v_start TIMESTAMP;
    v_end TIMESTAMP;
    v_time_ms NUMERIC;
    v_result BIGINT;
BEGIN
    -- First level: Create weekly sketches
    v_start := clock_timestamp();
    
    CREATE TEMP TABLE weekly_sketches AS
    SELECT 
        DATE_TRUNC('week', date) as week,
        precision,
        hll_union_agg(user_sketch) as weekly_sketch
    FROM daily_sketches
    WHERE precision = 12
    GROUP BY DATE_TRUNC('week', date), precision;
    
    v_end := clock_timestamp();
    v_time_ms := EXTRACT(EPOCH FROM (v_end - v_start)) * 1000;
    RAISE NOTICE 'Step 1: Daily → Weekly union: % ms', ROUND(v_time_ms, 2);
    
    -- Second level: Create monthly sketch from weekly
    v_start := clock_timestamp();
    
    SELECT hll_cardinality(hll_union_agg(weekly_sketch))::bigint
    INTO v_result
    FROM weekly_sketches;
    
    v_end := clock_timestamp();
    v_time_ms := EXTRACT(EPOCH FROM (v_end - v_start)) * 1000;
    RAISE NOTICE 'Step 2: Weekly → Monthly union: % ms', ROUND(v_time_ms, 2);
    RAISE NOTICE 'Final 90-day estimate: % unique users', v_result;
    
    DROP TABLE weekly_sketches;
END $$;

\echo ''
\echo '>>> Test 3: Partial Union (Growing Time Windows)'
\echo '    Simulating a rolling window aggregation'

DO $$
DECLARE
    v_start TIMESTAMP;
    v_end TIMESTAMP;
    v_day INTEGER;
    v_time_ms NUMERIC;
    v_cumulative_time NUMERIC := 0;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'Computing cumulative unique users (rolling window):';
    
    FOR v_day IN 1..10 LOOP
        v_start := clock_timestamp();
        
        PERFORM hll_cardinality(
            hll_union_agg(user_sketch)
        )
        FROM daily_sketches
        WHERE precision = 12
          AND date >= CURRENT_DATE - (v_day || ' days')::interval;
        
        v_end := clock_timestamp();
        v_time_ms := EXTRACT(EPOCH FROM (v_end - v_start)) * 1000;
        v_cumulative_time := v_cumulative_time + v_time_ms;
        
        IF v_day <= 3 OR v_day = 10 THEN
            RAISE NOTICE '  Last % days: % ms (cumulative: % ms)', 
                LPAD(v_day::text, 2), 
                ROUND(v_time_ms, 2),
                ROUND(v_cumulative_time, 2);
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- PHASE 6: Export Results
-- ============================================================================

\echo ''
\echo '========================================'
\echo 'EXPORTING RESULTS'
\echo '========================================'

-- Create directories first
\! mkdir -p /code/tables/hll_union_benchmark
\! mkdir -p /code/results/hll_union_benchmark

\copy results_comparison TO '/code/tables/hll_union_benchmark/comparison.csv' CSV HEADER
\copy results_union TO '/code/tables/hll_union_benchmark/union_detailed.csv' CSV HEADER
\copy results_exact_reagg TO '/code/tables/hll_union_benchmark/exact_detailed.csv' CSV HEADER

-- Export summary statistics
\copy (SELECT * FROM results_comparison ORDER BY num_days, precision) TO '/code/results/hll_union_benchmark/summary.csv' CSV HEADER

\echo ''
\echo '========================================'
\echo 'BENCHMARK COMPLETE!'
\echo '========================================'
\echo ''
\echo 'Key Findings:'
\echo '-------------'

SELECT 
    '• Average speedup: ' || ROUND(AVG(speedup_factor), 1) || 'x faster than exact count'
FROM results_comparison;

SELECT 
    '• Average error: ' || ROUND(AVG(error_pct), 2) || '% across all tests'
FROM results_comparison;

SELECT 
    '• Storage savings: ~' || ROUND(AVG(sc.raw_user_id_bytes::numeric / NULLIF(s.total_sketch_bytes, 0)), 0) || 'x compression'
FROM (
    SELECT SUM(pg_column_size(user_id))::bigint as raw_user_id_bytes
    FROM events
) sc
CROSS JOIN (
    SELECT AVG(total_sketch_bytes) as total_sketch_bytes
    FROM (
        SELECT precision, SUM(sketch_size_bytes)::bigint as total_sketch_bytes
        FROM daily_sketches
        GROUP BY precision
    ) x
) s;

\echo ''
\echo 'Results exported to:'
\echo '  /code/tables/hll_union_benchmark/comparison.csv'
\echo '  /code/tables/hll_union_benchmark/union_detailed.csv'
\echo '  /code/tables/hll_union_benchmark/exact_detailed.csv'
\echo '  /code/results/hll_union_benchmark/summary.csv'
\echo ''
\echo 'Next steps:'
\echo '  1. Visualize results: docker compose -f docker-compose.graphs.yml run --rm plotter python hll_union_plot.py'
\echo '  2. Review detailed CSVs for deeper analysis'
\echo '========================================'