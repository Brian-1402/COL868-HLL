# HyperLogLog (HLL) Benchmarking in PostgreSQL

This experiment aims to evaluate HyperLogLog (HLL) probabilistic data structure for approximate distinct counting in PostgreSQL. We benchmark HLL against exact COUNT(DISTINCT) operations across multiple scales (10K to 10M rows) and precision parameters (10, 12, 14), analyzing accuracy, performance, and memory usage.

## Setup Commands

Prerequisites - Docker Desktop (with Docker Compose)

- **Clone and Run Compose:**
```bash
# 1. Clone repository
git clone https://github.com/Brian-1402/COL868-HLL.git
cd COL868-HLL

# 2. Start PostgreSQL with HLL extension
docker-compose up -d

# 3. Verify container is running
docker ps
```

## Verification Commands
#### 1\. Verify Files (Run in Bash)
```bash
# 1. Check shared library
ls -l /usr/lib/postgresql/17/lib/hll.so

# 2. Check extension control/SQL files
ls -l /usr/share/postgresql/17/extension/hll*

# 3. Check JIT bitcode files
ls -l /usr/lib/postgresql/17/lib/bitcode/hll*

# 4. Verify HLL extension is installed
docker exec -it pgdb psql -U myuser -d mydb -c "\dx hll"
```

### Basic HLL Test Commands (Run in psql)
```sql
-- 1. Create a table
CREATE TABLE test_hll (id integer, items hll);

-- 2. Insert an empty hll set
INSERT INTO test_hll(id, items) VALUES (1, hll_empty());

-- 3. Add two distinct items
UPDATE test_hll SET items = hll_add(items, hll_hash_integer(12345)) WHERE id = 1;
UPDATE test_hll SET items = hll_add(items, hll_hash_text('hello')) WHERE id = 1;

-- 4. Check cardinality (Expected: 2)
SELECT hll_cardinality(items) FROM test_hll WHERE id = 1;

-- 5. Add a duplicate item
UPDATE test_hll SET items = hll_add(items, hll_hash_text('hello')) WHERE id = 1;

-- 6. Check cardinality again using the operator (Expected: 2)
SELECT #items FROM test_hll WHERE id = 1;
```

## Data Characteristics

- **Distribution:** Uniform random (not representative of real-world skew)
- **Data Type:** Integer only (text hashing not tested)
- **Cardinality:** Fixed at 10%

## Running Experiments

**Note:** JIT compilation is **enabled**, which affects first-run performance. All benchmarks include warmup runs to account for JIT compilation overhead.

### Experiment 1: hll_add_agg() - Aggregate Cardinality Estimation

Purpose: Compare HLL approximate counting vs exact COUNT(DISTINCT)

Parameters Varied:

Dataset size: 10K, 100K, 1M, 10M rows
HLL precision: 10, 12, 14
Cardinality: 10% of dataset size

Quick Benchmark (Recommended for Testing)
Runtime: ~5 minutes
Dataset: 100K rows, ~10K distinct values

```bash
# Copy benchmark script to container
docker cp quick_hll_add_agg.sql pgdb:/tmp/

# Run benchmark
docker exec -it pgdb psql -U myuser -d mydb -f /tmp/quick_hll_add_agg.sql

# Export results
docker cp pgdb:/tmp/results_exact.csv ./
docker cp pgdb:/tmp/results_hll.csv ./

# Generate plots
pip install pandas matplotlib seaborn numpy
python quick_plot.py
```

Multi-Scale Benchmark
Runtime: 10-30 mins
Datasets: 10K, 100K, 1M, 10M rows with 10% distinct values

```bash
# Copy comprehensive benchmark script
docker cp benchmark_hll_add_agg.sql pgdb:/tmp/

# Run multi-scale benchmark (this takes time!)
docker exec -it pgdb psql -U myuser -d mydb -f /tmp/benchmark_hll_add_agg.sql

# Export results
docker cp pgdb:/tmp/results_exact.csv ./
docker cp pgdb:/tmp/results_hll.csv ./

# Generate multi-scale plots
pip install pandas matplotlib seaborn numpy
python plot_results.py
```

## Cleanup Commands
- **Stop and Remove Container:**
```bash
docker-compose down
```
- **Stop/Remove Container AND Delete Volume:**
(This is the "full reset." It deletes the `pgdata` volume, forcing your init script to run again on next start)
```bash
docker-compose down -v
```

Full system details available in MANIFEST.md