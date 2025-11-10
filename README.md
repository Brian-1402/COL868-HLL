# HyperLogLog (HLL) Benchmarking in PostgreSQL

This experiment aims to evaluate **HyperLogLog (HLL)** probabilistic data structure for approximate distinct counting in PostgreSQL. We benchmark **HLL** against exact **COUNT(DISTINCT)** operations across multiple scales (database sizes) and precision; analyzing accuracy, performance, and memory usage.

## Setup Commands

Prerequisites - Docker Desktop (with Docker Compose)

- **Clone and Run Compose:**

```bash
# 1. Clone repository
git clone https://github.com/Brian-1402/COL868-HLL.git
cd COL868-HLL

# 2. Start PostgreSQL with HLL extension
docker-compose up -d

#c3. Verify hll extension is installed (optional)
docker exec -it pgdb psql -U myuser -d mydb -c "\dx hll"
```

### Basic HLL Test Commands (Run in psql)

```sql

-- Enter psql
docker exec -it pgdb psql -U myuser -d mydb

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


## Experiments

### Experiment 1:

Test **HLL approximate counting** using **hll_add_agg** and **hll_cardinality** and  compare it against baseline **exact COUNT(DISTINCT)**

** Test Configuration:**
- HLL precision: 10, 12, 14
- Datasets: 10K, 100K, 1M, 10M rows
- Cardinality: 10% of dataset size
- Data Type: Integer only
- Distribution: Uniform random (not representative of real-world skew)

```bash
# Run experiment
docker exec -it pgdb psql -f experiment_1.sql

# Make plots
docker compose -f docker-compose.graphs.yml run --rm plotter python plot_experiment_1.py
```

**Plots generated:**

- Query latency ( for hll precisions and exact COUNT)
- Speedup factors across scales for each precision compared to baseline
- Accuracy (relative error %) across precisions
- Storage vs dataset size for hll
- Accuracy vs storage trade-off across precisions

### Experiment 2:

Test **hll_union_agg** performance by evaluating real-world analytics pattern where daily sketches are pre-computed once, then unioned on-demand for any time range.

**What it tests:**
- Union performance across time windows (7, 14, 30, 60, 90 days)
- Compares performance with exact re-aggregation - latency, speedup, relative error
- Storage efficiency across precisions
- Compares precisions - latency, error ,speedup

**Dataset:**
- 1.28M events over 90 days
- ~50K unique users total
- ~10K-15K events per day

**User Distribution (Power-law - Realistic):**
- 10% super-active users (0-1K user IDs)
- 30% active users (0-10K user IDs)  
- 60% casual users (0-50K user IDs)

**Test Configuration:**
- Precisions tested: 10, 12, 14
- Time windows: 7, 14, 30, 60, 90 days

*Note:* Pre-computing daily sketches enables instant queries for any time range (e.g., "last 30 days unique users") without re-scanning raw data.

```bash
# Run experiment
docker exec -it pgdb psql -f experiment_2.sql

# Make plots
docker compose -f docker-compose.graphs.yml run --rm plotter python plot_experiment_2.py
```


## Cleanup Commands

- **Stop and Remove Container:**
```bash
docker-compose down
```

*System details available in **MANIFEST.md***
