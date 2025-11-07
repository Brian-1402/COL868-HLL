## Setup Commands
- **Run Compose:**
```bash
docker-compose up -d
```
- **Enter Bash:**
```bash
docker exec -it pgdb bash
```
- **Enter psql:**
```bash
docker exec -it pgdb psql
```

## Running benchmarks
- Todo

## Making plots
```bash
docker compose -f docker-compose.graphs.yml run --rm plotter
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
```
#### 2\. Verify Extension (Run in psql)
```sql
-- Check if available (should be listed) and installed (init script)
\dx hll

-- Alternative SQL to check for installation in 'mydb'
SELECT * FROM pg_extension WHERE extname = 'hll';
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

## Ideas
- Will password encryption for database access slow down the benchmarking? can I disable password login for psql
- Seems like building the extension runs cpp files whose outputs are saved in .sql files. It is based on system-dependent properties? there
  are points of errors here if the build process somehow makes the extension with some bottlenecks
- It's confirmed that this extension uses LLVM bitcode JIT compilation. Meaning, warmups are required for proper benchmarking so that
  initial compilation delay is reduced.
  - jit Parameter: Performance will be drastically different depending on the postgresql.conf setting jit = on (default) vs. jit = off. When
    off, the bitcode is ignored, and only the standard hll.so is called. Must test and report both.
