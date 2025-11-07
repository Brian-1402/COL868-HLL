#!/bin/bash

# =============================================================================
# run_benchmarks.sh
#
# Main execution script for the postgres-hll benchmark suite.
#
# This script:
# 1. Sets up timestamped logging and output directories in /tmp.
# 2. Uses 'tee' to mirror all command output to both logs AND the terminal.
# 3. Uses a 'trap' to ensure logs/outputs are copied back on success OR failure.
# 4. Runs pre-cleanup (99_cleanup.sql).
# 5. Runs data setup (00_setup.sql).
# 6. Executes PSQL-based tests (bulk agg, storage, hashing).
# 7. Executes pgbench-based tests (point insert, read, union).
# 8. Runs final cleanup (99_cleanup.sql).
#
# Usage:
#   cd /code
#   ./run_benchmarks.sh
#
# =============================================================================

set -e
set -o pipefail

# --- Configuration ---
# psql/pgbench connection params.
export PGUSER=myuser
export PGPASSWORD=mypassword
export PGDATABASE=mydb

# Directory where this script and the /sql folder are located
BASE_DIR="/code"
SQL_DIR="$BASE_DIR/sql"

# pgbench test parameters
PGBENCH_RUN_TIME=30 # Max 30s per user request
PGBENCH_CLIENTS=8
PGBENCH_THREADS=2

# --- Setup Directories ---
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Tmp directories for staging
LOG_DIR_TMP="/tmp/hll_bench_logs"
OUT_DIR_TMP="/tmp/hll_bench_outputs"

# Final destination directories on the /code mount
LOG_DIR_FINAL="$BASE_DIR/logs/$TIMESTAMP"
OUT_DIR_FINAL="$BASE_DIR/outputs/$TIMESTAMP"

# --- Define Runners ---
PSQL="psql -v ON_ERROR_STOP=1 -q" # -q for quiet, tee will handle terminal
PGBENCH="pgbench"

# =============================================================================
# 1. TRAP FOR LOG COPYING
#
# This function is triggered by the 'trap' command on any script exit.
# This guarantees that logs/outputs are copied from /tmp, even if
# the script fails validation (due to 'set -e').
# =============================================================================
function copy_results_on_exit {
    echo "---"
    echo ">>> [TRAP/EXIT] Copying results from /tmp to $BASE_DIR..."
    
    # Ensure final directories exist
    mkdir -p "$LOG_DIR_FINAL"
    mkdir -p "$OUT_DIR_FINAL"

    # Copy contents if temp dirs exist
    if [ -d "$LOG_DIR_TMP" ]; then
        echo "    Copying logs: $LOG_DIR_TMP -> $LOG_DIR_FINAL"
        # Use cp -a to preserve attributes, ignore errors if no files
        cp -a "$LOG_DIR_TMP"/* "$LOG_DIR_FINAL/" 2>/dev/null || true
    else
        echo "    No temp log directory found ($LOG_DIR_TMP)."
    fi

    if [ -d "$OUT_DIR_TMP" ]; then
        echo "    Copying outputs: $OUT_DIR_TMP -> $OUT_DIR_FINAL"
        cp -a "$OUT_DIR_TMP"/* "$OUT_DIR_FINAL/" 2>/dev/null || true
    else
        echo "    No temp output directory found ($OUT_DIR_TMP)."
    fi

    echo "---"
    echo "Benchmark run results:"
    echo "Logs:    $LOG_DIR_FINAL"
    echo "Outputs: $OUT_DIR_FINAL"
    echo "========================================="
}

# Set the trap to call the function on EXIT
trap copy_results_on_exit EXIT


# =============================================================================
# SCRIPT START
# =============================================================================
echo "========================================="
echo "HLL BENCHMARK SUITE"
echo "========================================="
echo "Timestamp:        $TIMESTAMP"
echo "Temp Logs:        $LOG_DIR_TMP"
echo "Temp Outputs:     $OUT_DIR_TMP"
echo "Final Logs:       $LOG_DIR_FINAL"
echo "Final Outputs:    $OUT_DIR_FINAL"
echo "---"

# Clean and create temp directories
rm -rf "$LOG_DIR_TMP" && mkdir -p "$LOG_DIR_TMP"
rm -rf "$OUT_DIR_TMP" && mkdir -p "$OUT_DIR_TMP"


# =============================================================================
# 1. PRE-RUN CLEANUP
# =============================================================================
echo ">>> [1/7] Running Pre-Run Cleanup (99_cleanup.sql)..."
# MODIFIED: Use 'tee' to pipe combined stdout/stderr to log AND terminal
$PSQL -f "$SQL_DIR/99_cleanup.sql" 2>&1 | tee "$LOG_DIR_TMP/00_cleanup_pre.log"

# =============================================================================
# 2. SETUP
# =============================================================================
echo ">>> [2/7] Running Setup (00_setup.sql)..."
# MODIFIED: Use 'tee'
$PSQL -f "$SQL_DIR/00_setup.sql" 2>&1 | tee "$LOG_DIR_TMP/01_setup.log"
echo "    Setup complete."

# =============================================================================
# 3. PSQL TESTS (Internal Loops)
# =============================================================================
echo ">>> [3/7] Running PSQL Tests..."

echo "    Running Test: 01_test_bulk_agg.sql"
# MODIFIED: Use 'tee'
$PSQL -f "$SQL_DIR/01_test_bulk_agg.sql" 2>&1 | tee "$LOG_DIR_TMP/01_test_bulk_agg.log"

echo "    Running Test: 02_test_storage.sql"
# MODIFIED: Use 'tee'
$PSQL -f "$SQL_DIR/02_test_storage.sql" 2>&1 | tee "$LOG_DIR_TMP/02_test_storage.log"

echo "    Running Test: 03_test_hashing.sql"
# MODIFIED: Use 'tee'
$PSQL -f "$SQL_DIR/03_test_hashing.sql" 2>&1 | tee "$LOG_DIR_TMP/03_test_hashing.log"

echo "    PSQL tests complete."

# =============================================================================
# 4. PGBENCH TESTS (Duration-based)
# =============================================================================
echo ">>> [4/7] Running pgbench Tests (T=$PGBENCH_RUN_TIME""s, C=$PGBENCH_CLIENTS)..."

# MODIFIED: Use process substitution ( >(tee ...) ) to tee stdout and stderr
# to separate files while also showing both in the terminal.
#   - stdout (logs) goes to .log file AND terminal stdout
#   - stderr (summary) goes to .txt file AND terminal stderr

echo "    Running Test: 04_test_point_insert_low_card.sql"
$PGBENCH -f "$SQL_DIR/04_test_point_insert_low_card.sql" \
    -c $PGBENCH_CLIENTS -j $PGBENCH_THREADS -T $PGBENCH_RUN_TIME -r \
    > >(tee "$LOG_DIR_TMP/04_pgbench_low_card.log") \
    2> >(tee "$OUT_DIR_TMP/04_summary_low_card_insert.txt" >&2)

echo "    Running Test: 05_test_point_insert_high_card.sql"
$PGBENCH -f "$SQL_DIR/05_test_point_insert_high_card.sql" \
    -c $PGBENCH_CLIENTS -j $PGBENCH_THREADS -T $PGBENCH_RUN_TIME -r \
    > >(tee "$LOG_DIR_TMP/05_pgbench_high_card.log") \
    2> >(tee "$OUT_DIR_TMP/05_summary_high_card_insert.txt" >&2)

echo "    Running Test: 06_test_read_cardinality.sql"
$PGBENCH -f "$SQL_DIR/06_test_read_cardinality.sql" \
    -c $PGBENCH_CLIENTS -j $PGBENCH_THREADS -T $PGBENCH_RUN_TIME -r --select-only \
    > >(tee "$LOG_DIR_TMP/06_pgbench_read_card.log") \
    2> >(tee "$OUT_DIR_TMP/06_summary_read_cardinality.txt" >&2)

echo "    Running Test: 07_test_read_union.sql"
$PGBENCH -f "$SQL_DIR/07_test_read_union.sql" \
    -c $PGBENCH_CLIENTS -j $PGBENCH_THREADS -T $PGBENCH_RUN_TIME -r --select-only \
    > >(tee "$LOG_DIR_TMP/07_pgbench_read_union.log") \
    2> >(tee "$OUT_DIR_TMP/07_summary_read_union.txt" >&2)

echo "    pgbench tests complete."

# =============================================================================
# 5. POST-RUN CLEANUP
# =============================================================================
echo ">>> [5/7] Running Post-Run Cleanup (99_cleanup.sql)..."
# MODIFIED: Use 'tee'
$PSQL -f "$SQL_DIR/99_cleanup.sql" 2>&1 | tee "$LOG_DIR_TMP/99_cleanup_post.log"

# =============================================================================
# 6. COPY RESULTS
# =============================================================================
# MODIFIED: This step is now handled automatically by the 'trap' function
# defined at the top of the script. It will run on exit,
# whether this point is reached or not.
echo ">>> [6/7] Copying results (handled by exit trap)..."


# =============================================================================
# 7. DONE
# =============================================================================
echo ">>> [7/7] Benchmark script finished."
# The trap function will now run and print the final summary.
