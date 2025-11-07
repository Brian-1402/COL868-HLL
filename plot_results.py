#!/usr/bin/env python3

"""
Parses and plots the results from the postgres-hll benchmark suite.

This script reads the CSV and pgbench summary .txt files from a specified
output directory, processes them with pandas, and generates plots
using matplotlib/seaborn.

Usage:
    python3 plot_results.py /path/to/outputs/20250101_120000
"""

import argparse
import os
import re
import warnings
from pathlib import Path
from typing import Dict, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Suppress warnings from matplotlib/seaborn
warnings.filterwarnings("ignore")

# Set a consistent plot style
sns.set_theme(style="whitegrid", palette="muted")


def load_data(
    base_path: Path, filename: str
) -> Optional[pd.DataFrame]:
    """
    Safely loads a CSV file from the output directory.

    Args:
        base_path: The Path object to the results directory.
        filename: The name of the CSV file.

    Returns:
        A pandas DataFrame or None if the file is not found.
    """
    filepath = base_path / filename
    if not filepath.exists():
        print(f"Warning: Could not find results file: {filepath}")
        return None
    try:
        return pd.read_csv(filepath)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None


def parse_pgbench_summary(
    base_path: Path, filename: str
) -> Optional[Dict[str, float]]:
    """
    Parses tps and latency from a pgbench summary .txt file.

    Args:
        base_path: The Path object to the results directory.
        filename: The name of the .txt file.

    Returns:
        A dictionary with 'tps' and 'latency' or None.
    """
    filepath = base_path / filename
    if not filepath.exists():
        print(f"Warning: Could not find pgbench file: {filepath}")
        return None

    try:
        with open(filepath, "r") as f:
            content = f.read()

        tps_match = re.search(
            r"tps = (\d+\.\d+) \(excluding", content
        )
        lat_match = re.search(
            r"latency average = (\d+\.\d+) ms", content
        )

        if not tps_match or not lat_match:
            print(f"Warning: Could not parse summary: {filepath}")
            return None

        return {
            "tps": float(tps_match.group(1)),
            "latency": float(lat_match.group(1)),
        }
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None


def plot_bulk_agg(
    df_exact: pd.DataFrame, df_hll: pd.DataFrame, save_path: Path
):
    """
    Plots bulk aggregation speed and accuracy.
    """
    if df_exact is None or df_hll is None:
        print("Skipping plot_bulk_agg: Missing data.")
        return

    print("Plotting: Bulk Aggregation")

    # 1. Plot Speed (Duration)
    avg_exact_ms = df_exact["duration_ms"].mean()
    df_hll_avg = (
        df_hll.groupby("test_name")["duration_ms"].mean().reset_index()
    )
    
    # Add exact to the dataframe for plotting
    exact_row = pd.DataFrame(
        [{"test_name": "exact_count", "duration_ms": avg_exact_ms}]
    )
    df_speed = pd.concat([exact_row, df_hll_avg], ignore_index=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=df_speed, x="test_name", y="duration_ms", ax=ax)
    ax.set_title("Bulk Aggregation Speed (Lower is Better)")
    ax.set_ylabel("Average Duration (ms)")
    ax.set_xlabel("Test Case")
    ax.bar_label(ax.containers[0], fmt="%.1f ms")
    plt.tight_layout()
    plt.savefig(save_path / "1_bulk_agg_speed.png")
    plt.close(fig)

    # 2. Plot Relative Error
    df_error_avg = (
        df_hll.groupby("test_name")["relative_error"]
        .mean()
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=df_error_avg, x="test_name", y="relative_error", ax=ax)
    ax.set_title("HLL Relative Error (Lower is Better)")
    ax.set_ylabel("Average Relative Error (%)")
    ax.set_xlabel("Test Case")
    ax.bar_label(ax.containers[0], fmt="%.3f %%")
    plt.tight_layout()
    plt.savefig(save_path / "2_bulk_agg_error.png")
    plt.close(fig)


def plot_storage(df_storage: pd.DataFrame, save_path: Path):
    """
    Plots storage footprint across states.
    """
    if df_storage is None:
        print("Skipping plot_storage: Missing data.")
        return

    print("Plotting: Storage Footprint")

    fig, ax = plt.subplots(figsize=(12, 7))
    sns.barplot(
        data=df_storage,
        x="item_count",
        y="storage_bytes",
        hue="hll_type",
        ax=ax,
    )
    ax.set_title("HLL Storage Footprint by Item Count and Type")
    ax.set_ylabel("Storage (Bytes)")
    ax.set_xlabel("Distinct Items Added")
    ax.legend(title="HLL Type", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig(save_path / "3_storage_footprint.png")
    plt.close(fig)


def plot_hashing(df_hashing: pd.DataFrame, save_path: Path):
    """
    Plots hashing function overhead.
    """
    if df_hashing is None:
        print("Skipping plot_hashing: Missing data.")
        return

    print("Plotting: Hashing Overhead")
    
    df_hash_avg = (
        df_hashing.groupby("test_name")["duration_ms"].mean().reset_index()
    )

    fig, ax = plt.subplots(figsize=(8, 6))
    sns.barplot(data=df_hash_avg, x="test_name", y="duration_ms", ax=ax)
    ax.set_title("Hashing Function Overhead (Lower is Better)")
    ax.set_ylabel("Average Duration (ms)")
    ax.set_xlabel("Hash Function")
    ax.bar_label(ax.containers[0], fmt="%.1f ms")
    plt.tight_layout()
    plt.savefig(save_path / "4_hashing_overhead.png")
    plt.close(fig)


def plot_pgbench_results(
    pgbench_data: Dict[str, Dict[str, float]], save_path: Path
):
    """
    Plots TPS and Latency from pgbench tests.
    """
    if not pgbench_data:
        print("Skipping plot_pgbench_results: No pgbench data parsed.")
        return

    print("Plotting: pgbench Results")

    # Convert to DataFrame for easier plotting
    df = pd.DataFrame.from_dict(pgbench_data, orient="index")
    df = df.reset_index().rename(columns={"index": "test_name"})

    # 1. Plot Insert TPS
    df_insert = df[df["test_name"].str.contains("insert")]
    if not df_insert.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.barplot(data=df_insert, x="test_name", y="tps", ax=ax)
        ax.set_title("Point Insert Throughput (Higher is Better)")
        ax.set_ylabel("Transactions per Second (TPS)")
        ax.set_xlabel("Test Case")
        ax.bar_label(ax.containers[0], fmt="%.1f")
        plt.tight_layout()
        plt.savefig(save_path / "5_insert_tps.png")
        plt.close(fig)

    # 2. Plot Read Latency
    df_read = df[df["test_name"].str.contains("read")]
    if not df_read.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.barplot(data=df_read, x="test_name", y="latency", ax=ax)
        ax.set_title("Read Query Latency (Lower is Better)")
        ax.set_ylabel("Average Latency (ms)")
        ax.set_xlabel("Test Case")
        ax.bar_label(ax.containers[0], fmt="%.2f ms")
        plt.tight_layout()
        plt.savefig(save_path / "6_read_latency.png")
        plt.close(fig)


def main():
    """
    Main entry point for the plotting script.
    """
    parser = argparse.ArgumentParser(
        description="Parse and plot postgres-hll benchmark results."
    )
    parser.add_argument(
        "input_dir",
        type=str,
        help="Path to the timestamped output directory (e.g., /code/outputs/20250101_120000)",
    )
    args = parser.parse_args()

    input_path = Path(args.input_dir)
    if not input_path.is_dir():
        print(f"Error: Input directory not found: {input_path}")
        return

    # Create a 'plots' subdirectory
    plot_path = input_path / "plots"
    plot_path.mkdir(exist_ok=True)
    print(f"Saving plots to: {plot_path}")

    # --- Load PSQL CSV Data ---
    df_bulk_exact = load_data(
        input_path, "01_results_bulk_exact.csv"
    )
    df_bulk_hll = load_data(input_path, "01_results_bulk_hll.csv")
    df_storage = load_data(input_path, "02_results_storage.csv")
    df_hashing = load_data(input_path, "03_results_hashing.csv")

    # --- Parse pgbench Data ---
    pgbench_results = {}
    test_files = {
        "low_card_insert": "04_summary_low_card_insert.txt",
        "high_card_insert": "05_summary_high_card_insert.txt",
        "read_cardinality": "06_summary_read_cardinality.txt",
        "read_union": "07_summary_read_union.txt",
    }
    for test_name, filename in test_files.items():
        parsed = parse_pgbench_summary(input_path, filename)
        if parsed:
            pgbench_results[test_name] = parsed

    # --- Generate Plots ---
    plot_bulk_agg(df_bulk_exact, df_bulk_hll, plot_path)
    plot_storage(df_storage, plot_path)
    plot_hashing(df_hashing, plot_path)
    plot_pgbench_results(pgbench_results, plot_path)

    print("-" * 40)
    print("Plot generation complete.")
    print(f"All plots saved in: {plot_path.resolve()}")
    print("-" * 40)


if __name__ == "__main__":
    main()
