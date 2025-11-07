#!/usr/bin/env python3
"""
HLL Benchmark Plotting Script
Generates all required plots for the E&A report
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os # Added for creating directories

# --- Configuration ---
# This prefix must match the one used in the SQL benchmark script
EXPERIMENT_PREFIX = 'hll_add_agg' 
INPUT_DIR = '/code/results'
OUTPUT_DIR = f'/code/plots/{EXPERIMENT_PREFIX}'
# --- End Configuration ---

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['font.size'] = 10

# Load data (you'll need to export from PostgreSQL first)
# For now, creating sample data structure

def create_sample_data():
    """Create sample data if CSV files don't exist"""
    # Exact count results
    exact_data = {
        'test_name': ['exact_count'] * 5,
        'row_count': [1000000] * 5,
        'distinct_count': [100000] * 5,
        'duration_ms': [245.3, 243.1, 246.8, 244.5, 245.9],
        'run_number': [1, 2, 3, 4, 5]
    }
    
    # HLL results for different precisions
    hll_data = {
        'precision': [10]*5 + [12]*5 + [14]*5,
        'row_count': [1000000] * 15,
        'hll_estimate': [99234]*5 + [100123]*5 + [99987]*5,
        'exact_count': [100000] * 15,
        'relative_error': [0.766]*5 + [0.123]*5 + [0.013]*5,
        'duration_ms': [45.2, 44.8, 45.5, 45.1, 44.9,
                        52.3, 51.9, 52.7, 52.1, 52.5,
                        67.8, 67.2, 68.1, 67.5, 67.9],
        'storage_bytes': [1280]*5 + [5120]*5 + [20480]*5,
        'run_number': list(range(1,6))*3
    }
    
    return pd.DataFrame(exact_data), pd.DataFrame(hll_data)

def plot_latency_comparison(exact_df, hll_df):
    """Plot 1: Latency comparison between exact and HLL methods"""
    fig, ax = plt.subplots(figsize=(8, 5))
    
    # Calculate means and std
    exact_mean = exact_df['duration_ms'].mean()
    exact_std = exact_df['duration_ms'].std()
    
    hll_means = hll_df.groupby('precision')['duration_ms'].mean()
    hll_stds = hll_df.groupby('precision')['duration_ms'].std()
    
    # Prepare data for plotting
    methods = ['Exact COUNT'] + [f'HLL (p={p})' for p in hll_means.index]
    means = [exact_mean] + list(hll_means)
    stds = [exact_std] + list(hll_stds)
    
    # Create bar plot
    x_pos = np.arange(len(methods))
    bars = ax.bar(x_pos, means, yerr=stds, capsize=5, 
                   color=['#e74c3c', '#3498db', '#2ecc71', '#f39c12'])
    
    ax.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
    ax.set_xlabel('Method', fontsize=12, fontweight='bold')
    ax.set_title('Query Latency: Exact COUNT vs HLL', fontsize=14, fontweight='bold')
    ax.set_xticks(x_pos)
    ax.set_xticklabels(methods, rotation=15, ha='right')
    ax.grid(axis='y', alpha=0.3)
    
    # Add speedup annotations
    for i, (mean, std) in enumerate(zip(means[1:], stds[1:]), 1):
        speedup = exact_mean / mean
        ax.text(i, mean + std + 5, f'{speedup:.1f}x', 
                ha='center', fontweight='bold', fontsize=10)
    
    plt.tight_layout()
    output_path = f'{OUTPUT_DIR}/plot1_latency_comparison.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_path}")
    plt.close()

def plot_accuracy_vs_storage(hll_df):
    """Plot 2: Accuracy vs Storage trade-off"""
    fig, ax1 = plt.subplots(figsize=(8, 5))
    
    # Calculate means
    summary = hll_df.groupby('precision').agg({
        'relative_error': 'mean',
        'storage_bytes': 'mean'
    }).reset_index()
    
    # Error on left axis
    color1 = '#e74c3c'
    ax1.set_xlabel('HLL Precision Parameter', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Relative Error (%)', color=color1, fontsize=12, fontweight='bold')
    line1 = ax1.plot(summary['precision'], summary['relative_error'], 
                       marker='o', markersize=10, linewidth=2.5, 
                       color=color1, label='Error')
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.set_xticks(summary['precision'])
    ax1.grid(alpha=0.3)
    
    # Storage on right axis
    ax2 = ax1.twinx()
    color2 = '#3498db'
    ax2.set_ylabel('Storage Size (bytes)', color=color2, fontsize=12, fontweight='bold')
    line2 = ax2.plot(summary['precision'], summary['storage_bytes'], 
                       marker='s', markersize=10, linewidth=2.5, 
                       color=color2, label='Storage', linestyle='--')
    ax2.tick_params(axis='y', labelcolor=color2)
    
    # Add value labels
    for i, row in summary.iterrows():
        ax1.text(row['precision'], row['relative_error'] + 0.05, 
                 f"{row['relative_error']:.2f}%", 
                 ha='center', fontsize=9, fontweight='bold')
        ax2.text(row['precision'], row['storage_bytes'] + 500, 
                 f"{row['storage_bytes']:.0f}B", 
                 ha='center', fontsize=9, fontweight='bold')
    
    ax1.set_title('HLL: Accuracy vs Storage Trade-off', fontsize=14, fontweight='bold')
    
    # Combined legend
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper right')
    
    plt.tight_layout()
    output_path = f'{OUTPUT_DIR}/plot2_accuracy_storage_tradeoff.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_path}")
    plt.close()

def plot_speedup_vs_error(exact_df, hll_df):
    """Plot 3: Speedup vs Error scatter"""
    fig, ax = plt.subplots(figsize=(8, 5))
    
    exact_mean = exact_df['duration_ms'].mean()
    
    summary = hll_df.groupby('precision').agg({
        'duration_ms': 'mean',
        'relative_error': 'mean'
    }).reset_index()
    
    summary['speedup'] = exact_mean / summary['duration_ms']
    
    # Scatter plot with different colors for each precision
    colors = ['#3498db', '#2ecc71', '#f39c12']
    for i, (idx, row) in enumerate(summary.iterrows()):
        ax.scatter(row['relative_error'], row['speedup'], 
                   s=300, c=colors[i], alpha=0.7, edgecolors='black', linewidth=2,
                   label=f'Precision {row["precision"]}')
        ax.annotate(f'p={row["precision"]:.0f}', 
                    xy=(row['relative_error'], row['speedup']),
                    xytext=(10, 10), textcoords='offset points',
                    fontsize=10, fontweight='bold')
    
    ax.set_xlabel('Relative Error (%)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Speedup Factor (vs Exact COUNT)', fontsize=12, fontweight='bold')
    ax.set_title('HLL: Performance vs Accuracy Trade-off', fontsize=14, fontweight='bold')
    ax.legend(loc='best', fontsize=10)
    ax.grid(alpha=0.3)
    
    plt.tight_layout()
    output_path = f'{OUTPUT_DIR}/plot3_speedup_vs_error.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_path}")
    plt.close()

def create_results_table(exact_df, hll_df):
    """Create summary table for the paper"""
    exact_mean = exact_df['duration_ms'].mean()
    exact_std = exact_df['duration_ms'].std()
    exact_count = exact_df['distinct_count'].iloc[0]
    
    print("\n" + "="*70)
    print("RESULTS TABLE FOR PAPER")
    print("="*70)
    print(f"\n{'Method':<20} {'Estimate':<12} {'Error %':<10} {'Latency (ms)':<15} {'Storage (B)':<12}")
    print("-"*70)
    print(f"{'Exact COUNT':<20} {exact_count:<12.0f} {'0.00':<10} {exact_mean:<7.1f}±{exact_std:<5.1f} {'N/A':<12}")
    
    for prec in sorted(hll_df['precision'].unique()):
        subset = hll_df[hll_df['precision'] == prec]
        mean_lat = subset['duration_ms'].mean()
        std_lat = subset['duration_ms'].std()
        mean_est = subset['hll_estimate'].mean()
        mean_err = subset['relative_error'].mean()
        mean_stor = subset['storage_bytes'].mean()
        speedup = exact_mean / mean_lat
        
        print(f"{'HLL (p=' + str(prec) + ')':<20} {mean_est:<12.0f} {mean_err:<10.2f} "
              f"{mean_lat:<7.1f}±{std_lat:<5.1f} {mean_stor:<12.0f}")
        print(f"{'  Speedup: ' + f'{speedup:.1f}x':<20}")
    
    print("="*70 + "\n")

def main():
    """Main function to generate all plots"""
    print(f"HLL Benchmark Plotting Script ({EXPERIMENT_PREFIX})")
    print("=" * 50)
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"✓ Output directory set to: {OUTPUT_DIR}")
    
    # Define input file paths
    exact_csv = f'{INPUT_DIR}/{EXPERIMENT_PREFIX}_exact.csv'
    hll_csv = f'{INPUT_DIR}/{EXPERIMENT_PREFIX}_hll.csv'
    
    try:
        # Try to load real data
        exact_df = pd.read_csv(exact_csv)
        hll_df = pd.read_csv(hll_csv)
        print(f"✓ Loaded data from {exact_csv} and {hll_csv}")
    except FileNotFoundError:
        # print(f"⚠ CSV files not found at {INPUT_DIR}/, using sample data")
        print(f"⚠ CSV files not found at {INPUT_DIR}/")
        print("  Run the SQL benchmark first to generate CSV files!")
        return
        # exact_df, hll_df = create_sample_data()
    
    print("\nGenerating plots...")
    plot_latency_comparison(exact_df, hll_df)
    plot_accuracy_vs_storage(hll_df)
    plot_speedup_vs_error(exact_df, hll_df)
    
    print("\nGenerating summary table...")
    create_results_table(exact_df, hll_df)
    
    print("\n✓ All plots generated successfully!")
    print(f"  Files saved in: {OUTPUT_DIR}")
    print(f"  > plot1_latency_comparison.png")
    print(f"  > plot2_accuracy_storage_tradeoff.png")
    print(f"  > plot3_speedup_vs_error.png")

if __name__ == "__main__":
    main()
