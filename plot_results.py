#!/usr/bin/env python3
"""
HLL Benchmark Plotting Script
Generates plots for multi-scale analysis (10K, 100K, 1M, 10M rows)
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['font.size'] = 11
plt.rcParams['figure.dpi'] = 300

def compute_scaling_summary(exact_df, hll_df):
    """Compute scaling summary from raw data"""
    scaling_data = []
    
    for size in sorted(exact_df['dataset_size'].unique()):
        exact_subset = exact_df[exact_df['dataset_size'] == size]
        hll_subset = hll_df[hll_df['dataset_size'] == size]
        
        row = {
            'dataset_size': size,
            'distinct_count': exact_subset['distinct_count'].mean(),
            'exact_avg_ms': exact_subset['duration_ms'].mean(),
            'exact_std_ms': exact_subset['duration_ms'].std(),
        }
        
        for prec in [10, 12, 14]:
            hll_prec = hll_subset[hll_subset['precision'] == prec]
            row[f'hll_p{prec}_avg_ms'] = hll_prec['duration_ms'].mean()
            row[f'hll_p{prec}_error'] = hll_prec['relative_error'].mean()
            row[f'hll_p{prec}_storage'] = hll_prec['storage_bytes'].mean()
        
        scaling_data.append(row)
    
    return pd.DataFrame(scaling_data)

def load_data():
    """Load benchmark results from CSV files"""
    try:
        exact_df = pd.read_csv('results_exact.csv')
        hll_df = pd.read_csv('results_hll.csv')
        print("✓ Loaded benchmark data")
        
        # Compute scaling summary from raw data
        scaling_df = compute_scaling_summary(exact_df, hll_df)
        print("✓ Computed scaling summary")
        
        return exact_df, hll_df, scaling_df
    except FileNotFoundError:
        print("✗ No CSV files found. Run benchmark first!")
        return None, None, None

def plot_scaling_performance(scaling_df):
    """Plot 1: Performance scaling across dataset sizes"""
    if scaling_df is None:
        print("⊘ Skipping scaling plot (no multi-scale data)")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    sizes = scaling_df['dataset_size']
    sizes_labels = [f'{s//1000}K' if s < 1000000 else f'{s//1000000}M' 
                    for s in sizes]
    
    # Plot lines for each method
    ax.plot(range(len(sizes)), scaling_df['exact_avg_ms'], 
            marker='o', markersize=10, linewidth=2.5, 
            label='Exact COUNT', color='#e74c3c')
    
    ax.plot(range(len(sizes)), scaling_df['hll_p10_avg_ms'], 
            marker='s', markersize=8, linewidth=2, 
            label='HLL (p=10)', color='#3498db', linestyle='--')
    
    ax.plot(range(len(sizes)), scaling_df['hll_p12_avg_ms'], 
            marker='^', markersize=8, linewidth=2, 
            label='HLL (p=12)', color='#2ecc71', linestyle='--')
    
    ax.plot(range(len(sizes)), scaling_df['hll_p14_avg_ms'], 
            marker='d', markersize=8, linewidth=2, 
            label='HLL (p=14)', color='#f39c12', linestyle='--')
    
    ax.set_xlabel('Dataset Size', fontsize=13, fontweight='bold')
    ax.set_ylabel('Query Latency (ms)', fontsize=13, fontweight='bold')
    ax.set_title('Query Latency vs Dataset Size', fontsize=15, fontweight='bold')
    ax.set_xticks(range(len(sizes)))
    ax.set_xticklabels(sizes_labels)
    ax.legend(loc='upper left', fontsize=11)
    ax.grid(alpha=0.3)
    ax.set_yscale('log')
    
    plt.tight_layout()
    plt.savefig('plot_scaling_performance.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: plot_scaling_performance.png")
    plt.close()

def plot_speedup_vs_scale(scaling_df):
    """Plot 2: Speedup factor across scales"""
    if scaling_df is None:
        print("⊘ Skipping speedup plot (no multi-scale data)")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    sizes = scaling_df['dataset_size']
    sizes_labels = [f'{s//1000}K' if s < 1000000 else f'{s//1000000}M' 
                    for s in sizes]
    
    speedup_p10 = scaling_df['exact_avg_ms'] / scaling_df['hll_p10_avg_ms']
    speedup_p12 = scaling_df['exact_avg_ms'] / scaling_df['hll_p12_avg_ms']
    speedup_p14 = scaling_df['exact_avg_ms'] / scaling_df['hll_p14_avg_ms']
    
    x = np.arange(len(sizes))
    width = 0.25
    
    bars1 = ax.bar(x - width, speedup_p10, width, label='p=10', 
                   color='#3498db', alpha=0.8, edgecolor='black', linewidth=1.5)
    bars2 = ax.bar(x, speedup_p12, width, label='p=12', 
                   color='#2ecc71', alpha=0.8, edgecolor='black', linewidth=1.5)
    bars3 = ax.bar(x + width, speedup_p14, width, label='p=14', 
                   color='#f39c12', alpha=0.8, edgecolor='black', linewidth=1.5)
    
    # Add value labels on bars
    for bars in [bars1, bars2, bars3]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}x',
                   ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    ax.set_xlabel('Dataset Size', fontsize=13, fontweight='bold')
    ax.set_ylabel('Speedup Factor (vs Exact COUNT)', fontsize=13, fontweight='bold')
    ax.set_title('HLL Speedup Across Dataset Sizes', fontsize=15, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(sizes_labels)
    ax.legend(loc='upper left', fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    ax.axhline(y=1, color='red', linestyle='--', linewidth=1, alpha=0.5, label='No speedup')
    
    plt.tight_layout()
    plt.savefig('plot_speedup_scaling.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: plot_speedup_scaling.png")
    plt.close()

def plot_error_vs_scale(scaling_df):
    """Plot 3: Error rates across scales"""
    if scaling_df is None:
        print("⊘ Skipping error plot (no multi-scale data)")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    sizes = scaling_df['dataset_size']
    sizes_labels = [f'{s//1000}K' if s < 1000000 else f'{s//1000000}M' 
                    for s in sizes]
    
    ax.plot(range(len(sizes)), scaling_df['hll_p10_error'], 
            marker='o', markersize=10, linewidth=2.5, 
            label='Precision 10', color='#3498db')
    
    ax.plot(range(len(sizes)), scaling_df['hll_p12_error'], 
            marker='s', markersize=10, linewidth=2.5, 
            label='Precision 12', color='#2ecc71')
    
    ax.plot(range(len(sizes)), scaling_df['hll_p14_error'], 
            marker='^', markersize=10, linewidth=2.5, 
            label='Precision 14', color='#f39c12')
    
    ax.set_xlabel('Dataset Size', fontsize=13, fontweight='bold')
    ax.set_ylabel('Relative Error (%)', fontsize=13, fontweight='bold')
    ax.set_title('HLL Accuracy Across Dataset Sizes', fontsize=15, fontweight='bold')
    ax.set_xticks(range(len(sizes)))
    ax.set_xticklabels(sizes_labels)
    ax.legend(loc='best', fontsize=11)
    ax.grid(alpha=0.3)
    
    # Add 1% error threshold line
    ax.axhline(y=1.0, color='red', linestyle='--', linewidth=1, alpha=0.5, 
               label='1% error threshold')
    
    plt.tight_layout()
    plt.savefig('plot_error_scaling.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: plot_error_scaling.png")
    plt.close()

def plot_storage_vs_scale(scaling_df):
    """Plot 4: Storage requirements"""
    if scaling_df is None:
        print("⊘ Skipping storage plot (no multi-scale data)")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    sizes = scaling_df['dataset_size']
    sizes_labels = [f'{s//1000}K' if s < 1000000 else f'{s//1000000}M' 
                    for s in sizes]
    
    # Convert to KB for readability
    p10_kb = scaling_df['hll_p10_storage'] / 1024
    p12_kb = scaling_df['hll_p12_storage'] / 1024
    p14_kb = scaling_df['hll_p14_storage'] / 1024
    
    x = np.arange(len(sizes))
    width = 0.25
    
    ax.bar(x - width, p10_kb, width, label='p=10', 
           color='#3498db', alpha=0.8, edgecolor='black')
    ax.bar(x, p12_kb, width, label='p=12', 
           color='#2ecc71', alpha=0.8, edgecolor='black')
    ax.bar(x + width, p14_kb, width, label='p=14', 
           color='#f39c12', alpha=0.8, edgecolor='black')
    
    ax.set_xlabel('Dataset Size', fontsize=13, fontweight='bold')
    ax.set_ylabel('Storage Size (KB)', fontsize=13, fontweight='bold')
    ax.set_title('HLL Storage Requirements', 
                 fontsize=15, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(sizes_labels)
    ax.legend(loc='upper left', fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('plot_storage_scaling.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: plot_storage_scaling.png")
    plt.close()

def plot_precision_comparison(hll_df):
    """Plot 5: Detailed precision comparison at largest scale"""
    # Use largest dataset
    max_size = hll_df['dataset_size'].max()
    subset = hll_df[hll_df['dataset_size'] == max_size]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Left: Error distribution
    precisions = sorted(subset['precision'].unique())
    errors_by_prec = [subset[subset['precision'] == p]['relative_error'].values 
                      for p in precisions]
    
    bp = ax1.boxplot(errors_by_prec, labels=[f'p={p}' for p in precisions],
                     patch_artist=True, showmeans=True)
    
    colors = ['#3498db', '#2ecc71', '#f39c12']
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax1.set_xlabel('HLL Precision', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Relative Error (%)', fontsize=12, fontweight='bold')
    ax1.set_title(f'Error Distribution ({max_size:,} rows)', 
                  fontsize=13, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    # Right: Latency distribution
    latencies_by_prec = [subset[subset['precision'] == p]['duration_ms'].values 
                         for p in precisions]
    
    bp2 = ax2.boxplot(latencies_by_prec, labels=[f'p={p}' for p in precisions],
                      patch_artist=True, showmeans=True)
    
    for patch, color in zip(bp2['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax2.set_xlabel('HLL Precision', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Query Latency (ms)', fontsize=12, fontweight='bold')
    ax2.set_title(f'Latency Distribution ({max_size:,} rows)', 
                  fontsize=13, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('plot_precision_comparison.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: plot_precision_comparison.png")
    plt.close()

def create_summary_table(scaling_df):
    """Create summary table"""
    if scaling_df is None:
        print("⊘ No scaling data for summary table")
        return
    
    print("\n" + "="*90)
    print("BENCHMARK RESULTS")
    print("="*90)
    
    print(f"\n{'Dataset':<12} {'Distinct':<10} {'Exact (ms)':<15} {'HLL p=12 (ms)':<15} "
          f"{'Speedup':<10} {'Error %':<10} {'Storage':<10}")
    print("-"*90)
    
    for _, row in scaling_df.iterrows():
        size_str = f"{row['dataset_size']//1000}K" if row['dataset_size'] < 1000000 else f"{row['dataset_size']//1000000}M"
        speedup = row['exact_avg_ms'] / row['hll_p12_avg_ms']
        storage_kb = row['hll_p12_storage'] / 1024
        
        print(f"{size_str:<12} {row['distinct_count']:<10.0f} "
              f"{row['exact_avg_ms']:<15.2f} {row['hll_p12_avg_ms']:<15.2f} "
              f"{speedup:<10.2f}x {row['hll_p12_error']:<10.3f} {storage_kb:<10.2f} KB")
    
    print("="*90 + "\n")

def main():
    """Main function"""
    print("="*50)
    print("HLL BENCHMARK PLOTTING")
    print("="*50)
    
    exact_df, hll_df, scaling_df = load_data()
    
    if exact_df is None or hll_df is None:
        print("✗ Cannot proceed without data files")
        return
    
    print("\nGenerating plots...")
    
    # Multi-scale plots
    plot_scaling_performance(scaling_df)
    plot_speedup_vs_scale(scaling_df)
    plot_error_vs_scale(scaling_df)
    plot_storage_vs_scale(scaling_df)
    
    # Detailed analysis
    plot_precision_comparison(hll_df)
    
    # Summary
    create_summary_table(scaling_df)
    
    print("\n✓ All plots generated successfully!")
    print("\nGenerated files:")
    print("  - plot_scaling_performance.png")
    print("  - plot_speedup_scaling.png")
    print("  - plot_error_scaling.png")
    print("  - plot_storage_scaling.png")
    print("  - plot_precision_comparison.png")

if __name__ == "__main__":
    main()