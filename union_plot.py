#!/usr/bin/env python3
"""
HLL Union Benchmark Visualization and Analysis
Performs Phase 4 analysis (aggregation/calculation) and Phase 5 plotting.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)
plt.rcParams['font.size'] = 10

# Create output directory
output_dir = Path('./plots/hll_union')
output_dir.mkdir(parents=True, exist_ok=True)
tables_dir = Path('./tables/hll_union')

# Load raw detailed data
print("Loading raw benchmark results for analysis...")
try:
    # These two files are the *only* inputs from the SQL script now.
    union_df = pd.read_csv(tables_dir / 'union_detailed.csv')
    exact_df = pd.read_csv(tables_dir / 'exact_detailed.csv')
except FileNotFoundError as e:
    print(f"Error: One or more raw data files not found. Ensure the SQL script ran successfully.")
    print(f"Missing file: {e.filename}")
    exit(1)

print(f"Loaded {len(union_df)} union test records")
print(f"Loaded {len(exact_df)} exact test records")


print("\nPerforming Data Aggregation and Calculation...")

# 1. Aggregate union stats (mean time, mean estimate, total size)
union_stats = union_df.groupby(['precision', 'num_days'], as_index=False).agg(
    avg_estimate=('estimated_count', 'mean'),
    union_time_ms=('query_time_ms', 'mean'),
    union_stddev_ms=('query_time_ms', 'std'),
    total_sketch_bytes=('total_sketch_size_bytes', 'max') # Max size is consistent for a given run
)

# 2. Aggregate exact stats (mean time, true count)
exact_stats = exact_df.groupby('num_days', as_index=False).agg(
    exact_count=('exact_count', 'mean'),
    exact_time_ms=('query_time_ms', 'mean'),
    exact_stddev_ms=('query_time_ms', 'std')
)

# 3. Join and calculate derived metrics
comparison_df = union_stats.merge(exact_stats, on='num_days')

comparison_df['error_absolute'] = abs(comparison_df['avg_estimate'] - comparison_df['exact_count'])
comparison_df['error_pct'] = (comparison_df['error_absolute'] / comparison_df['exact_count']) * 100
comparison_df['speedup_factor'] = comparison_df['exact_time_ms'] / comparison_df['union_time_ms']
comparison_df['sketch_size_kb'] = comparison_df['total_sketch_bytes'] / 1024

# Calculate efficiency score (for Plot 4)
comparison_df['efficiency_score'] = comparison_df['speedup_factor'] / comparison_df['error_pct']

# Save the generated comparison data back to a CSV (replacing the old SQL export)
comparison_df.to_csv(tables_dir / 'comparison.csv', index=False)
print(f"Saved aggregated results to: {tables_dir / 'comparison.csv'}")

# Use this comparison_df for all subsequent plotting and statistics
# ============================================================================
# PLOT 1: Speedup Comparison by Time Window
# ============================================================================
print("\nGenerating Plot 1: Speedup by Time Window...")

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('HLL Union vs Exact Re-aggregation Performance', fontsize=16, fontweight='bold')

# Plot 1a: Query Time Comparison
ax1 = axes[0, 0]
width = 0.35
x = np.arange(len(comparison_df['num_days'].unique()))
days_labels = sorted(comparison_df['num_days'].unique())

for i, precision in enumerate([10, 12, 14]):
    data = comparison_df[comparison_df['precision'] == precision]
    offset = width * (i - 1)
    ax1.bar(x + offset, data['union_time_ms'], width * 0.9, 
            label=f'HLL Union (p={precision})', alpha=0.8)

# Add exact time as a line
exact_times = comparison_df[comparison_df['precision'] == 12].groupby('num_days')['exact_time_ms'].mean()
ax1.plot(x, exact_times.values, 'r--', linewidth=2, marker='o', markersize=8, 
         label='Exact Re-aggregation', zorder=10)

ax1.set_xlabel('Time Window (days)', fontweight='bold')
ax1.set_ylabel('Query Time (ms)', fontweight='bold')
ax1.set_title('Query Time: HLL Union vs Exact Count')
ax1.set_xticks(x)
ax1.set_xticklabels(days_labels)
ax1.legend()
ax1.grid(True, alpha=0.3)

# Plot 1b: Speedup Factor
ax2 = axes[0, 1]
for precision in [10, 12, 14]:
    data = comparison_df[comparison_df['precision'] == precision].sort_values('num_days')
    ax2.plot(data['num_days'], data['speedup_factor'], marker='o', linewidth=2,
             markersize=8, label=f'Precision {precision}')

ax2.axhline(y=1, color='red', linestyle='--', linewidth=1, alpha=0.5, label='No speedup')
ax2.set_xlabel('Time Window (days)', fontweight='bold')
ax2.set_ylabel('Speedup Factor (x)', fontweight='bold')
ax2.set_title('Speedup: Exact Time / Union Time')
ax2.legend()
ax2.grid(True, alpha=0.3)

# Add value labels
for precision in [10, 12, 14]:
    data = comparison_df[comparison_df['precision'] == precision].sort_values('num_days')
    for x_val, y_val in zip(data['num_days'], data['speedup_factor']):
        ax2.text(x_val, y_val + 0.5, f'{y_val:.1f}x', ha='center', fontsize=8)

# Plot 1c: Accuracy (Error %)
ax3 = axes[1, 0]
for precision in [10, 12, 14]:
    data = comparison_df[comparison_df['precision'] == precision].sort_values('num_days')
    ax3.plot(data['num_days'], data['error_pct'], marker='s', linewidth=2,
             markersize=8, label=f'Precision {precision}')

ax3.set_xlabel('Time Window (days)', fontweight='bold')
ax3.set_ylabel('Error (%)', fontweight='bold')
ax3.set_title('Accuracy: Estimation Error')
ax3.legend()
ax3.grid(True, alpha=0.3)
ax3.set_ylim(bottom=0)

# Add theoretical error line
theoretical_errors = {10: 1.62, 12: 0.81, 14: 0.41}
for precision, error in theoretical_errors.items():
    ax3.axhline(y=error, linestyle=':', alpha=0.3, 
                label=f'p={precision} theoretical ({error}%)')

# Plot 1d: Storage Efficiency
ax4 = axes[1, 1]
storage_data = comparison_df.groupby('precision').agg({
    'sketch_size_kb': 'mean',
    'error_pct': 'mean'
}).reset_index()

colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
bars = ax4.bar(storage_data['precision'].astype(str), storage_data['sketch_size_kb'], 
               color=colors, alpha=0.7, edgecolor='black', linewidth=1.5)

ax4.set_xlabel('Precision', fontweight='bold')
ax4.set_ylabel('Average Total Sketch Size (KB)', fontweight='bold')
ax4.set_title('Storage Requirements by Precision')
ax4.grid(True, alpha=0.3, axis='y')

# Add error rate on top of bars
for bar, error in zip(bars, storage_data['error_pct']):
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height + 5,
            f'{error:.2f}% error',
            ha='center', va='bottom', fontweight='bold', fontsize=9)

plt.tight_layout()
plt.savefig(output_dir / 'hll_union_performance.png', dpi=300, bbox_inches='tight')
print(f"Saved: {output_dir / 'hll_union_performance.png'}")

# ============================================================================
# PLOT 2: Detailed Time Distribution
# ============================================================================
print("\nGenerating Plot 2: Detailed Time Distribution...")

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle('Query Time Distribution Analysis', fontsize=16, fontweight='bold')

# Plot 2a: Union time distribution
ax1 = axes[0]
for precision in [10, 12, 14]:
    data = union_df[union_df['precision'] == precision]
    ax1.hist(data['query_time_ms'], bins=20, alpha=0.5, label=f'Precision {precision}',
             edgecolor='black')

ax1.set_xlabel('Query Time (ms)', fontweight='bold')
ax1.set_ylabel('Frequency', fontweight='bold')
ax1.set_title('HLL Union Query Time Distribution')
ax1.legend()
ax1.grid(True, alpha=0.3)

# Plot 2b: Exact re-aggregation time distribution
ax2 = axes[1]
for num_days in sorted(exact_df['num_days'].unique()):
    data = exact_df[exact_df['num_days'] == num_days]
    ax2.hist(data['query_time_ms'], bins=15, alpha=0.5, 
             label=f'{num_days} days', edgecolor='black')

ax2.set_xlabel('Query Time (ms)', fontweight='bold')
ax2.set_ylabel('Frequency', fontweight='bold')
ax2.set_title('Exact Re-aggregation Time Distribution')
ax2.legend()
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(output_dir / 'time_distribution.png', dpi=300, bbox_inches='tight')
print(f"Saved: {output_dir / 'time_distribution.png'}")

# ============================================================================
# PLOT 3: Precision Trade-offs
# ============================================================================
print("\nGenerating Plot 3: Precision Trade-offs...")

fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.suptitle('Precision Trade-off Analysis', fontsize=16, fontweight='bold')

precisions = [10, 12, 14]
colors_prec = ['#3498db', '#e74c3c', '#2ecc71']

# Plot 3a: Error vs Query Time
ax1 = axes[0]
for precision, color in zip(precisions, colors_prec):
    data = comparison_df[comparison_df['precision'] == precision]
    avg_error = data['error_pct'].mean()
    avg_time = data['union_time_ms'].mean()
    
    ax1.scatter(avg_error, avg_time, s=500, alpha=0.6, color=color, 
                edgecolor='black', linewidth=2, label=f'p={precision}')
    ax1.text(avg_error, avg_time, f'p={precision}', ha='center', va='center',
            fontweight='bold', fontsize=11)

ax1.set_xlabel('Average Error (%)', fontweight='bold')
ax1.set_ylabel('Average Query Time (ms)', fontweight='bold')
ax1.set_title('Error vs Speed Trade-off')
ax1.legend()
ax1.grid(True, alpha=0.3)

# Plot 3b: Error vs Storage
ax2 = axes[1]
for precision, color in zip(precisions, colors_prec):
    data = comparison_df[comparison_df['precision'] == precision]
    avg_error = data['error_pct'].mean()
    avg_storage = data['sketch_size_kb'].mean()
    
    ax2.scatter(avg_storage, avg_error, s=500, alpha=0.6, color=color,
                edgecolor='black', linewidth=2, label=f'p={precision}')
    ax2.text(avg_storage, avg_error, f'p={precision}', ha='center', va='center',
            fontweight='bold', fontsize=11)

ax2.set_xlabel('Average Storage (KB)', fontweight='bold')
ax2.set_ylabel('Average Error (%)', fontweight='bold')
ax2.set_title('Error vs Storage Trade-off')
ax2.legend()
ax2.grid(True, alpha=0.3)

# Plot 3c: Speedup by Precision
ax3 = axes[2]
speedup_by_prec = comparison_df.groupby('precision')['speedup_factor'].agg(['mean', 'std'])
x_pos = np.arange(len(precisions))

bars = ax3.bar(x_pos, speedup_by_prec['mean'], yerr=speedup_by_prec['std'],
               color=colors_prec, alpha=0.7, edgecolor='black', linewidth=1.5,
               capsize=5, error_kw={'linewidth': 2})

ax3.set_xlabel('Precision', fontweight='bold')
ax3.set_ylabel('Average Speedup Factor (x)', fontweight='bold')
ax3.set_title('Average Speedup by Precision')
ax3.set_xticks(x_pos)
ax3.set_xticklabels([f'p={p}' for p in precisions])
ax3.grid(True, alpha=0.3, axis='y')

# Add value labels
for i, (bar, val) in enumerate(zip(bars, speedup_by_prec['mean'])):
    ax3.text(bar.get_x() + bar.get_width()/2., val + 0.5,
            f'{val:.1f}x', ha='center', va='bottom', fontweight='bold', fontsize=11)

plt.tight_layout()
plt.savefig(output_dir / 'precision_tradeoffs.png', dpi=300, bbox_inches='tight')
print(f"Saved: {output_dir / 'precision_tradeoffs.png'}")

# ============================================================================
# PLOT 4: Scalability Analysis
# ============================================================================
print("\nGenerating Plot 4: Scalability Analysis...")

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Scalability and Performance Characteristics', fontsize=16, fontweight='bold')

# Plot 4a: Time scaling with number of days
ax1 = axes[0, 0]
for precision in precisions:
    data = comparison_df[comparison_df['precision'] == precision].sort_values('num_days')
    ax1.plot(data['num_days'], data['union_time_ms'], marker='o', linewidth=2,
             markersize=8, label=f'Precision {precision}')

ax1.set_xlabel('Number of Days (Sketches to Union)', fontweight='bold')
ax1.set_ylabel('Union Time (ms)', fontweight='bold')
ax1.set_title('Union Time vs Number of Sketches')
ax1.legend()
ax1.grid(True, alpha=0.3)

# Plot 4b: Exact re-aggregation scaling
ax2 = axes[0, 1]
exact_by_days = exact_df.groupby('num_days')['query_time_ms'].agg(['mean', 'std'])
days = sorted(exact_df['num_days'].unique())

ax2.plot(days, exact_by_days['mean'], marker='s', linewidth=2, 
         markersize=8, color='red', label='Exact COUNT(DISTINCT)')
ax2.fill_between(days, 
                 exact_by_days['mean'] - exact_by_days['std'],
                 exact_by_days['mean'] + exact_by_days['std'],
                 alpha=0.2, color='red')

ax2.set_xlabel('Number of Days (Data Volume)', fontweight='bold')
ax2.set_ylabel('Query Time (ms)', fontweight='bold')
ax2.set_title('Exact Re-aggregation Time vs Data Volume')
ax2.legend()
ax2.grid(True, alpha=0.3)

# Plot 4c: Time per sketch
ax3 = axes[1, 0]
for precision in precisions:
    data = comparison_df[comparison_df['precision'] == precision].sort_values('num_days')
    # Time per sketch is calculated here in Python
    time_per_sketch = data['union_time_ms'] / data['num_days']
    ax3.plot(data['num_days'], time_per_sketch, marker='o', linewidth=2,
             markersize=8, label=f'Precision {precision}')

ax3.set_xlabel('Number of Days', fontweight='bold')
ax3.set_ylabel('Time per Sketch (ms)', fontweight='bold')
ax3.set_title('Amortized Time per Sketch Union')
ax3.legend()
ax3.grid(True, alpha=0.3)

# Plot 4d: Efficiency score (speedup / error)
ax4 = axes[1, 1]
# Efficiency score is calculated in Phase 4 Pandas step
for precision in precisions:
    data = comparison_df[comparison_df['precision'] == precision].sort_values('num_days')
    ax4.plot(data['num_days'], data['efficiency_score'], marker='o', linewidth=2,
             markersize=8, label=f'Precision {precision}')

ax4.set_xlabel('Number of Days', fontweight='bold')
ax4.set_ylabel('Efficiency Score (Speedup/Error)', fontweight='bold')
ax4.set_title('Overall Efficiency: Higher is Better')
ax4.legend()
ax4.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(output_dir / 'scalability_analysis.png', dpi=300, bbox_inches='tight')
print(f"Saved: {output_dir / 'scalability_analysis.png'}")

# ============================================================================
# PLOT 5: Summary Heatmap
# ============================================================================
print("\nGenerating Plot 5: Summary Heatmap...")

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle('Performance Heatmap Summary', fontsize=16, fontweight='bold')

# Pivot data for heatmaps
speedup_pivot = comparison_df.pivot(index='precision', columns='num_days', values='speedup_factor')
error_pivot = comparison_df.pivot(index='precision', columns='num_days', values='error_pct')

# Plot 5a: Speedup heatmap
ax1 = axes[0]
sns.heatmap(speedup_pivot, annot=True, fmt='.1f', cmap='RdYlGn', ax=ax1,
            cbar_kws={'label': 'Speedup Factor (x)'}, linewidths=0.5)
ax1.set_xlabel('Time Window (days)', fontweight='bold')
ax1.set_ylabel('Precision', fontweight='bold')
ax1.set_title('Speedup Factor Heatmap')

# Plot 5b: Error heatmap
ax2 = axes[1]
sns.heatmap(error_pivot, annot=True, fmt='.2f', cmap='RdYlGn_r', ax=ax2,
            cbar_kws={'label': 'Error (%)'}, linewidths=0.5)
ax2.set_xlabel('Time Window (days)', fontweight='bold')
ax2.set_ylabel('Precision', fontweight='bold')
ax2.set_title('Estimation Error Heatmap')

plt.tight_layout()
plt.savefig(output_dir / 'summary_heatmap.png', dpi=300, bbox_inches='tight')
print(f"Saved: {output_dir / 'summary_heatmap.png'}")

# ============================================================================
# Generate Summary Statistics
# ============================================================================
print("\n" + "="*70)
print("BENCHMARK SUMMARY STATISTICS")
print("="*70)

print("\n📊 Overall Performance Metrics:")
print(f"  • Average Speedup: {comparison_df['speedup_factor'].mean():.1f}x")
print(f"  • Max Speedup: {comparison_df['speedup_factor'].max():.1f}x")
print(f"  • Min Speedup: {comparison_df['speedup_factor'].min():.1f}x")

print("\n🎯 Accuracy Metrics:")
print(f"  • Average Error: {comparison_df['error_pct'].mean():.3f}%")
print(f"  • Max Error: {comparison_df['error_pct'].max():.3f}%")
print(f"  • Min Error: {comparison_df['error_pct'].min():.3f}%")

print("\n⚡ Query Time Comparison:")
print(f"  • Avg HLL Union Time: {comparison_df['union_time_ms'].mean():.2f} ms")
print(f"  • Avg Exact Time: {comparison_df['exact_time_ms'].mean():.2f} ms")
print(f"  • Time Saved per Query: {(comparison_df['exact_time_ms'] - comparison_df['union_time_ms']).mean():.2f} ms")

print("\n💾 Storage Efficiency:")
for precision in precisions:
    data = comparison_df[comparison_df['precision'] == precision]
    print(f"  • Precision {precision}: {data['sketch_size_kb'].mean():.1f} KB avg")

print("\n✅ Best Configuration by Use Case:")
best_speed = comparison_df.loc[comparison_df['speedup_factor'].idxmax()]
best_accuracy = comparison_df.loc[comparison_df['error_pct'].idxmin()]
best_efficiency = comparison_df.loc[comparison_df['efficiency_score'].idxmax()]

print(f"  • Fastest: p={best_speed['precision']}, {best_speed['num_days']} days ({best_speed['speedup_factor']:.1f}x)")
print(f"  • Most Accurate: p={best_accuracy['precision']}, {best_accuracy['num_days']} days ({best_accuracy['error_pct']:.3f}%)")
print(f"  • Best Balance: p={best_efficiency['precision']}, {best_efficiency['num_days']} days (score: {best_efficiency['efficiency_score']:.1f})")

print("\n" + "="*70)
print(f"All plots saved to: {output_dir}")
print("="*70)

print("\n✨ Visualization complete!")