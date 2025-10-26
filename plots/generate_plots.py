#!/usr/bin/env python3
"""
Generate plots from WordCount benchmark results
"""
import json
import sys
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    print("ERROR: matplotlib not installed")
    print("Run: pip install matplotlib numpy")
    sys.exit(1)

# Load results
with open("artifacts/benchmark_results.json") as f:
    results = json.load(f)

# Filter successful runs
successful_results = [r for r in results if r["success"]]

# Group by method and dataset
method_times = defaultdict(list)
dataset_times = defaultdict(lambda: defaultdict(list))

for result in successful_results:
    method = result["method"]
    dataset = result["dataset"]
    time_sec = result["execution_time_seconds"]

    method_times[method].append(time_sec)
    dataset_times[dataset][method].append(time_sec)

# Calculate averages
method_avg = {method: np.mean(times) for method, times in method_times.items()}
method_std = {method: np.std(times) for method, times in method_times.items()}

# Plot 1: Average execution time by method
print("Generating Plot 1: Average execution time by method...")
fig, ax = plt.subplots(figsize=(10, 6))
methods = sorted(method_avg.keys())
avg_times = [method_avg[m] for m in methods]
std_times = [method_std[m] for m in methods]

bars = ax.bar(methods, avg_times, yerr=std_times, capsize=5, alpha=0.7,
               color=['#1f77b4', '#ff7f0e', '#2ca02c'])
ax.set_ylabel('Average Execution Time (seconds)')
ax.set_title('WordCount Performance Comparison')
ax.grid(axis='y', alpha=0.3)

# Add value labels on bars
for bar, avg in zip(bars, avg_times):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{avg:.2f}s', ha='center', va='bottom')

plt.tight_layout()
plt.savefig('artifacts/plot_method_comparison.png', dpi=150)
print("  Saved: artifacts/plot_method_comparison.png")

# Plot 2: Execution time per dataset for each method
print("Generating Plot 2: Execution time per dataset...")
datasets = sorted(dataset_times.keys())
methods_list = ["hadoop", "spark", "linux"]

fig, ax = plt.subplots(figsize=(14, 6))
x = np.arange(len(datasets))
width = 0.25

for i, method in enumerate(methods_list):
    times = [np.mean(dataset_times[ds].get(method, [0])) for ds in datasets]
    offset = (i - 1) * width
    ax.bar(x + offset, times, width, label=method.capitalize(), alpha=0.7)

ax.set_xlabel('Dataset')
ax.set_ylabel('Average Execution Time (seconds)')
ax.set_title('WordCount Performance by Dataset')
ax.set_xticks(x)
ax.set_xticklabels(datasets, rotation=45, ha='right')
ax.legend()
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('artifacts/plot_dataset_comparison.png', dpi=150)
print("  Saved: artifacts/plot_dataset_comparison.png")

# Plot 3: Box plot showing distribution
print("Generating Plot 3: Distribution of execution times...")
fig, ax = plt.subplots(figsize=(10, 6))

data_to_plot = [method_times[m] for m in methods]
bp = ax.boxplot(data_to_plot, labels=methods, patch_artist=True)

# Color the boxes
colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)

ax.set_ylabel('Execution Time (seconds)')
ax.set_title('Distribution of WordCount Execution Times')
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('artifacts/plot_distribution.png', dpi=150)
print("  Saved: artifacts/plot_distribution.png")

# Generate summary statistics table
print("\n=== Summary Statistics ===")
print(f"{'Method':<10} {'Mean':<10} {'Median':<10} {'Std Dev':<10} {'Min':<10} {'Max':<10}")
print("-" * 60)

summary_stats = {}
for method in sorted(method_times.keys()):
    times = method_times[method]
    stats = {
        'mean': np.mean(times),
        'median': np.median(times),
        'std': np.std(times),
        'min': np.min(times),
        'max': np.max(times)
    }
    summary_stats[method] = stats

    print(f"{method:<10} {stats['mean']:<10.2f} {stats['median']:<10.2f} "
          f"{stats['std']:<10.2f} {stats['min']:<10.2f} {stats['max']:<10.2f}")

# Save summary statistics
with open("artifacts/summary_statistics.json", "w") as f:
    json.dump(summary_stats, f, indent=2)

print("\n✅ All plots generated successfully!")
print("\nGenerated files:")
print("  - artifacts/plot_method_comparison.png")
print("  - artifacts/plot_dataset_comparison.png")
print("  - artifacts/plot_distribution.png")
print("  - artifacts/summary_statistics.json")
