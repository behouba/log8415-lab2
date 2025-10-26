#!/usr/bin/env bash
set -euo pipefail

echo "=== Lab 2 Part 1: WordCount Benchmarking ==="
echo

# Check .env exists
if [ ! -f .env ]; then
  echo "No .env found."
  echo "Run: ./scripts/bootstrap_env.sh && set -a; source .env; set +a"
  exit 1
fi

# Load environment
set -a; source .env; set +a

# Setup Python venv
echo "Setting up Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip > /dev/null
pip install boto3 matplotlib numpy > /dev/null
echo "✅ Dependencies installed"
echo

# Step 1: Provision instance
echo "Step 1: Provisioning T2.large instance..."
python scripts/provision_wordcount.py
echo

# Wait for SSH to be ready
echo "Waiting 30 seconds for instance initialization..."
sleep 30
echo

# Step 2: Setup Hadoop and Spark
echo "Step 2: Installing Hadoop and Spark (this will take ~10 minutes)..."
python scripts/setup_hadoop_spark.py
echo

# Step 3: Run benchmarks
echo "Step 3: Running WordCount benchmarks (this will take ~30-60 minutes)..."
python scripts/run_wordcount_benchmarks.py
echo

# Step 4: Generate plots
echo "Step 4: Generating plots..."
python plots/generate_plots.py
echo

echo "=========================================="
echo "Part 1 Complete! ✅"
echo "=========================================="
echo
echo "Results:"
echo "  - Benchmark data:  artifacts/benchmark_results.json"
echo "  - Summary stats:   artifacts/summary_statistics.json"
echo "  - Plots:           artifacts/plot_*.png"
echo
echo "View plots:"
echo "  - artifacts/plot_method_comparison.png"
echo "  - artifacts/plot_dataset_comparison.png"
echo "  - artifacts/plot_distribution.png"
echo
echo "Remember to stop the instance when done!"
echo "Run: ./stop.sh"
