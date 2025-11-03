#!/usr/bin/env bash
set -euo pipefail

echo "=== Lab 2 Part 2: Friend Recommendation MapReduce ==="
echo

# Check .env exists
if [ ! -f .env ]; then
  echo "No .env found."
  echo "Run: ./scripts/bootstrap_env.sh && set -a; source .env; set +a"
  exit 1
fi

# Load environment
set -a; source .env; set +a

# Check data file exists
if [ ! -f data/soc-LiveJournal1Adj.txt ]; then
  echo "ERROR: Data file not found: data/soc-LiveJournal1Adj.txt"
  echo "Please download the file from Moodle and place it in data/"
  exit 1
fi

# Setup Python venv
echo "Setting up Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip > /dev/null
pip install boto3 > /dev/null
echo "OK Dependencies installed"
echo

# Step 1: Provision MapReduce instances
echo "Step 1: Provisioning mapper and reducer instances..."
python scripts/provision_mapreduce.py
echo

# Wait for SSH
echo "Waiting 30 seconds for instances to initialize..."
sleep 30
echo

# Step 2: Deploy mapper/reducer code
echo "Step 2: Deploying MapReduce code to instances..."
python scripts/deploy_mapreduce.py
echo

# Step 3: Run friend recommendation
echo "Step 3: Running distributed friend recommendation MapReduce..."
python scripts/run_friend_recommendation.py
echo

echo "=========================================="
echo "Part 2 Complete! OK"
echo "=========================================="
echo
echo "Results:"
echo "  - Full recommendations:   artifacts/friend_recommendations.txt"
echo "  - Report recommendations: artifacts/report_recommendations.txt"
echo
echo "View report recommendations:"
echo "  cat artifacts/report_recommendations.txt"
echo
echo "Remember to stop instances when done!"
echo "Run: ./stop.sh"
