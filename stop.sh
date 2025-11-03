#!/usr/bin/env bash
set -euo pipefail

echo "=== Lab 2 Cleanup ==="
echo

if [ ! -f .env ]; then
  echo "No .env found. Cannot determine AWS region."
  echo "If you want to cleanup manually, use AWS Console."
  exit 1
fi

# Load environment
set -a; source .env; set +a

# Activate venv if exists
if [ -d .venv ]; then
  source .venv/bin/activate
else
  # Create minimal venv for cleanup
  python3 -m venv .venv
  source .venv/bin/activate
  pip install --upgrade pip > /dev/null
  pip install boto3 > /dev/null
fi

# Run teardown
python scripts/teardown.py

# Clean up local artifacts
echo
echo "Cleaning up local artifacts..."
rm -rf data/chunks data/mapper_outputs data/reducer_outputs
rm -f artifacts/*.json
echo "OK Local cleanup complete"

echo
echo "=========================================="
echo "All Lab 2 resources cleaned up! OK"
echo "=========================================="
echo
echo "Note: Security groups and key pairs are preserved for reuse."
echo "To remove them, delete manually in AWS Console."
