#!/bin/bash
# Linux WordCount using bash commands with timing
# Usage: ./linux_wordcount.sh <input_file> <output_file>

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <input_file> <output_file>"
    exit 1
fi

INPUT_FILE=$1
OUTPUT_FILE=$2

# Start timing
START_TIME=$(date +%s.%N)

# WordCount using bash commands: cat | tr | sort | uniq
cat "$INPUT_FILE" | tr ' ' '\n' | sort | uniq -c | awk '{print $2, $1}' > "$OUTPUT_FILE"

# End timing
END_TIME=$(date +%s.%N)

# Calculate and output execution time
EXECUTION_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo "EXECUTION_TIME: $EXECUTION_TIME"
echo "WordCount complete. Output in $OUTPUT_FILE"
