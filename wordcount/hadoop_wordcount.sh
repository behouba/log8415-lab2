#!/bin/bash
# Hadoop WordCount wrapper script with timing
# Usage: ./hadoop_wordcount.sh <input_file> <output_dir>

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <input_file> <output_dir>"
    exit 1
fi

INPUT_FILE=$1
OUTPUT_DIR=$2

# Verify HADOOP_HOME is set
if [ -z "${HADOOP_HOME:-}" ]; then
    echo "ERROR: HADOOP_HOME is not set" >&2
    exit 1
fi

# Ensure /input directory exists in HDFS
$HADOOP_HOME/bin/hdfs dfs -test -d /input || $HADOOP_HOME/bin/hdfs dfs -mkdir -p /input

# Clean up previous output
$HADOOP_HOME/bin/hdfs dfs -rm -r -f "$OUTPUT_DIR" || true

# Copy input to HDFS if not already there
INPUT_HDFS="/input/$(basename $INPUT_FILE)"
$HADOOP_HOME/bin/hdfs dfs -rm -f "$INPUT_HDFS" || true
$HADOOP_HOME/bin/hdfs dfs -put "$INPUT_FILE" "$INPUT_HDFS"

# Start timing - only measure the actual MapReduce job
START_TIME=$(date +%s.%N)

# Run Hadoop WordCount
$HADOOP_HOME/bin/hadoop jar \
    $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    wordcount \
    "$INPUT_HDFS" \
    "$OUTPUT_DIR"

# End timing
END_TIME=$(date +%s.%N)

# Get the output file
$HADOOP_HOME/bin/hdfs dfs -cat "$OUTPUT_DIR/part-r-00000" > /tmp/hadoop_wordcount_output.txt || true

# Calculate and output execution time
EXECUTION_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo "EXECUTION_TIME: $EXECUTION_TIME"
echo "WordCount complete. Output in $OUTPUT_DIR"
