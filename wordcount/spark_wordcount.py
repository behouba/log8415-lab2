#!/usr/bin/env python3
"""
Spark WordCount implementation
Usage: spark-submit spark_wordcount.py <input_file> <output_dir>
"""
import sys
from pyspark import SparkContext, SparkConf

if len(sys.argv) != 3:
    print("Usage: spark_wordcount.py <input_file> <output_dir>", file=sys.stderr)
    sys.exit(1)

input_file = sys.argv[1]
output_dir = sys.argv[2]

# Create Spark context
conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

try:
    # Read input file
    text_file = sc.textFile(input_file)

    # WordCount: split -> map to (word, 1) -> reduce by key
    counts = text_file.flatMap(lambda line: line.split()) \
                      .map(lambda word: (word, 1)) \
                      .reduceByKey(lambda a, b: a + b)

    # Save output
    counts.saveAsTextFile(output_dir)

    print(f"WordCount complete. Output in {output_dir}")
finally:
    sc.stop()
