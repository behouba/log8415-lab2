#!/usr/bin/env python3
import sys
from pyspark import SparkContext, SparkConf

if len(sys.argv) != 3:
    print("Usage: spark_wordcount.py <input_file> <output_dir>", file=sys.stderr)
    sys.exit(1)

input_file = sys.argv[1]
output_dir = sys.argv[2]

conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

try:
    text_file = sc.textFile(input_file)
    counts = text_file.flatMap(lambda line: line.split()) \
                      .map(lambda word: (word, 1)) \
                      .reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile(output_dir)
    print(f"WordCount complete. Output in {output_dir}")
finally:
    sc.stop()
