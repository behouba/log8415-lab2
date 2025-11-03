# Lab 2: MapReduce on AWS

## Setup

```bash
./scripts/bootstrap_env.sh
set -a; source .env; set +a
```

## Part 1: WordCount Benchmarking

Compares Hadoop, Spark, and Linux bash on 9 datasets.

```bash
./run_part1.sh
```

Results in `artifacts/`:
- `benchmark_results.json`
- `plot_*.png`


## Part 2: Friend Recommendation

Distributed MapReduce with 3 mappers and 6 reducers on separate EC2 instances.

**Prerequisites**: Download `soc-LiveJournal1Adj.txt` from Moodle to `data/`

```bash
./run_part2.sh
```

Results: `artifacts/report_recommendations.txt`


### Algorithm

**Mapper**: For each user and their friends, emit (user, friend) -> -1 to mark existing friendships, and emit (friend_a, friend_b) -> user to indicate mutual friends.

**Reducer**: Group by user pairs, count mutual friends (ignore pairs with -1), sort by count descending, output top 10 per user.

## Cleanup

```bash
./stop.sh
```

## Manual Steps

If automation fails, run scripts individually:

```bash
# Part 1
python scripts/provision_wordcount.py
sleep 30
python scripts/setup_hadoop_spark.py
python scripts/run_wordcount_benchmarks.py
python plots/generate_plots.py

# Part 2
python scripts/provision_mapreduce.py
sleep 30
python scripts/deploy_mapreduce.py
python scripts/run_friend_recommendation.py
```

## Structure

```
scripts/          # AWS provisioning and deployment
app/              # Mapper and reducer
wordcount/        # WordCount implementations
plots/            # Visualization
artifacts/        # Benchmark results and generated plots
report/           # LaTeX source and compiled PDF
run_part1.sh      # Part 1 automation
run_part2.sh      # Part 2 automation
stop.sh           # Cleanup
```
