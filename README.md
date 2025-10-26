# Lab 2: MapReduce on AWS

Advanced Concepts of Cloud Computing - 2025

## Overview

This lab implements MapReduce on AWS in two parts:
1. **Part 1**: WordCount benchmarking comparing Hadoop, Spark, and Linux bash
2. **Part 2**: Distributed friend recommendation system using MapReduce paradigm with separate mapper and reducer EC2 instances

## Project Structure

```
lab2/
├── app/
│   ├── mapper.py              # Friend recommendation mapper
│   └── reducer.py             # Friend recommendation reducer
├── scripts/
│   ├── bootstrap_env.sh       # AWS resource setup
│   ├── provision_wordcount.py # Part 1: Provision T2.large instance
│   ├── setup_hadoop_spark.py  # Part 1: Install Hadoop & Spark
│   ├── run_wordcount_benchmarks.py  # Part 1: Run benchmarks
│   ├── provision_mapreduce.py # Part 2: Provision mapper/reducer instances
│   ├── deploy_mapreduce.py    # Part 2: Deploy MapReduce code
│   ├── run_friend_recommendation.py # Part 2: Run distributed MapReduce
│   └── teardown.py            # Cleanup all instances
├── wordcount/
│   ├── hadoop_wordcount.sh    # Hadoop WordCount wrapper
│   ├── spark_wordcount.py     # Spark WordCount implementation
│   └── linux_wordcount.sh     # Bash WordCount implementation
├── plots/
│   └── generate_plots.py      # Generate benchmark visualizations
├── data/
│   └── soc-LiveJournal1Adj.txt  # Friend network data (download from Moodle)
├── artifacts/                 # Generated outputs
├── run_part1.sh              # Main script for Part 1
├── run_part2.sh              # Main script for Part 2
└── stop.sh                   # Cleanup script
```

## Prerequisites

- AWS CLI configured with credentials
- Python 3.8+
- Bash shell
- AWS account with EC2 permissions
- soc-LiveJournal1Adj.txt file (download from Moodle for Part 2)

## Setup

### 1. Bootstrap AWS Environment

This creates necessary AWS resources (VPC, Security Groups, Key Pair):

```bash
./scripts/bootstrap_env.sh
set -a; source .env; set +a
```

This will:
- Create or use default VPC
- Create security group `lab2-instances`
- Create SSH key pair
- Configure firewall rules for Hadoop/Spark ports
- Save configuration to `.env`

## Part 1: WordCount Benchmarking

### Overview

Compares performance of Hadoop, Spark, and Linux bash on 9 text datasets.

### Run Part 1

```bash
./run_part1.sh
```

This automated script will:
1. Provision a T2.large EC2 instance
2. Install Hadoop 3.3.6 and Spark 3.5.0
3. Download 9 datasets
4. Run WordCount 3 times per dataset per method (27 runs per method)
5. Generate performance plots

**Estimated time**: 60-90 minutes
**Estimated cost**: ~$2-3

### Manual Steps (Optional)

If you prefer to run steps individually:

```bash
# Load environment
set -a; source .env; set +a

# Activate virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install boto3 matplotlib numpy

# 1. Provision instance
python scripts/provision_wordcount.py

# 2. Wait for SSH (30 seconds)
sleep 30

# 3. Install Hadoop and Spark
python scripts/setup_hadoop_spark.py

# 4. Run benchmarks
python scripts/run_wordcount_benchmarks.py

# 5. Generate plots
python plots/generate_plots.py
```

### Results

After completion, results are in `artifacts/`:
- `benchmark_results.json` - Raw timing data
- `summary_statistics.json` - Statistical summary
- `plot_method_comparison.png` - Bar chart comparing methods
- `plot_dataset_comparison.png` - Performance by dataset
- `plot_distribution.png` - Box plots of execution times

### Accessing Hadoop/Spark Web UIs

Get the instance IP from `artifacts/wordcount_instance.json`, then:
- Hadoop NameNode: http://INSTANCE_IP:9870
- YARN ResourceManager: http://INSTANCE_IP:8088

## Part 2: Friend Recommendation MapReduce

### Overview

Implements a distributed "People You Might Know" recommendation system using the MapReduce paradigm with **separate EC2 instances for mappers and reducers**.

### Architecture

- **3 Mapper Instances** (T2.micro): Process chunks of the social network graph
- **2 Reducer Instances** (T2.micro): Aggregate results and generate recommendations
- **Distributed Execution**: Mappers and reducers run on different EC2 instances, communicating via SSH/SCP

### Data Preparation

Download `soc-LiveJournal1Adj.txt` from Moodle and place in `data/`:

```bash
# Place the file in data/
mv ~/Downloads/soc-LiveJournal1Adj.txt data/
```

### Run Part 2

```bash
./run_part2.sh
```

This automated script will:
1. Provision 3 mapper and 2 reducer EC2 instances
2. Deploy mapper.py and reducer.py to respective instances
3. Split input data into 3 chunks
4. Execute mappers in parallel on separate instances
5. Collect mapper outputs
6. Execute reducers on separate instances
7. Generate friend recommendations for report users

**Estimated time**: 15-30 minutes
**Estimated cost**: ~$0.50-1

### Manual Steps (Optional)

```bash
# Load environment
set -a; source .env; set +a

# Activate virtual environment
source .venv/bin/activate

# 1. Provision mapper and reducer instances
python scripts/provision_mapreduce.py

# 2. Wait for SSH
sleep 30

# 3. Deploy MapReduce code
python scripts/deploy_mapreduce.py

# 4. Run distributed MapReduce
python scripts/run_friend_recommendation.py
```

### Algorithm

**Mapper:**
```
For each user U with friends [F1, F2, ..., Fn]:
  1. Emit (U, Fi) → -1 for all Fi (mark existing friendships)
  2. For each pair (Fi, Fj) where i < j:
     - Emit (Fi, Fj) → U (U is mutual friend)
```

**Reducer:**
```
For each user pair (A, B):
  1. If -1 in values: skip (already friends)
  2. Else: count mutual friends
  3. Aggregate recommendations per user
  4. Sort by mutual friend count (desc), then user ID (asc)
  5. Output top 10 recommendations per user
```

### Results

After completion:
- `artifacts/friend_recommendations.txt` - Full recommendations for all users
- `artifacts/report_recommendations.txt` - Recommendations for users: 924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993

View report recommendations:
```bash
cat artifacts/report_recommendations.txt
```

## Cleanup

**IMPORTANT**: Always stop instances when done to avoid charges!

```bash
./stop.sh
```

This will:
- Terminate all Lab 2 EC2 instances
- Clean up local temporary files
- Preserve `.env` and artifacts for report

## Cost Management

- **Part 1**: T2.large @ ~$0.0928/hour × ~1.5 hours = ~$0.14 + data transfer
- **Part 2**: 5× T2.micro @ ~$0.0116/hour × ~0.5 hours = ~$0.03 + data transfer
- **Total estimated**: $3-5 for complete assignment

**Cost-saving tips:**
1. Run Part 1 and Part 2 separately
2. Stop instances immediately after each part
3. Use `./stop.sh` to ensure all instances are terminated
4. Monitor AWS billing dashboard

## Troubleshooting

### SSH connection issues
```bash
# Check instance state
aws ec2 describe-instances --filters "Name=tag:Lab,Values=lab2" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name,PublicIpAddress]'

# Wait longer for SSH to become available
sleep 60
```

### Hadoop/Spark installation fails
- Check instance has internet access
- Verify security group allows outbound traffic
- SSH manually and check `/var/log/cloud-init-output.log`

### MapReduce data file missing
```bash
# Ensure file is in correct location
ls -lh data/soc-LiveJournal1Adj.txt

# If missing, download from Moodle and place in data/
```

### Out of memory errors
- Increase instance types in provision scripts
- Reduce dataset size for testing
- Process fewer chunks

## Testing

### Test Part 1 with small dataset
```bash
# Create small test file
echo "hello world hello spark hadoop spark" > data/datasets/test.txt

# Test manually on instance
ssh -i $AWS_KEY_PATH ubuntu@INSTANCE_IP
~/wordcount/linux_wordcount.sh ~/datasets/test.txt /tmp/test_output.txt
```

### Test Part 2 with small network
```bash
# Create small test file
cat > data/test_network.txt << EOF
1	2,3,4
2	1,3,5
3	1,2,4
4	1,3,5
5	2,4
EOF

# Modify run_friend_recommendation.py to use test_network.txt
# Then run Part 2
```

## Report Deliverables

For your lab report, include:

### Part 1
- Performance comparison tables (Hadoop vs Spark vs Linux)
- Plots from `artifacts/plot_*.png`
- Analysis of which method performs best and why
- Discussion of scaling characteristics

### Part 2
- Friend recommendations for users: 924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993
- MapReduce algorithm description (mapper and reducer logic)
- Architecture diagram showing distributed execution across instances
- Explanation of why mappers/reducers are on separate instances

## Architecture Highlights

### Part 1: Single-Node Big Data Processing
- 1× T2.large instance
- Hadoop 3.3.6 (HDFS + YARN)
- Spark 3.5.0
- Benchmark harness with timing measurements

### Part 2: Distributed MapReduce
- **Distributed Architecture**: Mappers and reducers on separate EC2 instances (as required)
- **Data Flow**:
  1. Coordinator splits data → sends to mappers
  2. Mappers process in parallel on separate instances
  3. Coordinator collects mapper outputs
  4. Coordinator distributes to reducers
  5. Reducers aggregate on separate instances
  6. Coordinator retrieves final results
- **Communication**: SSH/SCP for inter-instance data transfer
- **Scalability**: Configurable number of mappers and reducers

## References

- [Hadoop MapReduce Tutorial](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [AWS EC2 User Guide](https://docs.aws.amazon.com/ec2/)

## License

Educational use only - Polytechnique Montreal, Fall 2025
