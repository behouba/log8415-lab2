#!/usr/bin/env python3
import json, os, sys, subprocess, time
import urllib.request
import shutil

KEY_PATH = os.getenv("AWS_KEY_PATH")
if not KEY_PATH:
    sys.exit("Missing AWS_KEY_PATH. Run: set -a; source .env; set +a")

with open("artifacts/wordcount_instance.json") as f:
    instance = json.load(f)

HOST = instance["public_ip"]
SSH_USER = "ubuntu"

# Dataset URLs from the PDF
DATASETS = [
    "https://tinyurl.com/4vxdw3pa",
    "https://tinyurl.com/kh9excea",
    "https://tinyurl.com/dybs9bnk",
    "https://tinyurl.com/datumz6m",
    "https://tinyurl.com/j4j4xdw6",
    "https://tinyurl.com/ym8s5fm4",
    "https://tinyurl.com/2h6a75nk",
    "https://tinyurl.com/vwvram8",
    "https://tinyurl.com/weh83uyn",
]

SSH_BASE = [
    "ssh",
    "-o", "StrictHostKeyChecking=no",
    "-o", "BatchMode=yes",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=3",
    "-o", "ConnectTimeout=20",
]

def ssh(cmd):
    """Execute command on remote host"""
    remote = f"bash -lc '{cmd}'"
    result = subprocess.run(
        SSH_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{HOST}", remote],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    return result

def scp_upload(local_path, remote_path):
    result = subprocess.run(
        ["scp", "-o", "StrictHostKeyChecking=no", "-i", KEY_PATH,
         local_path, f"{SSH_USER}@{HOST}:{remote_path}"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    return result

print("=== WordCount Benchmarking Suite ===\n")

# Step 1: Download datasets
print("Step 1: Downloading datasets...")
os.makedirs("data/datasets", exist_ok=True)
dataset_files = []

for idx, url in enumerate(DATASETS):
    dataset_name = f"dataset_{idx+1}.txt"
    local_path = f"data/datasets/{dataset_name}"

    if os.path.exists(local_path):
        print(f"  [{idx+1}/9] {dataset_name} already exists, skipping download")
    else:
        print(f"  [{idx+1}/9] Downloading {url} -> {dataset_name}")
        try:
            with urllib.request.urlopen(url) as response:
                with open(local_path, 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)

            # Get file size
            size_mb = os.path.getsize(local_path) / (1024 * 1024)
            print(f"       Downloaded {size_mb:.2f} MB")
        except Exception as e:
            print(f"       ERROR: {e}")
            continue

    dataset_files.append((dataset_name, local_path))

print(f"\n✅ {len(dataset_files)} datasets ready\n")

# Step 2: Upload wordcount scripts to remote
print("Step 2: Uploading WordCount scripts to instance...")
ssh("mkdir -p ~/wordcount")
scp_upload("wordcount/hadoop_wordcount.sh", "~/wordcount/")
scp_upload("wordcount/spark_wordcount.py", "~/wordcount/")
scp_upload("wordcount/linux_wordcount.sh", "~/wordcount/")
ssh("chmod +x ~/wordcount/*.sh")
print("✅ Scripts uploaded\n")

# Step 3: Upload datasets to remote
print("Step 3: Uploading datasets to instance...")
ssh("mkdir -p ~/datasets")
for dataset_name, local_path in dataset_files:
    print(f"  Uploading {dataset_name}...")
    scp_upload(local_path, f"~/datasets/{dataset_name}")
print("✅ Datasets uploaded\n")

# Step 4: Run benchmarks
print("Step 4: Running benchmarks (3 iterations per dataset per method)...\n")
results = []

methods = [
    ("hadoop", "~/wordcount/hadoop_wordcount.sh ~/datasets/{dataset} /output/hadoop_{dataset}"),
    ("spark", "source ~/.bashrc && ~/spark/bin/spark-submit ~/wordcount/spark_wordcount.py ~/datasets/{dataset} /tmp/spark_output_{dataset}"),
    ("linux", "~/wordcount/linux_wordcount.sh ~/datasets/{dataset} /tmp/linux_output_{dataset}.txt"),
]

total_runs = len(dataset_files) * len(methods) * 3
current_run = 0

for dataset_name, _ in dataset_files:
    for method_name, cmd_template in methods:
        for iteration in range(1, 4):
            current_run += 1
            print(f"[{current_run}/{total_runs}] {dataset_name} | {method_name} | iteration {iteration}")

            cmd = cmd_template.format(dataset=dataset_name)

            # Measure execution time
            start_time = time.time()
            result = ssh(cmd)
            end_time = time.time()

            elapsed_time = end_time - start_time
            success = result.returncode == 0

            result_entry = {
                "dataset": dataset_name,
                "method": method_name,
                "iteration": iteration,
                "execution_time_seconds": elapsed_time,
                "success": success,
            }

            results.append(result_entry)

            status = "✓" if success else "✗"
            print(f"  {status} Time: {elapsed_time:.2f}s\n")
            if method_name == "hadoop":
                ssh(f"source ~/.bashrc && ~/hadoop/bin/hdfs dfs -rm -r -f /output/hadoop_{dataset_name} || true")
            elif method_name == "spark":
                ssh(f"rm -rf /tmp/spark_output_{dataset_name} || true")

# Save results
print("\n=== Saving Results ===")
output_file = "artifacts/benchmark_results.json"
with open(output_file, "w") as f:
    json.dump(results, f, indent=2)

print(f"✅ Results saved to {output_file}")

print("\n=== Summary ===")
successful_runs = sum(1 for r in results if r["success"])
print(f"Total runs: {len(results)}")
print(f"Successful: {successful_runs}")
print(f"Failed: {len(results) - successful_runs}")
for method_name, _ in methods:
    method_results = [r for r in results if r["method"] == method_name and r["success"]]
    if method_results:
        avg_time = sum(r["execution_time_seconds"] for r in method_results) / len(method_results)
        print(f"\n{method_name.upper()}: {avg_time:.2f}s avg ({len(method_results)} runs)")
