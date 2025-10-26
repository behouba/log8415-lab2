#!/usr/bin/env python3
"""
Orchestrate distributed MapReduce for friend recommendation

Workflow:
1. Split input data into chunks for mappers
2. Upload chunks to mapper instances
3. Execute mapper on each instance (parallel)
4. Download mapper outputs
5. Upload mapper outputs to reducer instances
6. Execute reducer
7. Download final results
8. Extract recommendations for specific users
"""
import json, os, sys, subprocess
import shutil

KEY_PATH = os.getenv("AWS_KEY_PATH")
if not KEY_PATH:
    sys.exit("Missing AWS_KEY_PATH. Run: set -a; source .env; set +a")

# Check if data file exists
DATA_FILE = "data/soc-LiveJournal1Adj.txt"
if not os.path.exists(DATA_FILE):
    print(f"ERROR: Data file not found: {DATA_FILE}")
    print("Please download the file from Moodle and place it in data/")
    sys.exit(1)

with open("artifacts/mapreduce_instances.json") as f:
    instances = json.load(f)

SSH_USER = "ubuntu"
SSH_BASE = [
    "ssh",
    "-o", "StrictHostKeyChecking=no",
    "-o", "BatchMode=yes",
    "-o", "ServerAliveInterval=15",
    "-o", "ConnectTimeout=20",
]

SCP_BASE = ["scp", "-o", "StrictHostKeyChecking=no"]

def ssh(host, cmd):
    """Execute command on remote host"""
    remote = f"bash -lc '{cmd}'"
    result = subprocess.run(
        SSH_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{host}", remote],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    return result

def scp_upload(host, local_path, remote_path):
    """Upload file to remote host"""
    result = subprocess.run(
        SCP_BASE + ["-i", KEY_PATH, local_path, f"{SSH_USER}@{host}:{remote_path}"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    return result

def scp_download(host, remote_path, local_path):
    """Download file from remote host"""
    result = subprocess.run(
        SCP_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{host}:{remote_path}", local_path],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    return result

print("=== Friend Recommendation MapReduce ===\n")

# Step 1: Split input data
print("Step 1: Splitting input data into chunks for mappers...")
num_mappers = len(instances["mappers"])
print(f"  Number of mappers: {num_mappers}")

# Count total lines
with open(DATA_FILE, 'r') as f:
    total_lines = sum(1 for _ in f)
print(f"  Total lines in input: {total_lines}")

lines_per_chunk = total_lines // num_mappers + 1
print(f"  Lines per chunk: ~{lines_per_chunk}")

# Split file into chunks
os.makedirs("data/chunks", exist_ok=True)
chunk_files = []

with open(DATA_FILE, 'r') as infile:
    for i in range(num_mappers):
        chunk_file = f"data/chunks/chunk_{i}.txt"
        chunk_files.append(chunk_file)

        with open(chunk_file, 'w') as outfile:
            for j in range(lines_per_chunk):
                line = infile.readline()
                if not line:
                    break
                outfile.write(line)

        print(f"  Created {chunk_file}")

print(f"✅ Split into {len(chunk_files)} chunks\n")

# Step 2: Upload chunks to mapper instances and run mappers
print("Step 2: Distributing chunks to mappers and executing...")
mapper_outputs = []

for i, mapper in enumerate(instances["mappers"]):
    host = mapper["public_ip"]
    chunk_file = chunk_files[i]
    remote_chunk = f"~/data/chunk_{i}.txt"
    remote_output = f"~/data/mapper_output_{i}.txt"

    print(f"\n  Mapper {i+1}/{num_mappers} ({host}):")
    print(f"    Uploading {chunk_file}...")
    result = scp_upload(host, chunk_file, remote_chunk)
    if result.returncode != 0:
        print(f"    ERROR uploading: {result.stderr}")
        sys.exit(1)

    print(f"    Running mapper...")
    result = ssh(host, f"python3 ~/mapreduce/mapper.py {remote_chunk} {remote_output}")
    if result.returncode != 0:
        print(f"    ERROR running mapper: {result.stderr}")
        sys.exit(1)

    print(f"    ✅ Mapper completed")
    mapper_outputs.append((host, remote_output, f"mapper_output_{i}.txt"))

print(f"\n✅ All {num_mappers} mappers completed\n")

# Step 3: Download mapper outputs
print("Step 3: Collecting mapper outputs...")
os.makedirs("data/mapper_outputs", exist_ok=True)
local_mapper_outputs = []

for host, remote_path, filename in mapper_outputs:
    local_path = f"data/mapper_outputs/{filename}"
    print(f"  Downloading from {host}...")
    result = scp_download(host, remote_path, local_path)
    if result.returncode != 0:
        print(f"    ERROR downloading: {result.stderr}")
        sys.exit(1)
    local_mapper_outputs.append(local_path)

print(f"✅ Downloaded {len(local_mapper_outputs)} mapper outputs\n")

# Step 4: Distribute mapper outputs to reducers and run
print("Step 4: Running reducers...")
num_reducers = len(instances["reducers"])

# For simplicity, we'll use the first reducer (or distribute across all)
# Here we'll send all mapper outputs to each reducer
reducer_results = []

for idx, reducer in enumerate(instances["reducers"]):
    host = reducer["public_ip"]
    print(f"\n  Reducer {idx+1}/{num_reducers} ({host}):")

    # Upload all mapper outputs to this reducer
    remote_mapper_files = []
    for i, local_output in enumerate(local_mapper_outputs):
        remote_path = f"~/data/mapper_input_{i}.txt"
        print(f"    Uploading mapper output {i+1}...")
        result = scp_upload(host, local_output, remote_path)
        if result.returncode != 0:
            print(f"    ERROR uploading: {result.stderr}")
            sys.exit(1)
        remote_mapper_files.append(remote_path)

    # Run reducer
    remote_output = f"~/data/reducer_output_{idx}.txt"
    reducer_cmd = f"python3 ~/mapreduce/reducer.py {' '.join(remote_mapper_files)} {remote_output}"
    print(f"    Running reducer...")
    result = ssh(host, reducer_cmd)
    if result.returncode != 0:
        print(f"    ERROR running reducer: {result.stderr}")
        sys.exit(1)

    print(f"    ✅ Reducer completed")
    reducer_results.append((host, remote_output, f"reducer_output_{idx}.txt"))

print(f"\n✅ All {num_reducers} reducers completed\n")

# Step 5: Download reducer outputs
print("Step 5: Collecting reducer outputs...")
os.makedirs("data/reducer_outputs", exist_ok=True)

for host, remote_path, filename in reducer_results:
    local_path = f"data/reducer_outputs/{filename}"
    print(f"  Downloading from {host}...")
    result = scp_download(host, remote_path, local_path)
    if result.returncode != 0:
        print(f"    ERROR downloading: {result.stderr}")
        sys.exit(1)

    # Use the first reducer output as final result
    final_output = "artifacts/friend_recommendations.txt"
    shutil.copy(local_path, final_output)
    print(f"  Saved to {final_output}")
    break  # Only need one reducer output

print(f"\n✅ Reducer output downloaded\n")

# Step 6: Extract recommendations for specific users
print("Step 6: Extracting recommendations for report users...")
REPORT_USERS = ["924", "8941", "8942", "9019", "9020", "9021", "9022", "9990", "9992", "9993"]

recommendations = {}
with open(final_output, 'r') as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            user_id = parts[0]
            recs = parts[1]
            recommendations[user_id] = recs

print("\n=== Friend Recommendations for Report Users ===\n")
report_output = "artifacts/report_recommendations.txt"
with open(report_output, 'w') as f:
    for user_id in REPORT_USERS:
        if user_id in recommendations:
            recs = recommendations[user_id]
            print(f"User {user_id}: {recs}")
            f.write(f"User {user_id}: {recs}\n")
        else:
            print(f"User {user_id}: No recommendations found")
            f.write(f"User {user_id}: No recommendations found\n")

print(f"\n✅ Saved report recommendations to {report_output}")

print("\n" + "="*50)
print("Friend Recommendation MapReduce Complete! ✅")
print("="*50)
print(f"\nResults:")
print(f"  - Full recommendations:   {final_output}")
print(f"  - Report recommendations: {report_output}")
print(f"\nMapper instances:  {num_mappers}")
print(f"Reducer instances: {num_reducers}")
