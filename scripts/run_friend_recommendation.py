#!/usr/bin/env python3
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections import defaultdict

KEY_PATH = os.getenv("AWS_KEY_PATH")
if not KEY_PATH:
    sys.exit("Missing AWS_KEY_PATH. Run: set -a; source .env; set +a")

DATA_FILE = "data/soc-LiveJournal1Adj.txt"
if not os.path.exists(DATA_FILE):
    print(f"ERROR: Data file not found: {DATA_FILE}")
    print("Please download the file from Moodle and place it in data/")
    sys.exit(1)

ARTIFACTS_DIR = "artifacts"
os.makedirs(ARTIFACTS_DIR, exist_ok=True)

with open(os.path.join(ARTIFACTS_DIR, "mapreduce_instances.json")) as f:
    instances = json.load(f)

SSH_USER = "ubuntu"
SSH_BASE = [
    "ssh",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "BatchMode=yes",
    "-o",
    "ServerAliveInterval=15",
    "-o",
    "ServerAliveCountMax=60",
    "-o",
    "ConnectionAttempts=10",
    "-o",
    "ConnectTimeout=20",
]

SCP_BASE = [
    "scp",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "ServerAliveInterval=15",
    "-o",
    "ServerAliveCountMax=60",
    "-o",
    "ConnectTimeout=20",
]


def ssh(host, cmd, stream_output=False, label=None):
    remote = f'bash -lc "{cmd}"'
    if stream_output:
        process = subprocess.Popen(
            SSH_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{host}", remote],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        collected = []
        prefix = f"[{label}] " if label else ""
        try:
            if process.stdout:
                for line in process.stdout:
                    collected.append(line)
                    print(f"{prefix}{line}", end="")
        finally:
            process.wait()
        return subprocess.CompletedProcess(
            args=process.args,
            returncode=process.returncode,
            stdout="".join(collected),
            stderr="",
        )
    result = subprocess.run(
        SSH_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{host}", remote],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result


def scp_upload(host, local_path, remote_path):
    result = subprocess.run(
        SCP_BASE + ["-i", KEY_PATH, local_path, f"{SSH_USER}@{host}:{remote_path}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result


def scp_download(host, remote_path, local_path):
    result = subprocess.run(
        SCP_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{host}:{remote_path}", local_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result


def sort_user_key(user_id):
    return int(user_id) if user_id.isdigit() else user_id


def shard_for_pair(pair_key, num_reducers):
    digest = hashlib.md5(pair_key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % num_reducers


print("=== Friend Recommendation MapReduce ===\n")

print("Step 1: Splitting input data into chunks for mappers...")
num_mappers = len(instances["mappers"])
print(f"  Number of mappers: {num_mappers}")

with open(DATA_FILE, "r") as f:
    total_lines = sum(1 for _ in f)
print(f"  Total lines in input: {total_lines}")

lines_per_chunk = total_lines // num_mappers + 1
print(f"  Lines per chunk: ~{lines_per_chunk}")

all_users = set()

shutil.rmtree("data/chunks", ignore_errors=True)
os.makedirs("data/chunks", exist_ok=True)
chunk_files = []

with open(DATA_FILE, "r") as infile:
    for i in range(num_mappers):
        chunk_file = f"data/chunks/chunk_{i}.txt"
        chunk_files.append(chunk_file)

        with open(chunk_file, "w") as outfile:
            for _ in range(lines_per_chunk):
                line = infile.readline()
                if not line:
                    break
                outfile.write(line)

                stripped = line.strip()
                if not stripped:
                    continue

                parts = stripped.split("\t")
                if not parts:
                    continue

                user_id = parts[0].strip()
                if user_id:
                    all_users.add(user_id)

                if len(parts) == 2 and parts[1].strip():
                    for friend in parts[1].split(","):
                        friend_id = friend.strip()
                        if friend_id:
                            all_users.add(friend_id)

        print(f"  Created {chunk_file}")

print(f"OK Split into {len(chunk_files)} chunks\n")

# Step 2: Upload chunks to mapper instances and run mappers
print("Step 2: Distributing chunks to mappers and executing...")
mapper_outputs = []

for i, mapper in enumerate(instances["mappers"]):
    host = mapper["public_ip"]
    chunk_file = chunk_files[i]
    remote_chunk = f"~/data/chunk_{i}.txt"
    remote_output = f"~/data/mapper_output_{i}.txt"

    print(f"\n  Mapper {i + 1}/{num_mappers} ({host}):")
    print(f"    Uploading {chunk_file}...")
    result = scp_upload(host, chunk_file, remote_chunk)
    if result.returncode != 0:
        print(f"    ERROR uploading: {result.stderr}")
        sys.exit(1)

    print("    Running mapper...")
    result = ssh(
        host,
        f"python3 ~/mapreduce/mapper.py {remote_chunk} {remote_output}",
        stream_output=True,
        label=f"mapper-{i+1}",
    )
    if result.returncode != 0:
        print(f"    ERROR running mapper: {result.stderr}")
        sys.exit(1)

    print("    OK Mapper completed")
    mapper_outputs.append((host, remote_output, f"mapper_output_{i}.txt"))

print(f"\nOK All {num_mappers} mappers completed\n")

print("Step 3: Collecting mapper outputs...")
shutil.rmtree("data/mapper_outputs", ignore_errors=True)
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

print(f"OK Downloaded {len(local_mapper_outputs)} mapper outputs\n")

print("Step 4: Preparing reducer partitions...")
num_reducers = len(instances["reducers"])
partition_dir = "data/reducer_partitions"
shutil.rmtree(partition_dir, ignore_errors=True)
os.makedirs(partition_dir, exist_ok=True)

partition_counts = [defaultdict(lambda: [0, False]) for _ in range(num_reducers)]
total_partition_lines = 0

for local_output in local_mapper_outputs:
    print(f"  Aggregating {local_output}...")
    with open(local_output, "r") as src:
        for line in src:
            total_partition_lines += 1
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 2:
                continue
            pair_key, marker = parts
            shard = shard_for_pair(pair_key, num_reducers)
            state = partition_counts[shard].get(pair_key)
            if state is None:
                state = [0, False]
                partition_counts[shard][pair_key] = state

            if marker == "-1":
                state[1] = True
                state[0] = 0
            else:
                if not state[1]:
                    state[0] += 1

print(f"  Total mapper tuples processed for partitioning: {total_partition_lines}")

partition_paths = []
for idx, counts in enumerate(partition_counts):
    path = os.path.join(partition_dir, f"reducer_{idx}.txt")
    partition_paths.append(path)
    pair_total = 0
    with open(path, "w") as outfile:
        for pair_key, (count, blocked) in counts.items():
            if blocked or count == 0:
                continue
            outfile.write(f"{pair_key}\t{count}\n")
            pair_total += 1
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"  Reducer {idx + 1} partition: {pair_total} pairs, {size_mb:.2f} MB")
    counts.clear()

print("OK Reducer partitions prepared\n")

print("Step 5: Running reducers...")
reducer_results = []

for idx, reducer in enumerate(instances["reducers"]):
    host = reducer["public_ip"]
    print(f"\n  Reducer {idx + 1}/{num_reducers} ({host}):")

    partition_path = partition_paths[idx]
    remote_input = f"~/data/reducer_input_{idx}.txt"
    print(f"    Uploading partition file ({partition_path})...")
    result = scp_upload(host, partition_path, remote_input)
    if result.returncode != 0:
        print(f"    ERROR uploading: {result.stderr}")
        sys.exit(1)

    remote_output = f"~/data/reducer_output_{idx}.txt"
    env_prefix = f"PARTITION_INDEX={idx} PARTITION_TOTAL={num_reducers} "
    reducer_cmd = (
        f"{env_prefix}python3 ~/mapreduce/reducer.py {remote_input} {remote_output}"
    )
    print("    Running reducer...")
    result = ssh(host, reducer_cmd, stream_output=True, label=f"reducer-{idx+1}")
    if result.returncode != 0:
        print(f"    ERROR running reducer: {result.stderr}")
        sys.exit(1)

    print("    OK Reducer completed")
    reducer_results.append((host, remote_output, f"reducer_output_{idx}.txt"))

print(f"\nOK All {num_reducers} reducers completed\n")

print("Step 6: Collecting reducer outputs...")
shutil.rmtree("data/reducer_outputs", ignore_errors=True)
os.makedirs("data/reducer_outputs", exist_ok=True)

reducer_local_files = []
for host, remote_path, filename in reducer_results:
    local_path = f"data/reducer_outputs/{filename}"
    print(f"  Downloading from {host}...")
    result = scp_download(host, remote_path, local_path)
    if result.returncode != 0:
        print(f"    ERROR downloading: {result.stderr}")
        sys.exit(1)

    reducer_local_files.append(local_path)

if not reducer_local_files:
    sys.exit("ERROR: No reducer outputs were downloaded.")

print(f"\nOK Reducer outputs downloaded: {len(reducer_local_files)} file(s)\n")

print("Step 7: Combining reducer outputs and generating final recommendations...")
user_candidate_counts = {}

for local_file in reducer_local_files:
    print(f"  Merging results from {local_file}...")
    with open(local_file, "r") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 2:
                continue
            user_id, recs = parts
            if user_id not in user_candidate_counts:
                user_candidate_counts[user_id] = {}
            rec_items = [r for r in recs.split(",") if r]
            for item in rec_items:
                if ":" not in item:
                    continue
                candidate, count_str = item.split(":", 1)
                candidate = candidate.strip()
                count_str = count_str.strip()
                if not candidate or not count_str:
                    continue
                try:
                    count_val = int(count_str)
                except ValueError:
                    continue
                current = user_candidate_counts[user_id].get(candidate, 0)
                user_candidate_counts[user_id][candidate] = current + count_val

final_output = os.path.join(ARTIFACTS_DIR, "friend_recommendations.txt")
combined_recommendations = {}

with open(final_output, "w") as f:
    for user_id in sorted(all_users, key=sort_user_key):
        candidate_counts = user_candidate_counts.get(user_id, {})
        if candidate_counts:
            sorted_candidates = sorted(
                candidate_counts.items(),
                key=lambda x: (-x[1], sort_user_key(x[0])),
            )
            top_candidates = [candidate for candidate, _ in sorted_candidates[:10]]
            recs_str = ",".join(top_candidates)
        else:
            recs_str = ""

        combined_recommendations[user_id] = recs_str
        f.write(f"{user_id}\t{recs_str}\n")

print(f"  Wrote final recommendations to {final_output}")

print("Step 8: Extracting report users...")
REPORT_USERS = [
    "924",
    "8941",
    "8942",
    "9019",
    "9020",
    "9021",
    "9022",
    "9990",
    "9992",
    "9993",
]

print(f"  Final output covers {len(all_users)} users")

print("\n=== Friend Recommendations for Report Users ===\n")
report_output = os.path.join(ARTIFACTS_DIR, "report_recommendations.txt")
with open(report_output, "w") as report_file:
    for user_id in REPORT_USERS:
        recs = combined_recommendations.get(user_id, "")
        if recs:
            print(f"User {user_id}: {recs}")
            report_file.write(f"User {user_id}: {recs}\n")
        else:
            print(f"User {user_id}: No recommendations found")
            report_file.write(f"User {user_id}: No recommendations found\n")

print(f"\nOK Saved report recommendations to {report_output}")

print("\n" + "="*50)
print("Friend Recommendation MapReduce Complete! OK")
print("="*50)
print(f"\nResults:")
print(f"  - Full recommendations:   {final_output}")
print(f"  - Report recommendations: {report_output}")
print(f"\nMapper instances:  {num_mappers}")
print(f"Reducer instances: {num_reducers}")
