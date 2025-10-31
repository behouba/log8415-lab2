#!/usr/bin/env python3
import hashlib
import os
import sys
from collections import defaultdict


def should_process_pair(pair_key, partition_index, partition_total):
    if partition_index is None or partition_total is None:
        return True
    digest = hashlib.md5(pair_key.encode("utf-8")).hexdigest()
    shard = int(digest[:8], 16) % partition_total
    return shard == partition_index


def reduce_friends(input_files, output_file, partition_index=None, partition_total=None):
    pair_mutuals = defaultdict(list)

    print(f"[Reducer] Reading {len(input_files)} mapper output files...", file=sys.stderr)
    for idx, input_file in enumerate(input_files):
        print(f"[Reducer] Processing file {idx+1}/{len(input_files)}: {input_file}", file=sys.stderr)
        line_count = 0
        with open(input_file, "r") as f:
            for line in f:
                line_count += 1
                if line_count % 100000 == 0:
                    print(
                        f"[Reducer]   ... processed {line_count} lines from this file",
                        file=sys.stderr,
                    )

                line = line.strip()
                if not line:
                    continue

                parts = line.split("\t")
                if len(parts) != 2:
                    continue

                pair_key = parts[0]  # "user1,user2"
                mutual_or_flag = parts[1]  # mutual friend ID or "-1"

                if not should_process_pair(pair_key, partition_index, partition_total):
                    continue

                pair_mutuals[pair_key].append(mutual_or_flag)

        print(
            f"[Reducer] ✓ Completed file {idx+1}/{len(input_files)} ({line_count} lines total)",
            file=sys.stderr,
        )

    print(f"[Reducer] Aggregating {len(pair_mutuals)} unique pairs...", file=sys.stderr)
    user_recommendations = defaultdict(dict)

    pair_count = 0
    for pair_key, mutuals in pair_mutuals.items():
        pair_count += 1
        if pair_count % 50000 == 0:
            print(
                f"[Reducer]   ... processed {pair_count}/{len(pair_mutuals)} pairs",
                file=sys.stderr,
            )
        if "-1" in mutuals:
            continue

        mutual_friends = [m for m in mutuals if m != "-1"]
        mutual_count = len(mutual_friends)

        if mutual_count == 0:
            continue

        users = pair_key.split(",")
        if len(users) != 2:
            continue

        user1, user2 = users
        user_recommendations[user1][user2] = mutual_count
        user_recommendations[user2][user1] = mutual_count

    print(
        f"[Reducer] Writing intermediate recommendations to {output_file}...",
        file=sys.stderr,
    )
    with open(output_file, "w") as f:
        for user in sorted(user_recommendations.keys(), key=lambda x: int(x) if x.isdigit() else x):
            rec_items = user_recommendations[user].items()
            sorted_recs = sorted(
                rec_items,
                key=lambda x: (-x[1], int(x[0]) if x[0].isdigit() else x[0]),
            )
            formatted = ",".join(f"{candidate}:{count}" for candidate, count in sorted_recs)
            f.write(f"{user}\t{formatted}\n")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: reducer.py <input_file1> [<input_file2> ...] <output_file>", file=sys.stderr)
        sys.exit(1)

    input_files = sys.argv[1:-1]
    output_file = sys.argv[-1]

    partition_index = os.getenv("PARTITION_INDEX")
    partition_total = os.getenv("PARTITION_TOTAL")
    if partition_index is not None and partition_total is not None:
        try:
            partition_index = int(partition_index)
            partition_total = int(partition_total)
        except ValueError:
            print(
                "[Reducer] WARN: Invalid PARTITION_INDEX/PARTITION_TOTAL values, ignoring partitioning.",
                file=sys.stderr,
            )
            partition_index = None
            partition_total = None

    print(
        f"Reducer processing {len(input_files)} mapper output(s) -> {output_file} "
        f"(partition={partition_index}/{partition_total})",
        file=sys.stderr,
    )
    reduce_friends(input_files, output_file, partition_index, partition_total)
    print(f"Reducer complete: {output_file}", file=sys.stderr)
