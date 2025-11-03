#!/usr/bin/env python3
import os
import sys
from collections import defaultdict


def reduce_friends(input_files, output_file):
    user_recommendations = defaultdict(dict)

    print(f"[Reducer] Reading {len(input_files)} mapper output files...", file=sys.stderr)
    for idx, input_file in enumerate(input_files):
        print(f"[Reducer] Processing file {idx+1}/{len(input_files)}: {input_file}", file=sys.stderr)
        line_count = 0
        with open(input_file, "r") as f:
            for raw_line in f:
                line_count += 1
                if line_count % 100000 == 0:
                    print(
                        f"[Reducer]   ... processed {line_count} lines from this file",
                        file=sys.stderr,
                    )

                line = raw_line.rstrip("\n")
                if not line:
                    continue

                parts = line.split("\t")
                if len(parts) != 2:
                    continue

                pair_key, count_str = parts
                try:
                    mutual_count = int(count_str)
                except ValueError:
                    continue

                if mutual_count <= 0:
                    continue

                users = pair_key.split(",")
                if len(users) != 2:
                    continue

                user1, user2 = users
                user_recommendations[user1][user2] = mutual_count
                user_recommendations[user2][user1] = mutual_count

        print(
            f"[Reducer] ✓ Completed file {idx+1}/{len(input_files)} ({line_count} lines total)",
            file=sys.stderr,
        )

    print(
        f"[Reducer] Aggregated recommendations for {len(user_recommendations)} users",
        file=sys.stderr,
    )

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

    print(
        f"Reducer processing {len(input_files)} mapper output(s) -> {output_file}",
        file=sys.stderr,
    )
    reduce_friends(input_files, output_file)
    print(f"Reducer complete: {output_file}", file=sys.stderr)
