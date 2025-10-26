#!/usr/bin/env python3
import sys
from collections import defaultdict

def reduce_friends(input_files, output_file):
    pair_mutuals = defaultdict(list)

    for input_file in input_files:
        with open(input_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split('\t')
                if len(parts) != 2:
                    continue

                pair_key = parts[0]  # "user1,user2"
                mutual_or_flag = parts[1]  # mutual friend ID or "-1"

                pair_mutuals[pair_key].append(mutual_or_flag)

    user_recommendations = defaultdict(list)

    for pair_key, mutuals in pair_mutuals.items():
        if "-1" in mutuals:
            continue

        mutual_friends = [m for m in mutuals if m != "-1"]
        mutual_count = len(mutual_friends)

        if mutual_count == 0:
            continue

        users = pair_key.split(',')
        if len(users) != 2:
            continue

        user1, user2 = users
        user_recommendations[user1].append((user2, mutual_count))
        user_recommendations[user2].append((user1, mutual_count))

    final_recommendations = {}

    for user, recommendations in user_recommendations.items():
        sorted_recs = sorted(recommendations, key=lambda x: (-x[1], int(x[0]) if x[0].isdigit() else x[0]))
        top_10 = sorted_recs[:10]
        recommended_users = [rec[0] for rec in top_10]
        final_recommendations[user] = recommended_users

    with open(output_file, 'w') as f:
        for user in sorted(final_recommendations.keys(), key=lambda x: int(x) if x.isdigit() else x):
            recommendations = final_recommendations[user]
            recommendations_str = ','.join(recommendations)
            f.write(f"{user}\t{recommendations_str}\n")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: reducer.py <input_file1> [<input_file2> ...] <output_file>", file=sys.stderr)
        sys.exit(1)

    input_files = sys.argv[1:-1]
    output_file = sys.argv[-1]

    print(f"Reducer processing {len(input_files)} mapper output(s) -> {output_file}", file=sys.stderr)
    reduce_friends(input_files, output_file)
    print(f"Reducer complete: {output_file}", file=sys.stderr)
