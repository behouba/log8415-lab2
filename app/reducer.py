#!/usr/bin/env python3
"""
Reducer for Friend Recommendation System

Input format: <UserPair><TAB><MutualFriend or -1> (sorted by key)
Output format: <UserID><TAB><Recommended1,Recommended2,...> (top 10)

Algorithm:
1. Group all values by key (user pair)
2. If any value is -1, skip (already friends)
3. Otherwise, count mutual friends
4. Aggregate recommendations per user
5. Sort by mutual friend count (desc), then by user ID (asc)
6. Output top 10 recommendations per user
"""
import sys
from collections import defaultdict

def reduce_friends(input_files, output_file):
    """
    Process mapper outputs and generate friend recommendations
    """
    # Step 1: Read all mapper outputs and group by user pair
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

    # Step 2: Process each pair and count mutual friends
    user_recommendations = defaultdict(list)  # user -> [(recommended_user, mutual_count), ...]

    for pair_key, mutuals in pair_mutuals.items():
        # Check if this pair are already friends (-1 marker)
        if "-1" in mutuals:
            continue  # Skip, they're already friends

        # Count mutual friends (excluding -1)
        mutual_friends = [m for m in mutuals if m != "-1"]
        mutual_count = len(mutual_friends)

        if mutual_count == 0:
            continue  # No mutual friends

        # Parse the pair
        users = pair_key.split(',')
        if len(users) != 2:
            continue

        user1, user2 = users

        # Add recommendation for both users
        user_recommendations[user1].append((user2, mutual_count))
        user_recommendations[user2].append((user1, mutual_count))

    # Step 3: Sort and select top 10 recommendations per user
    final_recommendations = {}

    for user, recommendations in user_recommendations.items():
        # Sort by:
        # 1. Mutual friend count (descending)
        # 2. User ID (ascending) for tie-breaking
        sorted_recs = sorted(recommendations, key=lambda x: (-x[1], int(x[0]) if x[0].isdigit() else x[0]))

        # Take top 10
        top_10 = sorted_recs[:10]

        # Extract just the user IDs
        recommended_users = [rec[0] for rec in top_10]

        final_recommendations[user] = recommended_users

    # Step 4: Write output
    with open(output_file, 'w') as f:
        # Sort users by ID for consistent output
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
