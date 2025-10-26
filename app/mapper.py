#!/usr/bin/env python3
"""
Mapper for Friend Recommendation System

Input format: <UserID><TAB><Friend1,Friend2,Friend3,...>
Output format: <UserPair><TAB><MutualFriend or -1>

Algorithm:
1. For each user and their friends:
   - Emit (user, friend) -> -1  (marks existing friendship)
   - For each pair of friends, emit (friend_a, friend_b) -> user (potential recommendation via mutual friend)
"""
import sys
import os

def emit(key, value):
    """Emit key-value pair"""
    print(f"{key}\t{value}")

def map_friends(input_file, output_file):
    """
    Process adjacency list and emit intermediate key-value pairs
    """
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        # Redirect stdout to output file
        original_stdout = sys.stdout
        sys.stdout = outfile

        for line in infile:
            line = line.strip()
            if not line:
                continue

            # Parse input: user<TAB>friend1,friend2,...
            parts = line.split('\t')
            if len(parts) != 2:
                continue

            user = parts[0].strip()
            friends_str = parts[1].strip()

            # Parse friends list
            if not friends_str:
                friends = []
            else:
                friends = [f.strip() for f in friends_str.split(',') if f.strip()]

            # Emit existing friendships (to exclude from recommendations)
            for friend in friends:
                # Create a normalized pair (smaller, larger) to avoid duplicates
                pair = tuple(sorted([user, friend]))
                emit(f"{pair[0]},{pair[1]}", "-1")

            # Emit potential recommendations (friends-of-friends)
            # For each pair of this user's friends, they could be recommended to each other
            # with 'user' as the mutual friend
            for i in range(len(friends)):
                for j in range(i + 1, len(friends)):
                    friend_a = friends[i]
                    friend_b = friends[j]

                    # Create normalized pairs
                    pair = tuple(sorted([friend_a, friend_b]))
                    emit(f"{pair[0]},{pair[1]}", user)

        # Restore stdout
        sys.stdout = original_stdout

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: mapper.py <input_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    print(f"Mapper processing: {input_file} -> {output_file}", file=sys.stderr)
    map_friends(input_file, output_file)
    print(f"Mapper complete: {output_file}", file=sys.stderr)
