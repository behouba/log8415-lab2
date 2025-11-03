#!/usr/bin/env python3
import sys
import os

def emit(key, value):
    print(f"{key}\t{value}")

def map_friends(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        # Redirect stdout to output file
        original_stdout = sys.stdout
        sys.stdout = outfile

        for line in infile:
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) != 2:
                continue

            user = parts[0].strip()
            friends_str = parts[1].strip()

            if not friends_str:
                friends = []
            else:
                friends = [f.strip() for f in friends_str.split(',') if f.strip()]

            for friend in friends:
                pair = tuple(sorted([user, friend]))
                emit(f"{pair[0]},{pair[1]}", "-1")

            for i in range(len(friends)):
                for j in range(i + 1, len(friends)):
                    friend_a = friends[i]
                    friend_b = friends[j]
                    pair = tuple(sorted([friend_a, friend_b]))
                    emit(f"{pair[0]},{pair[1]}", user)

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
