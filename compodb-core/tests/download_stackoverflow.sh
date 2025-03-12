#!/bin/bash

set -x  # Enable debugging output

# Move two levels up from the script's execution directory
TARGET_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/tests/data/stackoverflow"

# Remove existing directory if it exists
#rm -rf "$TARGET_DIR"

# Create the directory
#mkdir -p "$TARGET_DIR"

# Change into the target directory
cd "$TARGET_DIR" || exit 1

# Get CSV files and store them in TARGET_DIR
wget -O "$TARGET_DIR/account.csv" 'https://tubcloud.tu-berlin.de/s/X65xmXtcHG3mmig/download/account.csv'
wget -O "$TARGET_DIR/answer.csv" "https://tubcloud.tu-berlin.de/s/gzDEzor2YDNdjbJ/download/answer.csv"
wget -O "$TARGET_DIR/badge.csv" "https://tubcloud.tu-berlin.de/s/oYwKowei5tSAiPi/download/badge.csv"
wget -O "$TARGET_DIR/comment.csv" "https://tubcloud.tu-berlin.de/s/Qf2aCBCqTEEPi8F/download/comment.csv"
wget -O "$TARGET_DIR/post_link.csv" "https://tubcloud.tu-berlin.de/s/DP8Pzi9gX4CxErJ/download/postlink.csv"
wget -O "$TARGET_DIR/question.csv" "https://tubcloud.tu-berlin.de/s/Mti64bZrdKJ2x7f/download/question.csv"
wget -O "$TARGET_DIR/site.csv" "https://tubcloud.tu-berlin.de/s/id7kp9M65siyTHb/download/site.csv"
wget -O "$TARGET_DIR/so_user.csv" "https://tubcloud.tu-berlin.de/s/TRJjpncDWW52g9e/download/so_user.csv"
wget -O "$TARGET_DIR/tag.csv" "https://tubcloud.tu-berlin.de/s/J6RWX4QtPsqTm7b/download/tag.csv"
wget -O "$TARGET_DIR/tag_question.csv" "https://tubcloud.tu-berlin.de/s/B9kzmQ5T8L7FJJT/download/tag_question.csv"