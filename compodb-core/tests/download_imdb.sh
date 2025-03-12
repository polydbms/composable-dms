#!/bin/bash

set -x  # Enable debugging output

# Move two levels up from the script's execution directory
TARGET_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/tests/data/imdb"

# Remove existing directory if it exists
rm -rf "$TARGET_DIR"

# Create the directory
mkdir -p "$TARGET_DIR"

# Change into the target directory
cd "$TARGET_DIR" || exit 1

# Download the file
curl -L -O https://tubcloud.tu-berlin.de/s/YCNzjyCxt284DrL/download

# Extract the archive
tar xvf download