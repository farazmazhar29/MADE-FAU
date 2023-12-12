#!/bin/bash

# Set up a temporary directory for testing
test_dir="./test"
mkdir -p "$test_dir"

# Run the data pipeline script
python pipeline.py

# Check if the SQLite database file is created
if [ -f "../data/netflix-tv-shows-and-movies.sqlite" ]; then
    echo "SQLite database file created successfully."
else
    echo "Error: SQLite database file not found."
    exit 1
fi

# Add more tests as needed based on your specific requirements

# Clean up the temporary directory
rm -r "$test_dir"
