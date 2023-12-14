#!/bin/bash

# Set up a temporary directory for testing
test_dir="./test"
mkdir -p "$test_dir"

check_status() {
    if [ $? -eq 0 ]; then
        echo "Test Passed"
    else
        echo "Test Failed"
        exit 1
    fi
}

# Test Case 1: Run the main script
echo "Running Test Case 1: Execute main script"
python3 pipeline.py
check_status

# Test Case 2: Check if the SQLite database file is created
if [ -f "../data/netflix-tv-shows-and-movies.sqlite" ]; then
    echo "SQLite database file created successfully."
else
    echo "Error: SQLite database file not found."
    exit 1
fi

# Test Case 3: Check for Downloaded Files
echo "Running Test Case 3: Check for Downloaded Files"

# List files in the download directory
downloaded_files=$(ls ../data/raw)

# Check if the expected CSV files are present
expected_csv_files=("credits.csv" "titles.csv")  # Replace with the actual expected CSV file names

for expected_file in "${expected_csv_files[@]}"; do
    if [[ "${downloaded_files}" =~ ${expected_file} ]]; then
        echo "File ${expected_file} found"
    else
        echo "File ${expected_file} not found"
        exit 1
    fi
done

# Test Case 6: Check for Preprocessed Data
echo "Running Test Case 6: Check for Preprocessed Data"

# Run the main script to obtain preprocessed data
python3 pipeline.py

# Load preprocessed data from SQLite database
preprocessed_data=$(python3 -c "
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///../data/netflix-tv-shows-and-movies.sqlite')
credits_data = pd.read_sql_table('Neflix_credit_data', engine)
titles_data = pd.read_sql_table('Neflix_titles_data', engine)

print(credits_data.head())
print(titles_data.head())
")

# Check if rows with missing values are removed
if [[ "${preprocessed_data}" =~ "NaN" ]]; then
    echo "Test Failed: Rows with missing values not removed"
    exit 1
fi

# Check if missing values are imputed with mean
imputed_mean=$(python3 -c "
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///../data/netflix-tv-shows-and-movies.sqlite')
titles_data = pd.read_sql_table('Neflix_titles_data', engine)

imdb_score_mean = titles_data['IMDB_score'].mean()
imdb_votes_mean = titles_data['IMDB_votes'].mean()
popularity_mean = titles_data['TMDB_popularity'].mean()
tmdb_score_mean = titles_data['TMDB_score'].mean()

print(imdb_score_mean, imdb_votes_mean, popularity_mean, tmdb_score_mean)
")

if [[ "${imputed_mean}" != *"NaN"* ]]; then
    echo "Test Failed: Missing values not imputed with mean"
    exit 1
fi

echo "Test Passed: Data preprocessed successfully"


# Clean up: Remove downloaded files (optional)
echo "Cleaning up downloaded files"
rm -r ./../data/raw

echo "All tests passed successfully!"
