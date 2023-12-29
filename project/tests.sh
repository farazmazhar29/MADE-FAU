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
python3 pipeline.ipynb
check_status

# Test Case 2: Check if the SQLite database file is created
if [ -f "../data/correlation-analysis-of-health-datasets.sqlite" ]; then
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
expected_csv_files=("arterial_disease_and_IBD_EHRs_from_France.csv" "ChronicKidneyDisease_EHRs_from_AbuDhabi.csv" "hepatitis_C_EHRs_Japan.csv")  # Replace with the actual expected CSV file names

for expected_file in "${expected_csv_files[@]}"; do
    if [[ "${downloaded_files}" =~ ${expected_file} ]]; then
        echo "File ${expected_file} found"
    else
        echo "File ${expected_file} not found"
        exit 1
    fi
done


# Clean up: Remove downloaded files (optional)
echo "Cleaning up downloaded files"
rm -r ./../data/raw

echo "All tests passed successfully!"
