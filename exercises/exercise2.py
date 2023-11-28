import pandas as pd
import requests
from io import StringIO
import sqlite3

# Define the CSV URL
csv_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"


# Function to download and process the CSV
def process_csv(csv_url, database_name="trainstops.sqlite", table_name="trainstops"):
    # Download CSV file
    response = requests.get(csv_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Read CSV data into a DataFrame
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data, sep=';', decimal=',')  # Adjust separator based on the actual CSV format

        # Drop the "Status" column
        df = df.drop(columns=['Status'], errors='ignore')

        # Drop rows with invalid values
        df = df.dropna()  # Drop rows with any NaN values

        # Filter rows based on specified conditions
        df = df[df['Verkehr'].isin(['FV', 'RV', 'nur DPN'])]
        df = df[(df['Laenge'] >= -90) & (df['Laenge'] <= 90)]
        df = df[(df['Breite'] >= -90) & (df['Breite'] <= 90)]
        df = df[df['IFOPT'].str.match(r'^[A-Za-z]{2}:\d+:\d+(\:\d+)?$')]

        # SQLite connection and cursor
        conn = sqlite3.connect(database_name)
        cursor = conn.cursor()

        # Create the "trainstops" table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trainstops (
                EVA_NR BIGINT,
                DS100 TEXT,
                IFOPT TEXT,
                NAME TEXT,
                Verkehr TEXT,
                Laenge FLOAT,
                Breite FLOAT,
                Betreiber_Name TEXT,
                Betreiber_Nr BIGINT
            )
        ''')

        # Insert data into the "trainstops" table
        df.to_sql(name=table_name, con=conn, index=False, if_exists='replace')

        # Commit changes and close connection
        conn.commit()
        conn.close()

        print("Data processing and insertion into SQLite completed.")
    else:
        print(f"Failed to download CSV. Status code: {response.status_code}")


# Execute the pipeline
process_csv(csv_url)
