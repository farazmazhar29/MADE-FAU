import pandas as pd
import sqlite3

# Define the CSV URL
csv_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"

# Function to process the CSV and insert data into SQLite
def process_csv(csv_url, database_name="trainstops.sqlite", table_name="trainstops"):
    # Read CSV data into a DataFrame directly from the URL
    df = pd.read_csv(csv_url, sep=';', decimal=',')

    # Drop the "Status" column
    df = df.drop(columns=['Status'], errors='ignore')

    # Drop rows with invalid values
    df = df.dropna()

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

# Execute the pipeline
process_csv(csv_url)
