import os
import zipfile
import pandas as pd

# Set the Kaggle API configuration directory to a writable location
os.environ['KAGGLE_CONFIG_DIR'] = './'

from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine

# Kaggle dataset URL
datasetUrl = 'victorsoeiro/netflix-tv-shows-and-movies'

# Directory to download and extract the dataset
downloadDir = '../data/raw'

# SQLite database file
dbFile = '../data/netflix-tv-shows-and-movies.sqlite'


def DownloadDataset():
    api = KaggleApi()
    api.authenticate()

    # Create download directory if it doesn't exist
    if not os.path.exists(downloadDir):
        os.makedirs(downloadDir)

    api.dataset_download_files(datasetUrl, path=downloadDir, unzip=True)


def ExtractZip(zipFile, extractPath):
    with zipfile.ZipFile(zipFile, 'r') as zipRef:
        zipRef.extractall(extractPath)


def FetchData():
    # List files in the download directory
    files = os.listdir(downloadDir)

    # Find the CSV file in the extracted files
    creditCsvFile = [file for file in files if file.endswith('.csv')][0]
    titleCsvFile = [file for file in files if file.endswith('.csv')][1]

    # Read the CSV file into a Pandas DataFrame
    creditsData = pd.read_csv(os.path.join(downloadDir, creditCsvFile))
    titlesData = pd.read_csv(os.path.join(downloadDir, titleCsvFile))

    return [creditsData, titlesData]


def PreprocessData(data):
    # Remove rows with missing values
    data[0] = data[0].dropna()
    # data[1] = data[1].dropna()
    print(data[0])
    # print(data[1])

    # Impute missing values with mean
    data[1]['imdb_score'].fillna(data[1]['imdb_score'].mean(), inplace=True)
    data[1]['imdb_votes'].fillna(data[1]['imdb_votes'].mean(), inplace=True)
    data[1]['tmdb_popularity'].fillna(data[1]['tmdb_popularity'].mean(), inplace=True)
    data[1]['tmdb_score'].fillna(data[1]['tmdb_score'].mean(), inplace=True)

    data[1] = data[1].dropna()

    # Rename columns to make them more readable
    data[1].rename(columns={'imdb_id': 'IMDB_id', 'imdb_score': 'IMDB_score', 'imdb_votes': 'IMDB_votes',
                            'tmdb_popularity': 'TMDB_popularity', 'tmdb_score': 'TMDB_score'}, inplace=True)

    # Convert the 'Date' column to datetime format
    #    data['Date'] = pd.to_datetime(data['Date'])

    # Normalize numerical columns using StandardScaler
    #    scaler = StandardScaler()
    #    numerical_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']
    #    data[numerical_columns] = scaler.fit_transform(data[numerical_columns])

    return data


def TransferToSqlite(data):
    # Create SQLite engine
    engine = create_engine(f'sqlite:///{dbFile}')

    # Transfer the preprocessed data to SQLite
    data[0].to_sql('Neflix_credit_data', engine, index=False, if_exists='replace')
    data[1].to_sql('Neflix_titles_data', engine, index=False, if_exists='replace')


def main():
    DownloadDataset()
    data = FetchData()

    preprocessed_data = PreprocessData(data)

    TransferToSqlite(preprocessed_data)


if __name__ == "__main__":
    main()
