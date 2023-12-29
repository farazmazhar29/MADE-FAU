import os
import zipfile
import pandas as pd

# Set the Kaggle API configuration directory to a writable location
os.environ['KAGGLE_CONFIG_DIR'] = './'

from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine

# Kaggle dataset URL
hepatitis_c_url = 'davidechicco/hepatitis-c-ehrs-from-japan'
arterial_disease_url = 'davidechicco/arterial-disease-and-ibd-ehrs-from-france'
chronic_kidney_disease_url = 'davidechicco/chronic-kidney-disease-ehrs-abu-dhabi'

# Directory to download and extract the dataset
downloadDir = '../data/raw'

# SQLite database file
dbFile = '../data/correlation-analysis-of-health-datasets.sqlite'


def DownloadDataset():
    api = KaggleApi()
    api.authenticate()

    # Create download directory if it doesn't exist
    if not os.path.exists(downloadDir):
        os.makedirs(downloadDir)

    api.dataset_download_files(hepatitis_c_url, path=downloadDir, unzip=True)
    api.dataset_download_files(arterial_disease_url, path=downloadDir, unzip=True)
    api.dataset_download_files(chronic_kidney_disease_url, path=downloadDir, unzip=True)


def ExtractZip(zipFile, extractPath):
    with zipfile.ZipFile(zipFile, 'r') as zipRef:
        zipRef.extractall(extractPath)


def FetchData():
    # List files in the download directory
    files = os.listdir(downloadDir)

    # Find the CSV file in the extracted files
    arterialDiseaseCsvFile = [file for file in files if file.endswith('.csv')][0]
    CKDCsvFile = [file for file in files if file.endswith('.csv')][1]
    hepatitisCsvFile = [file for file in files if file.endswith('.csv')][2]

    # Read the CSV file into a Pandas DataFrame
    arterialDiseaseData = pd.read_csv(os.path.join(downloadDir, arterialDiseaseCsvFile))
    CKDData = pd.read_csv(os.path.join(downloadDir, CKDCsvFile))
    hepatitisData = pd.read_csv(os.path.join(downloadDir, hepatitisCsvFile))

    return [arterialDiseaseData, CKDData, hepatitisData]


def PreprocessData(data):
    # Remove rows with missing values
    data[0] = data[0].dropna()
    data[1] = data[1].dropna()
    data[2] = data[2].dropna()

    data[1] = data[1].dropna()

    # Rename columns to make them more readable
    data[0].rename(columns={'sex_0female_1male': 'sex'}, inplace=True)
    data[0].rename(columns={'immunossupressants': 'immunosuppressant'}, inplace=True)

    data[1].rename(columns={'CholesterolBaseline': 'cholesterol'}, inplace=True)
    data[1].rename(columns={'CreatinineBaseline': 'creatinine'}, inplace=True)
    data[1].rename(columns={'BMIBaseline': 'BMI'}, inplace=True)
    data[1].rename(columns={'BMIBaseline': 'BMI'}, inplace=True)

    data[2].rename(columns={'BMIBaseline': 'BMI'}, inplace=True)
    return data


def TransferToSqlite(data):
    # Create SQLite engine
    engine = create_engine(f'sqlite:///{dbFile}')

    # Transfer the preprocessed data to SQLite
    data[0].to_sql('arterial_disease_data', engine, index=False, if_exists='replace')
    data[1].to_sql('CKD_data', engine, index=False, if_exists='replace')
    data[2].to_sql('hepatitis_data', engine, index=False, if_exists='replace')


def main():
    DownloadDataset()
    data = FetchData()

    preprocessed_data = PreprocessData(data)

    TransferToSqlite(preprocessed_data)


if __name__ == "__main__":
    main()
