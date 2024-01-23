import urllib.request
import zipfile
import pandas as pd
import sqlalchemy


def celsius_to_fahrenheit(temp_cels):
    return (temp_cels * 9 / 5) + 32


URL = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"

urllib.request.urlretrieve(URL, "data.zip")
with zipfile.ZipFile('data.zip', 'r') as zip_ref:
    zip_ref.extractall("../exercises/")
fields = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C",
          "Geraet aktiv"]

df = pd.read_csv('../exercises/data.csv', sep=";", index_col=False, usecols=fields, encoding='utf-8', decimal=",")
df.rename(columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"},
          inplace=True)
df['Temperatur'] = celsius_to_fahrenheit(df['Temperatur'])
df['Batterietemperatur'] = celsius_to_fahrenheit(df['Batterietemperatur'])

# Validate data
df = df.loc[df['Geraet'] > 0]
df = df.loc[(df['Monat'] >= 1) & (df['Monat'] <= 12)]
df = df.loc[(df['Temperatur'] >= -459.67) & (df['Temperatur'] <= 212)]
df = df.loc[(df['Batterietemperatur'] >= -459.67) & (df['Batterietemperatur'] <= 212)]
valid_gear_aktiv_values = ["Ja", "Nein"]
df = df.loc[(df['Geraet aktiv'].isin(valid_gear_aktiv_values))]

df.to_sql('temperatures', 'sqlite:///temperatures.sqlite', if_exists='replace', index=False, dtype={
    "Geraet": sqlalchemy.BIGINT,
    "Hersteller": sqlalchemy.TEXT,
    "Model": sqlalchemy.TEXT,
    "Monat": sqlalchemy.BIGINT,
    "Temperatur": sqlalchemy.FLOAT,
    "Batterietemperatur": sqlalchemy.FLOAT,
    "Geraet aktiv": sqlalchemy.TEXT,
})
