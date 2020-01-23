import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


def get_excel_data(filename: str) -> pd.DataFrame:
    XLSX_FILE = f'input_data\\{filename}'

    df = pd.read_excel(
        XLSX_FILE, sheet_name=1, index=False, usecols='A,D', skiprows=11, nrows=31)

    df.rename(
        columns={'Aufwand \nnach Anwesenheit (h)': 'working_hours', 'Datum': 'date'}, inplace=True)

    return df


def enrich(df: pd.DataFrame, client: str, project: str = None, location: str = None) -> pd.DataFrame:
    if 'weekday' not in df:
        df['weekday'] = df['date'].dt.dayofweek
    if 'client' not in df:
        df['client'] = client
    if 'project' not in df:
        df['project'] = project
    if 'location' not in df:
        df['location'] = location
        df['location'].loc[df['weekday'] == 4] = 'Home Office'
    if 'travelling_hours' not in df:
        df['travelling_hours'] = 0.75
        df['travelling_hours'].loc[df['weekday'] == 4] = None
    if 'travelling_means' not in df:
        df['travelling_means'] = 'local transport'
        df['travelling_means'].loc[df['weekday'] == 4] = None

    return df


def write_to_database(db_name: str, table_name: str, df: pd.DataFrame, drop_nan: bool = True, append: bool = True):
    engine = create_engine(f'sqlite:///{db_name}.db', echo=False)

    if(drop_nan):
        df = df.dropna()

    df.to_sql(f'{table_name}', con=engine, index=False,
              if_exists='append' if append else 'replace')


def archive(filename: str):
    cwd = os.getcwd()
    source = f'{cwd}\\input_data\\{filename}'
    destination = f'{cwd}\\processed_data\\{filename}'
    os.rename(source, destination)

load_dotenv()

for _, _, files in os.walk(os.getcwd()+'\\input_data'):

    for input_file in files:
        if(input_file.endswith('.xlsx')):
            yd = enrich(get_excel_data(input_file), os.getenv('CLIENT'),
                        os.getenv('PROJECT'), os.getenv('LOCATION'))

            write_to_database('output\\migration', 'time_sheet', yd)

            archive(input_file)
