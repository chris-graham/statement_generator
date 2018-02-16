import datetime
import os
import pandas as pd
import psycopg2
from sqlalchemy import *

DATA_PATH = '/home/ubuntu/projects/statement_generator/data/ezpass/'

# Cleanup data from CSV file
def transform_ezpass_data(filename):
    posting_dates = []
    transaction_dates = []
    df = pd.read_csv(filename)
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('/', '_')

    # Format addresses in YYYYMMDD format
    for posting_date in df['posting_date']:
        posting_dates.append(convert_date_format(posting_date))
    df['posting_date'] = posting_dates

    for transaction_date in df['transaction_date']:
        transaction_dates.append(convert_date_format(transaction_date))
    df['transaction_date'] = transaction_dates

    # Replace '-' with None
    df = df.replace('-', df.replace(['-'], [None]))
    
    return df

# Convert US date format to database friendly format
def convert_date_format(date):
    return datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y/%m/%d')

def backup_file(filename):
    pass

# Connect to database
engine = create_engine('postgresql+psycopg2:///vervoer')
# Get all data filenames from data directory
filenames = os.listdir(DATA_PATH)

for filename in filenames:
    print('loading data from ' + filename)
    table_data = transform_ezpass_data(DATA_PATH + filename)
    # Dump data from dataframe to database
    table_data.to_sql('ezpass', engine, if_exists='append', index=False)