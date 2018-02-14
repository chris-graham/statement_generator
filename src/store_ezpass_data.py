import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def connect(user, password, db, host='localhost', port=5432):
    '''Returns a connection and a metadata object'''
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')

    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con, reflect=True)

    return con, meta

def create_ezpass_table_pg():
    pass

def create_ezpass_table(connection):
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS ezpass')

    cur.execute("""
    CREATE TABLE ezpass(
        id serial PRIMARY KEY,
        posting_date date,
        transaction_date date,
        tag_plate_number varchar(30),
        agency varchar(20),
        activity varchar(20),
        entry_time time,
        entry_plaza varchar(10),
        entry_lane varchar(10),
        exit_time time,
        exit_plaza varchar(10),
        exit_lane varchar(10),
        vehicle_type_code varchar(10),
        amount money,
        prepaid varchar(5),
        plan_rate varchar(20),
        fare_type char,
        balance money)
    """)

    conn.commit()

def insert_ezpass_data(connection, filename):
    cur = conn.cursor()
    with open(filename, 'r') as f:
        next(f)
        cur.copy_from(f, 'ezpass', sep=',', null='-', columns=('posting_date','transaction_date','tag_plate','agency','activity','entry_time','entry_plaza','entry_lane','exit_time','exit_plaza','exit_lane','vehicle_type_code','amount','prepaid','plan_rate','fare_type','balance'))
    conn.commit()

def transform_ezpass_data(filename):
    posting_dates = []
    transaction_dates = []
    df = pd.read_csv(filename)
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('/', '_')

    for posting_date in df['posting_date']:
        posting_dates.append(convert_date_format(posting_date))
    df['posting_date'] = posting_dates

    for transaction_date in df['transaction_date']:
        transaction_dates.append(convert_date_format(transaction_date))
    df['transaction_date'] = transaction_dates
    
    return df

def convert_date_format(date):
    return datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y/%m/%d')

# conn = psycopg2.connect("dbname=vervoer user=ubuntu")
# con, meta = connect('ubuntu', '', 'vervoer')
# con
# meta
# create_ezpass_table(conn)
engine = create_engine('postgresql+psycopg2:///vervoer')
# conn = engine.connect()
table_data = transform_ezpass_data('/home/ubuntu/projects/statement_generator/data/ezpass/ezpass.csv')
table_data.to_sql('ezpass', engine, if_exists='replace')
# print(table_data)
# insert_ezpass_data(conn, table_data)

# conn.close()