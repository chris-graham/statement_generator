import psycopg2

conn = psycopg2.connect("dbname=vervoer user=ubuntu")

with open('/home/ubuntu/projects/statement_generator/data/ezpass/Transactions_transaction_csv_report_127004637.csv', 'r') as f:
    next(f)
    cur.copy_from(f, 'ezpass', sep=',')

conn.commit()
conn.close()

def transform_ezpass_data(connection, filename):

def insert_ezpass_data(connection, filename):

def create_ezpass_table(connection):
    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS ezpass')
    cur.execute('DROP TYPE IF EXISTS y_or_n')

    cur.execute("CREATE TYPE y_or_n AS ENUM ('Y', 'N')")
    cur.execute("""
    CREATE TABLE ezpass(
        id integer PRIMARY KEY,
        posting_date date,
        transaction_date date,
        tag_plate varchar(30),
        agency varchar(20),
        activity varchar(20),
        entry_time time,
        entry_plaza varchar(10),
        entry_lane varchar(10),
        exit_time time,
        exit_plaza varchar(10),
        exit_lane varchar(10),
        vehicle_type_code smallint,
        amount money,
        prepaid y_or_n,
        plan_rate varchar(20),
        fare_type char,
        balance money)
    """)
