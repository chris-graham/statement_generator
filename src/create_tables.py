import psycopg2

conn = psycopg2.connect("dbname=vervoer user=ubuntu")
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

cur.execute('DROP TABLE IF EXISTS xero')
cur.execute("""
CREATE TABLE xero(
    id serial PRIMARY KEY,
    contact_id varchar(50),
    name varchar(100),
    input_date datetime,
    balance money)
""")

conn.commit()
conn.close()
