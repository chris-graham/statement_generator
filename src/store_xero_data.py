import os
import psycopg2
from datetime import datetime
from psycopg2 import extras
from xero import Xero
from xero.auth import PrivateCredentials

# Get Consumer Key from system environment
XERO_CONSUMER_KEY = os.environ['XERO_CONSUMER_KEY']

# Get private key
with open('../xero.pem', 'r') as f:
    rsa_key = f.read()

# Authenticate with Xero API
credentials = PrivateCredentials(XERO_CONSUMER_KEY, rsa_key)
xero = Xero(credentials)

# Get all drivers from Xero API
contacts = xero.contacts.filter(IsCustomer='true')

contacts_with_balance = []

# Determine which drivers have an oustanding balance
for contact in contacts:
    if 'Balances' in contact:
        contact_id = contact['ContactID']
        name = contact['Name']
        input_date = datetime.now()
        balance = contact['Balances']['AccountsReceivable']['Outstanding']
        contacts_with_balance.append((contact_id, name, input_date, balance))

# Connect to database
conn = psycopg2.connect("dbname=vervoer user=ubuntu")
cur = conn.cursor()

# Insert drivers with balances into xero table
extras.execute_values(cur, 'INSERT INTO xero (contact_id, name, input_date, ' +
                      'balance) VALUES %s', contacts_with_balance)

conn.commit()
conn.close()
