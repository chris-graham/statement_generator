import datetime
import pprint
import os
from xero import Xero
from xero.auth import PrivateCredentials

pp = pprint.PrettyPrinter(indent=4)

XERO_CONSUMER_KEY = os.environ['XERO_CONSUMER_KEY']
XERO_CONSUMER_SECRET = os.environ['XERO_CONSUMER_SECRET']

with open('../xero.pem', 'r') as f:
	rsa_key = f.read()

credentials = PrivateCredentials(XERO_CONSUMER_KEY, rsa_key)
xero = Xero(credentials)
contacts = xero.contacts.filter(IsCustomer='true')

contacts_with_balance = []

for contact in contacts:
	if 'Balances' in contact:
		contacts_with_balance.append(contact)

pp.pprint(contacts_with_balance)
# print(len(contacts))