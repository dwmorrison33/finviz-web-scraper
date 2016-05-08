import psycopg2
from scraper import get_data

DB = psycopg2.connect(dbname="david", user="david")
cursor = DB.cursor()
table_name = "market_cap"
cursor.execute("DROP TABLE IF EXISTS " + table_name)
cursor.execute("""
	CREATE TABLE {} (
		no INT,
		ticket VARCHAR(10),
		company VARCHAR(255),
		sector VARCHAR(255),
		industry VARCHAR(255),
		country VARCHAR(255),
		market_cap FLOAT(4),
		pe FLOAT(4),
		price FLOAT(4),
		change FLOAT(4),
		volume INT
	);
""".format(table_name))
DB.commit()

data = get_data()

from pprint import pprint
pprint(data)

for row in data:
	cursor.execute("""
	INSERT INTO {}(no, ticket, company, sector, industry, country, market_cap, pe, price, change, volume)
	VALUES({}, '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {})
	""".format(
		table_name,
		row['No.'],
		row['Ticker'],
		row['Company'],
		row['Sector'],
		row['Industry'],
		row['Country'],
		row['Market Cap'][:-1],
		"NULL" if row['P/E'] == "-" else row['P/E'],
		row['Price'],
		row['Change'][:-1],
		row['Volume'].replace(',', ""),
	))
DB.commit()

avg = cursor.execute('select AVG(market_cap) from market_cap;')

#results = cursor.fetchall()
#print(results)
#DB.close()