# import libralies
import requests
import json
import pandas as pd
import mysql.connector
import csv

# Get data from APIs
url = "https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/AAPL?apikey=ddfe0e9a4ee01fc0a7c60f21cd01d374"
response = requests.get(url=url)

# Convert json to DataFrame
df = pd.json_normalize(response.json())
historical = response.json()["historical"]
df = pd.DataFrame(historical)

# Convert to correct data type
schema = {
    "date": "datetime64",
    "label": str,
    "adjDividend": float,
    "dividend": float,
    "recordDate": "datetime64",
    "paymentDate": "datetime64",
    "declarationDate": "datetime64"
}

for col, dtype in schema.items():
    df[col] = df[col].astype(dtype)

# save to csv file
csv_data = df.to_csv("AAPL_historical.csv", index=False)

# set up Connection python to MySQL
conn = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "password1",
    database = 'testDB'
)

cursor = conn.cursor()

# Create table on testDB database
create_table = """
CREATE TABLE AAPL_historical_7 (
    date date,
    label varchar(255),
    adjDividend float,
    dividend float,
    recordDate date,
    paymentDate date,
    declarationDate date
);
"""
cursor.execute(create_table)

# Insert value to AAPL_historical table
with open('AAPL_historical.csv') as csv_file:
    csvfile = csv.reader(csv_file, delimiter=",")
    all_value = []
    for row in csvfile:
        for i in range(len(row)):
            if row[i] == '':
                row[i] = None
            value = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])
        all_value.append(value)
del all_value[0]

cursor.executemany("""
    INSERT INTO AAPL_historical_7 (date, label, adjDividend, dividend, recordDate, paymentDate, declarationDate)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, all_value)

conn.commit()

conn.close()