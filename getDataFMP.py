# import libralies
import requests
import json
import pandas as pd
import mysql.connector
import csv

# Get data from APIs
url_his = "https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/AAPL?apikey=ddfe0e9a4ee01fc0a7c60f21cd01d374"
url_del = "https://financialmodelingprep.com/api/v3/delisted-companies?page=0&apikey=ddfe0e9a4ee01fc0a7c60f21cd01d374"
response_his = requests.get(url=url_his)
response_del = requests.get(url=url_del)
print("Complete: Get Data from APIs")
# Convert json to DataFrame
historical = response_his.json()["historical"]
delisted = response_del.json()
df_his = pd.DataFrame(historical)
df_del = pd.DataFrame(delisted)
print("Complete: Convert json to DataFrame")
# Convert to correct data type
schema_his = {
    "date": "datetime64",
    "label": str,
    "adjDividend": float,
    "dividend": float,
    "recordDate": "datetime64",
    "paymentDate": "datetime64",
    "declarationDate": "datetime64"
}
schema_del = {
    "symbol": str, 
    "companyName": str, 
    "exchange": str, 
    "ipoDate": "datetime64", 
    "delistedDate": "datetime64"
}

for col, dtype in schema_his.items():
    df_his[col] = df_his[col].astype(dtype)
for col, dtype in schema_del.items():
    df_del[col] = df_del[col].astype(dtype)
print("Complete: Convert to correct data type")
# save to csv file
df_his.to_csv("historicalDividends.csv", index=False)
df_del.to_csv("delistedCompanies.csv", index=False)
print("Complete: save to csv file")
# set up Connection python to MySQL
conn = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "password1",
    database = 'testDB'
)

cursor = conn.cursor()
print("Complete: set up Connection python to MySQL")
# Create table on testDB database
create_his = """
CREATE TABLE historicalDividends (
    date date,
    label date,
    adjDividend float,
    dividend float,
    recordDate date,
    paymentDate date,
    declarationDate date
);
"""
create_del = """
CREATE TABLE delistedCompanies (
    symbol varchar(255), 
    companyName varchar(255), 
    exchange varchar(255), 
    ipoDate date, 
    delistedDate date
);
"""
cursor.execute(create_his)
cursor.execute(create_del)
print("Complete: Create table on testDB database")
# Insert value to table
with open('historicalDividends.csv') as csv_file:
    csvfile = csv.reader(csv_file, delimiter=",")
    all_value_his = []
    for row in csvfile:
        for i in range(len(row)):
            if row[i] == '':
                row[i] = None
            value = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])
        all_value_his.append(value)
del all_value_his[0]

with open('delistedCompanies.csv') as csv_file:
    csvfile = csv.reader(csv_file, delimiter=",")
    all_value_del = []
    for row in csvfile:
        for i in range(len(row)):
            if row[i] == '':
                row[i] = None
            value = (row[0], row[1], row[2], row[3], row[4])
        all_value_del.append(value)
del all_value_del[0]

insert_his = """
    INSERT INTO historicalDividends (date, label, adjDividend, dividend, recordDate, paymentDate, declarationDate)
    VALUES (%s, STR_TO_DATE(%s, "%M %d, %y"), %s, %s, %s, %s, %s)
    """
insert_del = """
    INSERT INTO delistedCompanies (symbol, companyName, exchange, ipoDate, delistedDate)
    VALUES (%s, %s, %s, %s, %s)
    """

cursor.executemany(insert_his, all_value_his)
cursor.executemany(insert_del, all_value_del)
print("Complete: Insert value to table")
conn.commit()

conn.close()
print("Complete: All task Done!")