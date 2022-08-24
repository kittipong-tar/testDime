# import libralies
import requests
import json
import pandas as pd
import mysql.connector
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, exc
from sqlalchemy.types import String, Date, DateTime, Float, VARCHAR

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
df_his = df_his.replace('', np.nan)
df_his["label"] = df_his["label"].apply(lambda d: datetime.strptime(d, '%B %d, %y').strftime('%B %d, %y'))
df_del = df_del.replace('', np.nan)
print("Complete: Convert json to DataFrame")

# Credentials to database connection
host = "localhost"
user = "root"
pw = "password1"
db = "DBPRD"

try:
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=host, db=db, user=user, pw=pw))
except exc.DatabaseError as e:
    print(e)
else:
    print("Complete: Connecting to database: {db}".format(db=db))

# Writing dataframt to MySql
schema_his = {
    "date": Date,
    "label": VARCHAR(255),
    "adjDividend": Float,
    "dividend": Float,
    "recordDate": Date,
    "paymentDate": Date,
    "declarationDate": Date
}
schema_del = {
    "symbol": VARCHAR(255), 
    "companyName": VARCHAR(255), 
    "exchange": VARCHAR(255), 
    "ipoDate": Date,
    "delistedDate": Date,
}

try:
    df_his.to_sql('historicalDividends', engine, index=False, if_exists='replace', dtype=schema_his)
    df_del.to_sql('delistedCompanies', engine, index=False, if_exists='replace', dtype=schema_del)
except exc.DataError as e:
    print(e)
else:
    print("Complete: Writing dataframe to MySqlDB")