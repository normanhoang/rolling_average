# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 17:08:24 2018

@author: nhoang
"""

import pandas as pd
from pandas_datareader import data as pdr
import fix_yahoo_finance as yf
import pyodbc
import sqlalchemy

yf.pdr_override()

stock_list = pd.read_excel("symbol.xlsx")
stocks = stock_list['Ticker '].tolist()

# stocks are listed with "-" instead of "." This replaces with the "-" for the stocks
stocks = [s.replace('\x2e', '-') for s in stocks]

start_date = "2018-09-01"
end_date = "2018-10-08"

# gets a test sample to use as index
test = pdr.get_data_yahoo("AAN", start=start_date, end=end_date)
df1 = pd.DataFrame(index = test.index)

stocks_success = []
stocks_error = []

# Takes about 15 minutes to run. 
# keeps running until it does the number of stock_error remains unchanged, no more gets downloaded
while True:
    for i in stocks:
        try:
            data = pdr.get_data_yahoo(i, start=start_date, end=end_date)
            df1 = df1.join(data['Adj Close'], rsuffix="_"+i)
            stocks_success.append(i)
            print(i, "success")
        except Exception as e:
            print("error with", i, e)
            stocks_error.append(i)
    if len(stocks) == len(stocks_error):
        print("end: ",len(stocks))
        break
    else:
        print("stocks length: ", len(stocks))
        print("stocks error length: ", len(stocks_error))
        stocks = stocks_error
        stocks_error = []
        
 
#renames columns       
df1.columns = stocks_success
#transposes matrix, as 1024 is the max number of columns
df1_T = df1.T
df1_T.index.name = 'stock'
df1_T = df1_T.reset_index()

#writes raw price data to SQL table named 'stock_raw_data'
engine = sqlalchemy.create_engine("mssql://.\SQLEXPRESS/project?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes")
df1_T.to_sql('stock_raw_data', con=engine, if_exists='replace', index=False)


#reads the sql table to perform calculations
df2_T = pd.read_sql_table('stock_raw_data', con=engine, index_col='stock', coerce_float=True)

#finds return and drops the first row.
df2_return = df2_T.pct_change(periods = 1, axis='columns')
df2_return = df2_return.drop(df2_return.columns[[0]], axis=1)

#calculates rolling return (rolling average of 5 days, minimum period of 1 day)
df2_rollmean = df2_return.rolling(window = 5, min_periods = 1, axis = 1).mean()
df2_rollmean = df2_rollmean.reset_index()


#writes the new table into SQL table
df2_rollmean.to_sql('stock_rolling_average', con=engine, if_exists='replace', index=False)

df_read = pd.read_sql_query('SELECT * from stock_rolling_average', con= engine, index_col='stock', coerce_float=True)
print(df_read.head())

# saves the stock errors to a text file
with open('error.txt', 'w') as f:
    for item in stocks_error:
        f.write("%s\n" % item)