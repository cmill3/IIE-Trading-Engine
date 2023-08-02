import requests
import json
import os
import pandas as pd
import numpy as np
import helpers.tradier as trade
import re
import boto3
from io import StringIO
import datetime
from datetime import date

trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
trading_mode = os.getenv('TRADING_MODE')
user = os.getenv('USER')


def pull_sym(symbol):
    sym = " ".join(re.findall("[a-zA-Z]+", symbol))
    final_symbol = sym[:-1]
    return final_symbol
    
def dateandtime():
    year = date.today().year
    month = date.today().month
    day = date.today().day
    hour = datetime.datetime.now().hour
    return year, month, day, hour

def exposure_totalling(trading_mode):
    #pull all open contract symbols & contract values
    base_url = "https://sandbox.tradier.com/v1/"
    account_id = "VA72174659"
    access_token = "ld0Mx4KbsOBYwmJApdowZdFcIxO7"
    # base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode, user)
    position_list = trade.get_account_positions(base_url, account_id, access_token)
    #total open trades & values in df
    df = pd.DataFrame.from_dict(position_list)
    df['sym'] = df['symbol'].apply(lambda symbol: pull_sym(symbol))
    agg_functions = {'cost_basis': ['sum', np.mean], 'quantity': 'sum'}
    df_new = df.groupby(df['sym']).aggregate(agg_functions)
    # export df as csv --> AWS S3
    year, month, day, hour = dateandtime()
    bucket = 'position-exposure-tracking' # already created on S3
    csv_buffer = StringIO()
    df_new.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, f'{year}/{month}/{day}/{hour}/exposure.csv').put(Body=csv_buffer.getvalue())
    return df_new


df, df_new = exposure_totalling(trading_mode)

df_new.to_csv('/Users/ogdiz/Downloads/exposure_tracking.csv')
print(df)
print(df_new)
print(trading_data_bucket)