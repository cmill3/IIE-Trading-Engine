import helpers.dynamo_helper as db
import helpers.tradier as trade
import helpers.helper as helper
import boto3
import pandas as pd
import numpy as np
import os
from datetime import datetime
import re
from io import StringIO

s3 = boto3.client('s3')

date = datetime.now().strftime("%Y/%m/%d/%H_%M")


trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
trading_mode = os.getenv('TRADING_MODE')
user = os.getenv('USER')

def run_reconciliation(event, context):
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode, user)
    trades_df = pull_pending_trades()
    formatted_df = format_pending_df(trades_df)
    still_pending  = process_dynamo_orders(formatted_df, base_url, account_id, access_token)
    return "Reconciliation Complete"



def pull_pending_trades():
    keys = s3.list_objects(Bucket=trading_data_bucket,Prefix='pending_orders/2023')["Contents"]
    key = keys[-1]['Key']
    dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
    new_trades_df = pd.read_csv(dataset.get("Body"))
    return new_trades_df

def process_dynamo_orders(formatted_df, base_url, account_id, access_token):
    completed_trades = []
    for index, row in formatted_df.iterrows():
        fulfilled_orders, unfulfilled_orders = db.process_opened_orders(row, index, base_url, account_id, access_token, trading_mode)
        if len(unfulfilled_orders) == 0:
            completed_trades.append(index)
    # response = update_currently_open_orders(fulfilled_orders, [])
    pending_df = formatted_df.drop(completed_trades)
    print(pending_df)
    return pending_df

def format_pending_df(df):
    columns = df['Unnamed: 0'].values
    indexes = df.columns.values[1:]

    unpacked_data = []
    for index in indexes:
        data = df[index].values
        unpacked_data.append(data)
    
    formatted_df = pd.DataFrame(unpacked_data,indexes,columns)
    return formatted_df

def exposure_totalling(trading_mode, user):
    # base_url = "https://sandbox.tradier.com/v1/"
    # account_id = "VA72174659"
    # access_token = "ld0Mx4KbsOBYwmJApdowZdFcIxO7"
    #pull all open contract symbols & contract values
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode, user)
    position_list = trade.get_account_positions(base_url, account_id, access_token)
    # total open trades & values in df
    df = pd.DataFrame.from_dict(position_list)
    df['sym'] = df['symbol'].apply(lambda symbol: helper.pull_sym(symbol))
    agg_functions = {'cost_basis': ['sum', np.mean], 'quantity': 'sum'}
    df_new = df.groupby(df['sym']).aggregate(agg_functions)
    # export df as csv --> AWS S3
    year, month, day, hour = helper.dateandtime()
    bucket = 'position-exposure-tracking'
    csv_buffer = StringIO()
    df_new.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, f'{user}/{year}/{month}/{day}/{hour}/exposure.csv').put(Body=csv_buffer.getvalue())
    return "Exposure Analysis Complete"

if __name__ == "__main__":
    run_reconciliation(None, None)
    exposure_totalling(trading_mode, user)
