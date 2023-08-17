import helpers.dynamo_helper as db
import helpers.tradier as trade
import helpers.helper as helper
import boto3
import pandas as pd
import numpy as np
import os
from datetime import datetime
import re

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
    exposure_totalling()
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

def exposure_totalling():
    # base_url = "https://sandbox.tradier.com/v1/"
    # account_id = "VA72174659"
    # access_token = "ld0Mx4KbsOBYwmJApdowZdFcIxO7"
    #pull all open contract symbols & contract values
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode, user)
    position_list = trade.get_account_positions(base_url, account_id, access_token)
    # total open trades & values in df
    df = pd.DataFrame.from_dict(position_list)
    df['underlying_symbol'] = df['symbol'].apply(lambda symbol: helper.pull_symbol(symbol))
    agg_functions = {'cost_basis': ['sum', np.mean], 'quantity': 'sum'}
    df_new = df.groupby(df['underlying_symbol']).aggregate(agg_functions)
    # export df as csv --> AWS S3
    year, month, day, hour = helper.date_and_time()
    bucket = trading_data_bucket
    df_csv = df_new.to_csv()
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, f'positions_exposure/{year}/{month}/{day}/{hour}.csv').put(Body=df_csv.getvalue())
    return "Exposure Analysis Complete"

if __name__ == "__main__":
    run_reconciliation(None, None)
