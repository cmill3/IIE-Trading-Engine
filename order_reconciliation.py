import helpers.dynamo_helper as db
import helpers.tradier as trade
import boto3
import pandas as pd
import os
from datetime import datetime

s3 = boto3.client('s3')

date = datetime.now().strftime("%Y/%m/%d/%H_%M")


trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
trading_mode = os.getenv('TRADING_MODE')

def run_reconciliation(event, context):
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode)
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

if __name__ == "__main__":
    run_reconciliation(None, None)