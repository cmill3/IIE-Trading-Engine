import dynamo_helper as db
import tradier as trade
import boto3
import pandas as pd
import os
from datetime import datetime

s3 = boto3.client('s3')

date = datetime.now().strftime("%Y-%m-%d/%H_%M")


trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
trading_mode = os.getenv('TRADING_MODE')

def run_reconciliation(event, context):
    base_url, access_token, account_id = trade.get_tradier_credentials(trading_mode)
    trades_df = pull_pending_trades()
    formatted_df = format_pending_df(trades_df)
    still_pending = run_reconciliation(formatted_df, base_url, access_token, account_id)
    if len(still_pending) > 0:
        csv = still_pending.to_csv()
        s3.put_object(Bucket=trading_data_bucket, Key=f"pending_orders/{date}.csv", Body=csv)
    return "Reconciliation Complete"



def pull_pending_trades():
    keys = s3.list_objects(Bucket=trading_data_bucket,Prefix='yqalerts_pending_trades/')["Contents"]
    key = keys[-1]['Key']
    dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
    df = pd.read_csv(dataset.get("Body"))
    return df

def run_reconciliation(trades_df, base_url, access_token, account_id):
    completed_trades = []
    for index, row in trades_df.iterrows():
        db_success = db.process_opened_orders(row, index, base_url, access_token, account_id,trading_mode)
        if db_success:
            completed_trades.append(index)
    pending_df = trades_df.drop(completed_trades)
    return pending_df

def format_pending_df(df):
    columns = df['Unnamed: 0'].values[2:]
    indexes = df.columns.values[1:]

    unpacked_data = []
    for index in indexes:
        data = df[index].values
        unpacked_data.append(data[2:])
    
    formatted_df = pd.DataFrame(unpacked_data,indexes,columns)
    return formatted_df


if __name__ == "__main__":
    run_reconciliation(None, None)