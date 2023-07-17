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
    # formatted_df = format_pending_df(trades_df)
    if len(trades_df) > 0:
        still_pending  = process_dynamo_orders(trades_df, base_url, account_id, access_token)
        pending_df = pd.DataFrame.from_dict(still_pending)
    if len(pending_df) > 0:
        pending_csv = pending_df.to_csv()
        response = s3.put_object(Bucket=trading_data_bucket, Key=f"pending_orders_enriched/{date}.csv", Body=pending_csv)
        return response
    return "No pending orders"



def pull_pending_trades():
    keys = s3.list_objects(Bucket=trading_data_bucket,Prefix='pending_orders_enriched/2023')["Contents"]
    pending_keys = keys[-2:-1]['Key']
    dfs = []
    for key in pending_keys:
        dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
        df = pd.read_csv(dataset.get("Body"))
        dfs.append(df)
    pending_df = pd.concat(dfs)
    return pending_df

def process_dynamo_orders(trades_df, base_url, account_id, access_token, trading_mode):
    # for index, row in trades_df.iterrows():
    #     unfilled_order = db.process_opened_orders(row, row['position_id'],base_url, account_id, access_token, trading_mode)
    unfulfilled_orders = db.process_opened_ordersv2(trades_df, base_url, account_id, access_token, trading_mode)
    return unfulfilled_orders

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

# def run_reconciliation(event, context):
#     base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode)
#     trades_df = pull_pending_trades()
#     formatted_df = format_pending_df(trades_df)
#     still_pending  = process_dynamo_orders(formatted_df, base_url, account_id, access_token, trading_mode)
#     # if len(trades_df) > 0:
#     #     still_pending  = process_dynamo_orders(trades_df, base_url, account_id, access_token)
#     #     pending_df = pd.DataFrame.from_dict(still_pending)
#     # if len(pending_df) > 0:
#     #     pending_csv = pending_df.to_csv()
#     #     response = s3.put_object(Bucket=trading_data_bucket, Key=f"pending_orders_enriched/{date}.csv", Body=pending_csv)
#     #     return response
#     return "No pending orders"



# def pull_pending_trades():
#     keys = s3.list_objects(Bucket=trading_data_bucket,Prefix='pending_orders/2023')["Contents"]
#     # pending_keys = keys[-2:-1]['Key']
#     key = keys[-1]['Key']
#     dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
#     pending_df = pd.read_csv(dataset.get("Body"))
    
#     # dfs = []
#     # for key in pending_keys:
#     #     dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
#     #     df = pd.read_csv(dataset.get("Body"))
#     #     dfs.append(df)
#     # pending_df = pd.concat(dfs)
#     return pending_df

# def process_dynamo_orders(trades_df, base_url, account_id, access_token, trading_mode):
#     for index, row in trades_df.iterrows():
#         unfilled_order = db.process_opened_orders(row, row['position_id'],base_url, account_id, access_token, trading_mode)
#     # unfulfilled_orders = db.process_opened_ordersv2(trades_df, base_url, account_id, access_token, trading_mode)
#     return unfilled_order

# def format_pending_df(df):
#     columns = df['Unnamed: 0'].values
#     indexes = df.columns.values[1:]

#     unpacked_data = []
#     for index in indexes:
#         data = df[index].values
#         unpacked_data.append(data)
    
#     formatted_df = pd.DataFrame(unpacked_data,indexes,columns)
#     return formatted_df

# if __name__ == "__main__":
#     run_reconciliation(None, None)