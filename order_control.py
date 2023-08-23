import boto3
import pandas as pd
import os
from helpers import helper, tradier, dynamo_helper as db
import logging
from datetime import datetime

bucket = os.getenv("TRADING_BUCKET")
trading_mode = os.getenv("TRADING_MODE")
user = os.getenv("USER")

ddb = boto3.resource('dynamodb','us-east-1')
orders_table = ddb.Table('icarus-orders-table')
closed_orders_table = ddb.Table('icarus-closed-orders-table')
s3 = boto3.client("s3")

logger = logging.getLogger()


def run_order_control(event, context):
    date_prefix = helper.calculate_date_prefix()
    base_url, account_id, access_token = tradier.get_tradier_credentials(trading_mode=trading_mode, user=user)
    closed_orders_df = helper.pull_data_s3(path='enriched_closed_orders_data',bucket=bucket,date_prefix=date_prefix)
    opened_orders_df = helper.pull_opened_data_s3(path='orders_data',bucket=bucket,date_prefix=date_prefix)
    tradier_orders = tradier.get_account_orders(base_url, account_id, access_token)
    tradier_df = pd.DataFrame.from_dict(tradier_orders)
    untracked_open_orders, untracked_closed_orders = process_orders_data(tradier_df, opened_orders_df, closed_orders_df)
    print(untracked_open_orders)
    print(untracked_closed_orders)
    if len(untracked_open_orders) > 0:
        helper.write_to_s3(untracked_open_orders, 'orders_data', bucket, date_prefix)
    if len(untracked_closed_orders) > 0:
        for order in untracked_closed_orders:
            order_info_obj = tradier.get_order_info(base_url, account_id, access_token, order)
            create_response, full_order_record = db.create_new_dynamo_record_closed_order_reconciliation(order_info_obj, trading_mode)
    # dynamo_open_trades_df = get_all_orders_from_dynamo(orders_table)
    # dynamo_closed_trades_df = get_all_orders_from_dynamo(closed_orders_table)

    # process_opened_data(opened_orders_df)


def process_orders_data(tradier_df, opened_orders_df, closed_orders_df):
    untracked_open_orders = []
    untracked_closed_orders = []
    closed_orders = process_closed_data(closed_orders_df)
    opened_orders = process_opened_data(opened_orders_df)
    print(tradier_df['reason_description'])
    for index, row in tradier_df.iterrows():
        if row['side'] == 'buy':
            if row['id'] in opened_orders:
                continue
            else:
                untracked_open_orders.append(row.to_dict())
        elif row['side'] == 'sell_to_close':
            if row['id'] in closed_orders:
                continue
            elif row['status'] == 'filled':
                continue
            else:
                try:
                    if 'Sell order cannot be placed unless you are' in row['reason_description']:
                        continue
                except Exception as e:
                    print(row)
                    print(e)
                untracked_closed_orders.append(row.to_dict())
    return untracked_open_orders, untracked_closed_orders

def process_opened_data(df):
    all_orders = []
    for order_list in all_orders:
        for order in order_list:
            all_orders.append(order)
    
    return all_orders

def process_closed_data(df):
    orders_list = df['closing_order_id']
    return orders_list

def get_all_orders_from_dynamo(table):
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    df = pd.DataFrame(data)
    return df

def open_orders_reconciliation(orders,base_url, account_id, access_token):
    untracked_info = []
    date_str = datetime.now().strftime("%Y/%m/%d")
    for order_id in orders:
        order_info_obj = tradier.get_order_info(base_url, account_id, access_token, order_id)
        db.create_new_dynamo_record_order_reconciliation(order_info_obj, trading_mode)
        logger.info(f"Error getting order info {order_id}: {e}")
        untracked_info.append(order_info_obj)
    
    df = pd.DataFrame.from_dict(untracked_info)
    csv = df.to_csv(index=False)

    s3.put_object(Body=csv, Bucket=bucket, Key=f"untracked_orders/{date_str}.csv")


if __name__ == "__main__":
    run_order_control(None, None)