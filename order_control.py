import boto3
import pandas as pd
import os
from helpers import helper, tradier, dynamo_helper as db
import logging
from datetime import datetime

bucket = os.getenv("TRADING_BUCKET")
env = os.getenv("ENV")
orders_table = os.getenv("ORDERS_TABLE")

ddb = boto3.resource('dynamodb','us-east-1')
s3 = boto3.client("s3")

logger = logging.getLogger()


def run_order_control(event, context):
    date_prefix = helper.calculate_date_prefix()
    base_url, account_id, access_token = tradier.get_tradier_credentials(env=env)
    dynamo_orders_df = db.get_all_orders_from_dynamo(orders_table)
    tradier_orders = tradier.get_account_positions(base_url, account_id, access_token)

    ddb_symbol_count = dynamo_orders_df.groupby('option_symbol').size().reset_index(name='count')
    tradier_df = pd.DataFrame.from_dict(tradier_orders)
    print(tradier_df)
    print(ddb_symbol_count)
    tradier_df = tradier_df[['symbol','quantity']]
    tradier_df.rename(columns={'symbol':'option_symbol'}, inplace=True)
    tradier_df['quantity'] = tradier_df['quantity'].astype(int)
    ddb_symbol_count['count'] = ddb_symbol_count['count'].astype(int)
    
    mismatched_symbols = compare_dataframes(tradier_df, ddb_symbol_count)
    if len(mismatched_symbols) > 0:
        logger.info(f"Mismatched symbols: {mismatched_symbols}")
        raise ValueError(f"Mismatched symbols: {mismatched_symbols}")
    

def compare_dataframes(tradier_df, ddb_symbol_count):
    mismatched_symbols = {}
    
    for index, row in tradier_df.iterrows():
        symbol = row['option_symbol']
        quantity_tradier = row['quantity']
        
        if symbol in ddb_symbol_count['option_symbol'].values:
            quantity_ddb = ddb_symbol_count.loc[ddb_symbol_count['option_symbol'] == symbol, 'count'].values[0]
            
            if quantity_tradier != quantity_ddb:
                mismatched_symbols[symbol] = quantity_tradier - quantity_ddb
        else:
            mismatched_symbols[symbol] = quantity_tradier
    
    return mismatched_symbols


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
        db.create_new_dynamo_record_order_reconciliation(order_info_obj, env)
        logger.info(f"Error getting order info {order_id}: {e}")
        untracked_info.append(order_info_obj)
    
    df = pd.DataFrame.from_dict(untracked_info)
    csv = df.to_csv(index=False)

    s3.put_object(Body=csv, Bucket=bucket, Key=f"untracked_orders/{date_str}.csv")


if __name__ == "__main__":
    run_order_control(None, None)