import helpers.trade_executor as trade_executor
import helpers.tradier as trade
import helpers.dynamo_helper as db
import pandas as pd
from datetime import datetime, timedelta
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import boto3
import os

s3 = boto3.client('s3')
trading_mode = os.getenv('TRADING_MODE')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
urllib3.disable_warnings(category=InsecureRequestWarning)

d = datetime.datetime.now() #Today's date
current_date = d.strftime("%Y-%m-%d")

order_side = "sell_to_close"
order_type = "market"
duration = "gtc"


def manage_portfolio(event, context):
    access_token, base_url, account_id = trade.get_tradier_credentials()
    open_trades_df = get_open_trades(base_url, account_id, access_token)
    orders_to_close = evaluate_open_trades(open_trades_df, base_url, account_id, access_token)
    trade_response = close_orders(orders_to_close, base_url, account_id, access_token)


def pull_new_trades():
    keys = s3.list_objects(Bucket="yqalerts-potential-trades")["Contents"]
    key = keys[-1]['Key']
    dataset = s3.get_object(Bucket="yqalerts-potential-trades", Key=key)
    df = pd.read_csv(dataset.get("Body"))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    return df

def get_open_trades(base_url, account_id, access_token):
    order_id_list = []
    open_trades_list = trade.get_account_positions(base_url, account_id, access_token)

    for trade in open_trades_list:
        order_id_list.append(trade['open_order_id'])

    open_trades_df = db.get_open_trades_by_orderid(order_id_list)
    return open_trades_df


def pull_open_orders_df():
    keys = s3.list_objects(Bucket=trading_data_bucket, Prefix="open_orders_data/")["Contents"]
    query_key = keys[-1]['Key']
    data = s3.get_object(Bucket=trading_data_bucket, Key=query_key)
    df = pd.read_csv(data.get("Body"))
    return df

def evaluate_open_trades(orders_df,base_url, access_token):
    df_unique = orders_df.drop_duplicates(subset='order_id', keep='first')
    positions_to_close = []
    for index, row in df_unique.iterrows():
        close_order = date_performance_check(row, base_url, access_token)
        if close_order:
            positions_to_close.append(row['position_id'])

    orders_to_close = orders_df.loc[orders_df['position_id'].isin(positions_to_close)]
    return orders_to_close

def close_orders(orders_df, account_id, base_url, access_token):
    for index, row in orders_df.iterrows():
        id, status_code, status, result = trade.position_exit(base_url, account_id, access_token, row['underlying_symbol'], row['contract'], order_side, row['quantity'], order_type, duration)
        if status_code == 200:
            order_info_obj = trade.get_order_info(base_url, account_id, access_token, row['open_order_id'])
            order_info_obj['pm_data'] = row
            db.create_dynamo_record(order_info_obj)



def calculate_sellby_date(current_date, trading_days_to_add): #End date, n days later for the data set built to include just trading days, but doesnt filter holiday
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    return current_date
    
def date_performance_check(row, base_url, access_token):
    # date_delta = current_date - row['position_open_date']
    sellby_date = calculate_sellby_date(row['position_open_date'], 3)
    current_strike = trade.get_last_price(row['underlying_symbol'], base_url, access_token)
    price_delta = current_strike - row['purchase_strike']
    percent_change = int((price_delta / row['purchase_strike']) * 100)
    
    if percent_change >= 5 or percent_change <= -5 or current_date > sellby_date:
        # order_dict = {
        #     "contract": row['contract'],
        #     "underlying_symbol": row['underlying_symbol'],
        #     "quantity": row['quantity'], 
        # }
        # return True, order_dict
        return True
    else:
        return False, {}