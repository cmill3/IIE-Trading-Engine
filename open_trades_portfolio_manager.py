import helpers.trade_executor as trade_executor
import helpers.tradier as trade
import helpers.dynamo_helper as db
from helpers.helper import date_performance_check
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

dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
current_date = datetime.now().strftime("%Y-%m-%d")

order_side = "sell_to_close"
order_type = "market"
duration = "gtc"


def manage_portfolio(event, context):
    access_token, base_url, account_id = trade.get_tradier_credentials()
    open_trades_df = get_open_trades(base_url, account_id, access_token)
    orders_to_close = evaluate_open_trades(open_trades_df, base_url, account_id, access_token)
    trade_response = trade_executor.close_orders(orders_to_close, base_url, account_id, access_token, trading_mode)
    return trade_response

def get_open_trades(base_url, account_id, access_token):
    order_id_list = []
    open_trades_list = trade.get_account_positions(base_url, account_id, access_token)
    if open_trades_list == "No Positions":
        return None
    for open_trade in open_trades_list:
        order_id_list.append(open_trade['id'])

    open_trades_df = db.get_open_trades_by_orderid(order_id_list)
    return open_trades_df, order_id_list

def evaluate_open_trades(orders_df,base_url, access_token):
    df_unique = orders_df.drop_duplicates(subset='order_id', keep='first')
    positions_to_close = []
    for index, row in df_unique.iterrows():
        close_order = date_performance_check(row, base_url, access_token)
        if close_order:
            positions_to_close.append(row['position_id'])

    orders_to_close = orders_df.loc[orders_df['position_id'].isin(positions_to_close)]
    return orders_to_close


def create_positions_list(total_transactions):
    positions_dict = {}
    for position in total_transactions:
        for key, value in position.items():
            if key in positions_dict:
                positions_dict[key].extend(value)
            else:
                positions_dict[key] = value
    return positions_dict

    
if __name__ == "__main__":
    manage_portfolio(None, None)