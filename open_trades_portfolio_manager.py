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
import logging

s3 = boto3.client('s3')
trading_mode = os.getenv('TRADING_MODE')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
urllib3.disable_warnings(category=InsecureRequestWarning)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dt = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
current_date = datetime.now().strftime("%Y-%m-%d")

order_side = "sell_to_close"
order_type = "market"
duration = "gtc"


def manage_portfolio(event, context):
    logger.info(f'Initializing new trades PM: {dt}')
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode)
    open_trades_df = db.get_all_orders_from_dynamo()
    orders_to_close = evaluate_open_trades(open_trades_df, base_url, account_id, access_token)
    if len(orders_to_close) == 0:
        return {"message": "No open trades to close"}
    trade_response = trade_executor.close_orders(orders_to_close, base_url, account_id, access_token, trading_mode)
    return trade_response

# def get_open_trades(base_url, account_id, access_token):
#     keys = s3.list_objects(Bucket=trading_data_bucket,Prefix='currently_open_orders')["Contents"]
#     key = keys[-1]['Key']
#     dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
#     open_trades = pd.read_csv(dataset.get("Body"))
#     order_id_list = open_trades['order_id'].tolist()
#     # order_id_list = []
#     # open_trades_list = trade.get_account_positions(base_url, account_id, access_token)
#     # if open_trades_list == "No Positions":
#     #     return None
#     # for open_trade in open_trades_list:
#     #     order_id_list.append(open_trade['id'])

#     open_trades_df = db.get_open_trades_by_orderid(order_id_list)
#     return open_trades_df, order_id_list

def evaluate_open_trades(orders_df, base_url, account_id, access_token):
    df_unique = orders_df.drop_duplicates(subset='order_id', keep='first')
    positions_to_close = []
    close_reasons = []
    for index, row in df_unique.iterrows():
        close_order, reason = date_performance_check(row, base_url, access_token)
        if close_order:
            positions_to_close.append(row['position_id'])
            ### figure out how to add reason to the order
            close_reasons.append(reason)

    orders_to_close = orders_df.loc[orders_df['position_id'].isin(positions_to_close)]
    return orders_to_close

    
if __name__ == "__main__":
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode)
    orders_to_close = pd.read_csv('/Users/charlesmiller/Code/PycharmProjects/FFACAP/Icarus/icarus_production/icarus-trading-engine/test/closed_orders.csv')
    position_ids = ""
    db_success = db.process_closed_orders(orders_to_close, base_url, account_id, access_token, position_ids, trading_mode)
#     # base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode)
#     # x = trade.get_account_positions(base_url, account_id, access_token)
#     # print(x)
#     # print(y)