import helpers.trade_executor as trade_executor
import helpers.tradier as trade
import helpers.dynamo_helper as db
from helpers.helper import date_performance_check
import pandas as pd
from datetime import datetime, timedelta
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from yahooquery import Ticker
import boto3
import os
import logging

s3 = boto3.client('s3')
trading_mode = os.getenv('TRADING_MODE')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
urllib3.disable_warnings(category=InsecureRequestWarning)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def manage_portfolio(event, context):

    dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    current_date = datetime.now().strftime("%Y-%m-%d")

    order_side = "sell_to_close"
    order_type = "market"
    duration = "gtc"
    logger.info(f'Initializing new trades PM: {dt}')
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode)
    new_trades_df = pull_new_trades()
    open_trades_df = db.get_all_orders_from_dynamo()
    ## Future feature to deal with descrepancies between our records and tradier
    # if len(open_trades_df) > len(open_trades_list):
    # TO-DO create an alarm mechanism to report this 
    if len(open_trades_df) > 1:
        orders_to_close = evaluate_open_trades(open_trades_df, base_url, access_token)
        if len(orders_to_close) > 1:
            trade_response = trade_executor.close_orders(orders_to_close, base_url, account_id, access_token, trading_mode)
    trades_placed = evaluate_new_trades(new_trades_df, trading_mode)
    return trades_placed


def pull_new_trades():
    keys = s3.list_objects(Bucket=trading_data_bucket,Prefix='yqalerts_potential_trades/')["Contents"]
    key = keys[-1]['Key']
    dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
    df = pd.read_csv(dataset.get("Body"))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    return df


def evaluate_new_trades(new_trades_df, trading_mode):
    approved_trades_df = new_trades_df.loc[new_trades_df['classifier_prediction'] > .5]
    execution_result = trade_executor.run_executor(approved_trades_df, trading_mode)
    return execution_result


def get_open_trades(base_url, account_id, access_token):
    order_id_list = []
    open_trades_list = trade.get_account_positions(base_url, account_id, access_token)
    if open_trades_list == "No Positions":
        logger.info(f'Found no positions.')
        return None
    for open_trade in open_trades_list:
        order_id_list.append(open_trade['id'])

    open_trades_df = db.get_open_trades_by_orderid(order_id_list)
    return open_trades_df, order_id_list


def evaluate_open_trades(orders_df,base_url, access_token):
    df_unique = orders_df.drop_duplicates(subset='order_id', keep='first')
    positions_to_close = []
    for index, row in df_unique.iterrows():
        sell_code, reason = date_performance_check(row, base_url, access_token)
        if sell_code == 2:
            logger.info(f'Closing order {row["option_symbol"]}: {reason}')
            positions_to_close.append(row['position_id'])

    orders_to_close = orders_df.loc[orders_df['position_id'].isin(positions_to_close)]
    return orders_to_close


# def close_orders(orders_df, account_id, base_url, access_token):
#     position_ids = orders_df['position_id'].unique()
#     total_transactions = []
#     for index, row in orders_df.iterrows():
#         id, status_code, status, result = trade.position_exit(base_url, account_id, access_token, row['underlying_symbol'], row['contract'], order_side, row['quantity'], order_type, duration)
#         if status_code == 200:
#             transaction_id = f'{row["option_name"]}_{dt}'
#             transactions = row['transaction_ids']
#             transactions.append(transaction_id)
#             row_data = row.to_dict()
#             row_data['transaction_ids'] = transactions
#             row_data['closing_transaction'] = transaction_id
#             row_data['closing_order_id'] = id
#             total_transactions.append(row_data)
#     db_success = db.process_closed_orders(total_transactions, base_url, access_token, account_id, position_ids, trading_mode)
#     return db_success
    

# if __name__ == "__main__":
#     base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode)
#     open_trades_df = db.get_all_orders_from_dynamo()
#     for index, row in open_trades_df.iterrows():
#        order = trade.get_order_info(base_url, account_id, access_token, row['order_id'])
#        db.create_new_dynamo_record_order(order, row, row['position_id'], row['order_id'], row['underlying_purchase_price'], "PAPER")
