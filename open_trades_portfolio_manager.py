import helpers.trade_executor as te
import helpers.tradier as trade
import helpers.dynamo_helper as db
import pandas as pd
from datetime import datetime, timedelta, time
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import boto3
import os
import logging

s3 = boto3.client('s3')
trading_mode = os.getenv('TRADING_MODE')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
table = os.getenv('TABLE')
close_table = os.getenv("CLOSE_TABLE")
urllib3.disable_warnings(category=InsecureRequestWarning)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dt = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
current_date = datetime.now().strftime("%Y-%m-%d")

order_side = "sell_to_close"
order_type = "market"
duration = "gtc"

user = os.getenv("USER")


def manage_portfolio(event, context):
    try:
        check_time()
    except ValueError as e:
        return "disallowed"
    
    logger.info(f'Initializing open trades PM: {dt}')
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode,user)
    open_trades_df = db.get_all_orders_from_dynamo(table)
    open_trades_df['pos_id'] = open_trades_df['position_id'].apply(lambda x: f'{x.split("-")[0]}{x.split("-")[1]}')
    open_positions = open_trades_df['pos_id'].unique().tolist()
    orders_to_close = evaluate_open_trades(open_trades_df, base_url, account_id, access_token)

    try:
        # open_trades_df['pos_id'] = open_trades_df['position_id'].apply(lambda x: f'{x.split("-")[0]}{x.split("-")[1]}')
        # open_positions = open_trades_df['pos_id'].unique().tolist()
        orders_to_close = evaluate_open_trades(open_trades_df, base_url, account_id, access_token)
    except Exception as e:
        print(e)
        print("no trades to close")
        return {"open_positions": open_positions}
    if len(orders_to_close) == 0:
        return {"open_positions": open_positions}
    
    if trading_mode == "DEV":
        return {"open_positions": open_positions}
    trade_response = te.close_orders(orders_to_close, base_url, account_id, access_token, trading_mode, table, close_table)
    return {"open_positions": open_positions}

def evaluate_open_trades(orders_df, base_url, account_id, access_token):
    df_unique = orders_df.drop_duplicates(subset='position_id', keep='first')
    positions_to_close = []
    close_reasons = []
    for index, row in df_unique.iterrows():
        sell_code, reason = te.date_performance_check(row)
        if sell_code == 2:
            positions_to_close.append(row['position_id'])
            logger.info(f'Closing order {row["option_symbol"]}: {reason}')
            ### figure out how to add reason to the order
            close_reasons.append(reason)
    orders_to_close = orders_df.loc[orders_df['position_id'].isin(positions_to_close)]
    return orders_to_close

def check_time():
    current_utc_time = datetime.utcnow().time()
    
    if current_utc_time < time(13, 45) or current_utc_time > time(19, 55):
        raise ValueError("The current time is outside the allowed window!")
    return "The time is within the allowed window."
    
if __name__ == "__main__":
   manage_portfolio(None, None)