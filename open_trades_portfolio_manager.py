import helpers.trade_executor as te
import helpers.tradier as trade
import helpers.dynamo_helper as db
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

base_url = os.getenv("BASE_URL")
account_id = os.getenv("ACCOUNT_ID")
access_token = os.getenv("ACCESS_TOKEN")


def manage_portfolio(event, context):
    logger.info(f'Initializing open trades PM: {dt}')
    open_trades_df = db.get_all_orders_from_dynamo()
    open_trades_df['pos_id'] = open_trades_df['position_id'].apply(lambda x: x.split("-")[0] + x.split("-")[1])
    open_positions = open_trades_df['pos_id'].unique().tolist()
    try:
        orders_to_close = evaluate_open_trades(open_trades_df, base_url, account_id, access_token)
    except Exception as e:
        return "Orders to Close failed"
    if len(orders_to_close) == 0:
        return {"message": "No open trades to close"}
    # trade_response = te.close_orders(orders_to_close, base_url, account_id, access_token, trading_mode)
    return open_positions

def evaluate_open_trades(orders_df, base_url, account_id, access_token):
    df_unique = orders_df.drop_duplicates(subset='position_id', keep='first')
    positions_to_close = []
    close_reasons = []
    for index, row in df_unique.iterrows():
        sell_code, reason = te.date_performance_check(row, base_url, access_token)
        if sell_code == 2:
            positions_to_close.append(row['position_id'])
            logger.info(f'Closing order {row["option_symbol"]}: {reason}')
            ### figure out how to add reason to the order
            close_reasons.append(reason)
    orders_to_close = orders_df.loc[orders_df['position_id'].isin(positions_to_close)]
    return orders_to_close

    
if __name__ == "__main__":
   manage_portfolio(None, None)