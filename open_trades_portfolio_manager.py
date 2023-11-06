from datetime import datetime, time
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import boto3
import os
import logging
import helpers.trade_executor as te
import helpers.tradier as trade
import helpers.dynamo_helper as db

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
    logger.info(f'Initializing open trades PM: {dt}')
    try:
        check_time()
    except ValueError as e:
        return "disallowed"
    
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode,user)
    open_trades_df = db.get_all_orders_from_dynamo(table)
    open_trades_df['pos_id'] = open_trades_df['position_id'].apply(lambda x: f'{x.split("-")[0]}{x.split("-")[1]}')
    open_positions = open_trades_df['pos_id'].unique().tolist()

    orders_to_close = evaluate_open_trades(open_trades_df)
    
    if len(orders_to_close) == 0:
        return {"open_positions": open_positions}
    
    if trading_mode == "DEV":
        return {"open_positions": open_positions}
    
    trade_response = te.close_orders(orders_to_close, base_url, account_id, access_token, trading_mode, table, close_table)
    logger.info(f'Closing orders: {trade_response}')

    if datetime.now().minute < 10:
        open_trades_df = db.get_all_orders_from_dynamo(table)
        open_trades_df['pos_id'] = open_trades_df['position_id'].apply(lambda x: f'{x.split("-")[0]}{x.split("-")[1]}')
        open_positions = open_trades_df['pos_id'].unique().tolist()

    return {"open_positions": open_positions}

def evaluate_open_trades(orders_df):
    positions_to_close = []
    close_reasons = []
    for _, row in orders_df.iterrows():
        sell_code, reason = te.date_performance_check(row)
        if sell_code != 0:
            positions_to_close.append(row['position_id'])
            logger.info(f'Closing order {row["option_symbol"]}: {reason}')
            ### figure out how to add reason to the order
            close_reasons.append(reason)
    positions_to_close = list(set(positions_to_close))
    orders_to_close = orders_df.loc[orders_df['position_id'].isin(positions_to_close)]
    return orders_to_close

def check_time():
    current_utc_time = datetime.utcnow().time()
    
    if current_utc_time < time(13, 45) or current_utc_time > time(19, 55):
        raise ValueError("The current time is outside the allowed window!")
    return "The time is within the allowed window."
    
if __name__ == "__main__":
   manage_portfolio(None, None)