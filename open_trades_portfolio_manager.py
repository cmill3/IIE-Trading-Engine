from datetime import datetime, time
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import boto3
import os
import logging
import helpers.trade_executor as te
import helpers.tradier as trade
import helpers.dynamo_helper as db
import pytz

s3 = boto3.client('s3')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
table = os.getenv('TABLE')
close_table = os.getenv("CLOSE_TABLE")
urllib3.disable_warnings(category=InsecureRequestWarning)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dt = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
current_date = datetime.now().strftime("%Y-%m-%d")
lambda_signifier = datetime.now().strftime("%Y%m%d+%H%M")

order_side = "sell_to_close"
order_type = "market"
duration = "gtc"

env = os.getenv("ENV")


def manage_portfolio(event, context):
    logger.info(f'Initializing open trades PM: {dt}')
    try:
        check_time()
    except ValueError as e:
        return "disallowed"
    
    base_url, account_id, access_token = trade.get_tradier_credentials(env)
    open_trades_df = db.get_all_orders_from_dynamo(table)

    if len(open_trades_df) == 0:
        {"lambda_signifier": lambda_signifier}
    open_trades_df['pos_id'] = open_trades_df['position_id'].apply(lambda x: f'{x.split("-")[0]}{x.split("-")[1]}')
    open_positions = open_trades_df['pos_id'].unique().tolist()
    closed_orders = evaluate_open_trades(open_trades_df)
    
    # if len(orders_to_close) == 0:
    #     return {"open_positions": open_positions}
    
    # if env == "DEV":
    #     return {"open_positions": open_positions}
    
    # trade_response = te.close_orders(orders_to_close, base_url, account_id, access_token, env, table, close_table)
    # logger.info(f'Closing orders: {trade_response}')

    # if datetime.now().minute < 10:
    #     open_trades_df = db.get_all_orders_from_dynamo(table)
    #     open_trades_df['pos_id'] = open_trades_df['position_id'].apply(lambda x: f'{x.split("-")[0]}{x.split("-")[1]}')
    #     open_positions = open_trades_df['pos_id'].unique().tolist()

    logger.info(f'closed_orders: {closed_orders['closing_order_id']}')
    return {"lambda_signifier": lambda_signifier}

def  evaluate_open_trades(orders_df):
    orders_to_close = []
    for _, row in orders_df.iterrows():
        order_data = te.date_performance_check(row,env,lambda_signifier)
        if order_data is not None:
            orders_to_close.append({"open_order_id":order_data['order_id'],"closing_order_id":order_data['closing_order_id']})
    # positions_to_close = list(set(positions_to_close))
    logger.info(f'orders_to_close: {orders_to_close}')
    return orders_to_close


def check_time():
    current_time = datetime.now().astimezone(pytz.timezone('US/Eastern'))
    hour = current_time.hour
    minute = current_time.minute
    
    if hour <= 9:
        if hour == 9 and minute > 45:
            return "The time is within the allowed window."
        else:
            raise ValueError("The current time is outside the allowed window!")
    elif hour > 16:
        raise ValueError("The current time is outside the allowed window!")
    
if __name__ == "__main__":
   manage_portfolio(None, None)