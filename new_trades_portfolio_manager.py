import helpers.trade_executor as te
import helpers.tradier as trade
import helpers.dynamo_helper as db
import helpers.helper as helpers
import pandas as pd
from datetime import datetime, timedelta, time
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import boto3
import os
import logging
import pytz
from helpers.constants import ACTIVE_STRATEGIES
import warnings
warnings.filterwarnings('ignore')

s3 = boto3.client('s3')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
urllib3.disable_warnings(category=InsecureRequestWarning)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
env = os.getenv("ENV")
table = os.getenv("TABLE")
now = datetime.now().astimezone(pytz.timezone('US/Eastern'))
dt = now.strftime("%Y-%m-%dT%H:%M:%S")
lambda_signifier = datetime.now().strftime("%Y%m%d+%H")



def manage_portfolio_inv(event, context):
    logger.info(f'Initializing open trades PM: {dt} for {lambda_signifier}')
    try:
        check_time()
    except ValueError as e:
        return "disallowed"

    year, month, day, hour = format_dates(now)

    new_trades_df = pull_new_trades_inv(year, month, day, hour)
    trades_placed = evaluate_new_trades(new_trades_df, table)
    
    logger.info(f'Placed trades: {trades_placed}')
    logger.info(f'Finished new trades PM: {lambda_signifier}')
    return {"lambda_signifier": lambda_signifier}


def store_signifier(signifier):
    s3.put_object(Bucket=trading_data_bucket, Key=f"lambda_signifiers/recent_signifier_new_trades.txt", Body=str(signifier).encode('utf-8'))

def pull_new_trades_inv(year, month, day, hour):
    trade_dfs = []
    for stratgey in ACTIVE_STRATEGIES:
        try:
            if env == "DEV":
                dataset = s3.get_object(Bucket="inv-alerts-trading-data", Key=f"invalerts_potential_trades/PROD_VAL/{stratgey}/{year}/{month}/{day}/{hour}.csv")
            dataset = s3.get_object(Bucket=trading_data_bucket, Key=f"invalerts_potential_trades/{env}/{stratgey}/{year}/{month}/{day}/{hour}.csv")
            df = pd.read_csv(dataset.get("Body"))
            df.dropna(subset=["trade_details2wk"],inplace=True)
            df.dropna(subset=["trade_details1wk"],inplace=True)
            df.reset_index(inplace= True, drop = True)
            trade_dfs.append(df)
        except Exception as e:
            print(e)
            print(f"invalerts_potential_trades/{env}/{stratgey}/{year}/{month}/{day}/{hour}.csv")
    full_df = pd.concat(trade_dfs)
    return full_df


def evaluate_new_trades(new_trades_df,table):
    approved_trades_df = new_trades_df.loc[new_trades_df['classifier_prediction'] > .5]
    execution_result = te.execute_new_trades(approved_trades_df, table,lambda_signifier)
    return execution_result


def get_open_trades():
    base_url, account_id, access_token = trade.get_tradier_credentials(env)
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
        sell_code, reason = te.date_performance_check(row)
        if sell_code == 2:
            logger.info(f'Closing order {row["option_symbol"]}: {reason}')
            positions_to_close.append(row['position_id'])

    orders_to_close = orders_df.loc[orders_df['position_id'].isin(positions_to_close)]
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

def format_dates(now):
    now_str = now.strftime("%Y-%m-%d-%H")
    year, month, day, hour = now_str.split("-")
    hour = int(hour)
    return year, month, day, hour
    

if __name__ == "__main__":
    # accepted_df = pd.read_csv("17_19.csv")
     # env = env
    # env = "DEV"
    # table = "icarus-orders-table-inv"
    manage_portfolio_inv(None, None)