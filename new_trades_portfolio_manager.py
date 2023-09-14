import helpers.trade_executor as te
import helpers.tradier as trade
import helpers.dynamo_helper as db
import helpers.helper as helpers
import pandas as pd
from datetime import datetime, timedelta, time
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
user = os.getenv("USER")
table = os.getenv("TABLE")
now = datetime.now()
dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def manage_portfolio_inv(event, context):
    current_positions = event['Payload'][-1]['open_positions']
    # current_positions = []
    try:
        check_time()
    except ValueError as e:
        return "disallowed"

    year, month, day, hour = format_dates(now)
    logger.info(f'Initializing new trades PM: {dt}')
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode, user)
    new_trades_df = pull_new_trades_inv(year, month, day, hour)
    ## Future feature to deal with descrepancies between our records and tradier
    # if len(open_trades_df) > len(open_trades_list):
    # TO-DO create an alarm mechanism to report this 
    trades_placed = evaluate_new_trades(new_trades_df, trading_mode, base_url, account_id, access_token, table, current_positions)
    return "success"


def pull_new_trades_inv(year, month, day, hour):
    trading_strategies = ["bfC","bfP",'indexC','indexP','bfC_1d','bfP_1d']
    trade_dfs = []
    for stratgey in trading_strategies:
        try:
            print(f"invalerts_potential_trades/{stratgey}/{year}/{month}/{day}/{hour}.csv")
            dataset = s3.get_object(Bucket="inv-alerts-trading-data", Key=f"invalerts_potential_trades/{stratgey}/{year}/{month}/{day}/{hour}.csv")
            df = pd.read_csv(dataset.get("Body"))
            df.dropna(subset=["trade_details2wk"],inplace=True)
            df.dropna(subset=["trade_details1wk"],inplace=True)
            df.reset_index(inplace= True, drop = True)
            trade_dfs.append(df)
        except Exception as e:
            print(e)
            print(f"invalerts_potential_trades/{stratgey}/{year}/{month}/{day}/{hour}.csv")
    full_df = pd.concat(trade_dfs)
    return full_df


def evaluate_new_trades(new_trades_df, trading_mode, base_url, account_id, access_token, table, current_positons):
    approved_trades_df = new_trades_df.loc[new_trades_df['classifier_prediction'] > .5]
    if trading_mode == "DEV":
        return "test execution"
    execution_result = te.run_executor(approved_trades_df, trading_mode, base_url, account_id, access_token, table, current_positons)
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
        sell_code, reason = te.date_performance_check(row, base_url, access_token)
        if sell_code == 2:
            logger.info(f'Closing order {row["option_symbol"]}: {reason}')
            positions_to_close.append(row['position_id'])

    orders_to_close = orders_df.loc[orders_df['position_id'].isin(positions_to_close)]
    return orders_to_close


def check_time():
    current_utc_time = datetime.utcnow().time()
    
    if current_utc_time < time(14, 0) or current_utc_time > time(19, 55):
        raise ValueError("The current time is outside the allowed window!")
    return "The time is within the allowed window."

def format_dates(now):
    now_str = now.strftime("%Y-%m-%d-%H")
    year, month, day, hour = now_str.split("-")
    hour = int(hour) - 4
    return year, month, day, hour
    

# if __name__ == "__main__":
#     accepted_df = pd.read_csv("17_19.csv")
#     user="inv"
#     trading_mode = "PAPER"
#     table = "icarus-orders-table-inv"
#     base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode, user)
#     pending_orders = te.process_dynamo_orders(accepted_df, base_url, account_id, access_token, trading_mode, table)