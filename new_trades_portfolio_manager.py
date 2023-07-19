import helpers.trade_executor as te
import helpers.tradier as trade
import helpers.dynamo_helper as db
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
user = os.getenv("USER")
table = os.getenv("TABLE")

def manage_portfolio(event, context):
    current_positions = event[-1]['open_positions']   
    dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    current_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f'Initializing new trades PM: {dt}')
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode, user)
    new_trades_df = pull_new_trades()
    ## Future feature to deal with descrepancies between our records and tradier
    # if len(open_trades_df) > len(open_trades_list):
    # TO-DO create an alarm mechanism to report this 
    trades_placed = evaluate_new_trades(new_trades_df, trading_mode, base_url, account_id, access_token, table, current_positions)
    return "success"

def manage_portfolio_inv(event, context):
    current_positions = event['Payload'][-1]['open_positions']
    print(current_positions)
    dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    current_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f'Initializing new trades PM: {dt}')
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode, user)
    new_trades_df = pull_new_trades_inv()
    ## Future feature to deal with descrepancies between our records and tradier
    # if len(open_trades_df) > len(open_trades_list):
    # TO-DO create an alarm mechanism to report this 
    trades_placed = evaluate_new_trades(new_trades_df, trading_mode, base_url, account_id, access_token, table, current_positions)
    return "success"


def pull_new_trades():
    keys = s3.list_objects(Bucket=trading_data_bucket,Prefix='yqalerts_potential_trades/')["Contents"]
    key = keys[-1]['Key']
    dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
    df = pd.read_csv(dataset.get("Body"))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    return df

def pull_new_trades_inv():
    trading_strategies = ["day_losers", "maP","vdiff_gainP","day_gainers", "most_actives","vdiff_gainC"]
    year = datetime.now().year
    keys = s3.list_objects(Bucket=trading_data_bucket,Prefix=f'day_gainers/invalerts_potential_trades/{year}')["Contents"]
    key = keys[-1]['Key']
    query_key = key.split("day_gainers/invalerts_potential_trades/")[1]

    trade_dfs = []
    for stratgey in trading_strategies:
        try:
            dataset = s3.get_object(Bucket="inv-alerts-trading-data", Key=f"{stratgey}/invalerts_potential_trades/{query_key}")
            df = pd.read_csv(dataset.get("Body"))
            df.dropna(inplace = True)
            df.reset_index(inplace= True, drop = True)
            trade_dfs.append(df)
        except:
            print(f"{stratgey}/invalerts_potential_trades/{query_key}")
    full_df = pd.concat(trade_dfs)
    return full_df


def evaluate_new_trades(new_trades_df, trading_mode, base_url, account_id, access_token, table, current_positons):
    approved_trades_df = new_trades_df.loc[new_trades_df['classifier_prediction'] > .5]
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
    

if __name__ == "__main__":
    trading_strategies = ["day_losers", "maP","vdiff_gainP","day_gainers", "most_actives","vdiff_gainC"]
    year = datetime.now().year
    keys = s3.list_objects(Bucket=trading_data_bucket,Prefix=f'invalerts_potential_trades/day_gainers/{year}')["Contents"]
    key = keys[-1]['Key']

    trade_dfs = []
    for strategy in trading_strategies:
        print(strategy)
        dataset = s3.get_object(Bucket=trading_data_bucket, Key=f"{strategy}/invalerts_potential_trades/{key}")
        df = pd.read_csv(dataset.get("Body"))
        df.dropna(inplace = True)
        df.reset_index(inplace= True, drop = True)
        trade_dfs.append(df)
    full_df = pd.concat(trade_dfs)
    print(full_df)
