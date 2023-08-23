# Purpose: This script is used to build trades for the Icarus Trading Bot
import pandas as pd
from datetime import datetime, timedelta
from yahooquery import Ticker
from helpers import strategy_helper, trading_algorithms, tradier
import boto3
import os
import logging
import requests
import json

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
user = os.getenv("USER")
trading_mode = os.getenv('TRADING_MODE')

trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
model_results_bucket = os.getenv('MODEL_RESULTS_BUCKET')
title = os.getenv("TITLE")
trading_strategy = os.getenv("TRADING_STRATEGY")

prefixes = {
    "most_actives": "invalerts-xgb-MA-classifier",
    "maP":"invalerts-xgb-MAP-classifier",
    "day_gainers":"invalerts-xgb-gainers-classifier",
    "day_losers":"invalerts-xgb-losers-classifier",
    "vdiff_gainC":"invalerts-xgb-vdiff-gainC-classifier",
    "vdiff_gainP":"invalerts-xgb-vdiff-gainP-classifier"
}

now = datetime.now()
d = now.date() # Monday


def build_trade(event, context):
    logger.info('build_trade function started.')
    df, key = pull_data()
    logger.info(f'pulled key: {key}')
    results_df = process_data(df)
    # csv_buffer = results_df.to_csv("/Users/charlesmiller/Code/PycharmProjects/FFACAP/Icarus/icarus_production/icarus-trading-engine/test.csv")
    csv = results_df.to_csv()
    key = key.replace("yqalerts_full_results", "yqalerts_potential_trades")
    s3.put_object(Body=csv, Bucket=trading_data_bucket, Key=f"yqalerts_potential_trades/{now.year}/{now.month}/{now.day}/{now.hour}.csv")
    return {
        'statusCode': 200
    }

def build_trade_inv(event, context):
    logger.info('build_trade function started.')
    df, key = pull_data_inv()
    logger.info(f'pulled key: {key}')
    df['strategy'] = trading_strategy
    results_df = process_data(df)
    csv = results_df.to_csv()
    key = key.replace("inv-alerts-full-results", "invalerts_potential_trades")
    fix_key = key.split(".csv.csv")[0]
    response = s3.put_object(Body=csv, Bucket=trading_data_bucket, Key=f"invalerts_potential_trades/{trading_strategy}/{now.year}/{now.month}/{now.day}/{now.hour}.csv")
    print(response)
    return {
        'statusCode': 200
    }

#Non-Explanatory Variables Explained Below:
#CP = Call/Put (used to represent the Call/Put Trend Value)
#Sym = Symbol (used to repesent the symbol of the value that we are analyzing)

def pull_data():
    strategies = ['gainers','losers','ma','maP']
    dfs = []
    for strategy in strategies:
        keys = s3.list_objects(Bucket=model_results_bucket,Prefix=f"{strategy}/full_model_results/3d/classifier/yqalert_{strategy}")["Contents"]
        key = keys[-1]['Key']
        dataset = s3.get_object(Bucket=model_results_bucket, Key=key)
        df = pd.read_csv(dataset.get("Body"))
        df.dropna(inplace = True)
        df.reset_index(inplace= True, drop = True)
        if strategy == "gainers":
            df['strategy'] = 'day_gainers'
        elif strategy == "losers":
            df['strategy'] = 'day_losers'
        elif strategy == "ma":
            df['strategy'] = 'most_actives'
        elif strategy == "maP":
            df['strategy'] = 'maP'
        dfs.append(df)
    full_df = pd.concat(dfs)
    return full_df, key

def pull_data_inv():
    prefix_root = prefixes[trading_strategy]
    keys = s3.list_objects(Bucket=trading_data_bucket,Prefix=f"classifier_predictions/{prefix_root}/{title}")["Contents"]
    key = keys[-1]['Key']
    dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
    df = pd.read_csv(dataset.get("Body"))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    return df, key


def process_data(df):
    df['Call/Put'] = df['strategy'].apply(lambda strategy: infer_CP(strategy))
    df['expiry_1wk'] = Date_1wk()
    df['expiry_2wk'] = Date_2wk()
    result_df1 = df.apply(lambda row: build_trade_structure_1wk(row), axis=1,result_type='expand')
    result_df2 = df.apply(lambda row: build_trade_structure_2wk(row), axis=1,result_type='expand')
    df[['trade_details1wk', 'vol_check1wk']] = pd.DataFrame(result_df1, index=df.index)
    df[['trade_details2wk', 'vol_check2wk']] = pd.DataFrame(result_df2, index=df.index)
    df['sector'] = df['Sym'].apply(lambda Sym: strategy_helper.match_sector(Sym))
    df['sellby_date'] = calculate_sellby_date(d, 3)
    logger.info(f"Data processed successfully: {d}")
    print(df)
    return df

def infer_CP(strategy):
    call_strategies = ["day_gainers", "most_actives","vdiff_gainC"]
    put_strategies = ["day_losers", "maP","vdiff_gainP"]
    if strategy in call_strategies:
        return "call"
    elif strategy in put_strategies:
        return "put"
    

def calculate_sellby_date(current_date, trading_days_to_add): #End date, n days later for the data set built to include just trading days, but doesnt filter holiday
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    return current_date

def build_trade_structure_1wk(row):
    base_url, account_id, access_token = tradier.get_tradier_credentials(trading_mode, user)
    underlying_price = tradier.get_last_price(base_url, access_token, row['symbol'])
    try:
        option_chain = get_option_chain(row['symbol'], row['expiry_1wk'], row['Call/Put'])
        # if row['strategy'] == 'day_losers':
        #     if len(option_chain) < 12:
        #         contracts = None
        #         return contracts
        # else:
        #     if len(option_chain) < 20:  
        #         contracts = None
        #         return contracts
        contracts_1wk = strategy_helper.build_spread(option_chain, 3, row['Call/Put'], underlying_price)
        trade_details_1wk, vol_check = trading_algorithms.bet_sizer(contracts_1wk, now, spread_length=3, call_put=row['Call/Put'])
    except Exception as e:
        print("FAIL")
        trade_details = None
        logger.info(f"Could not build spread for {row['symbol']}: {e}")
        print(f"Could not build spread for {row['symbol']}: {e}")
        return pd.DataFrame(), "FALSE"
    # trade_details_1wk['underlying_price'] = underlying_price
    return trade_details_1wk, vol_check

def build_trade_structure_2wk(row):
    base_url, account_id, access_token = tradier.get_tradier_credentials(trading_mode, user)
    underlying_price = tradier.get_last_price(base_url, access_token, row['symbol'])
    try:
        option_chain = get_option_chain(row['symbol'], row['expiry_2wk'], row['Call/Put'])
        # option_chain = pd.concat([ochain_1wk, ochain_2wk])
        # if row['strategy'] == 'day_losers':
        #     if len(option_chain) < 12:
        #         contracts = None
        #         return contracts
        # else:
        #     if len(option_chain) < 20:
        #         contracts = None
        #         return contracts
        contracts_2wk = strategy_helper.build_spread(option_chain, 3, row['Call/Put'], underlying_price)
        trade_details_2wk, vol_check = trading_algorithms.bet_sizer(contracts_2wk, now, spread_length=3, call_put=row['Call/Put'])
    except Exception as e:
        trade_details = None
        logger.info(f"Could not build spread for {row['symbol']}: {e}")
        print(f"Could not build spread for {row['symbol']}: {e}")
        return pd.DataFrame(), "FALSE"
    # trade_details_2wk['underlying_price'] = underlying_price
    return trade_details_2wk, vol_check

def volume_check(trade_details):
    volumes = []
    for trade in trade_details:
        volumes.append(trade['volume'])
    avg_volume = sum(volumes)/len(trade_details)
    if avg_volume < 40:
        return "False"
    else:
        return "True"

def Date_1wk():
    t_1wk = timedelta((11 - d.weekday()) % 7)
    Expiry_Date = (d + t_1wk).strftime('%Y-%m-%d')
    return Expiry_Date 

def Date_2wk():
    t_2wk = timedelta((11 - d.weekday()) % 14)
    Expiry_Date = (d + t_2wk).strftime('%Y-%m-%d')
    return Expiry_Date 

def get_option_chain(symbol, expiry, call_put):
    url = f"https://api.polygon.io/v3/snapshot/options/{symbol}?expiration_date={expiry}&contract_type={call_put}&limit=250&apiKey=A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    headers = {}
    payload = {}
    response = requests.get(url, headers=headers, data=payload)
    try:
        response_data = json.loads(response.text)
    except Exception as e:
        print(e)
        print(response.json())

    results = response_data.get('results', [])
    details = [entry['details'] for entry in results]
    days = [entry['day'] for entry in results]

    for index, value in enumerate(details):
        try:
            value['volume'] = days[index]['volume']
            value['last_price'] = days[index]['close']
        except:
            value['volume'] = 0
            value['last_price'] = 0

    df = pd.DataFrame(details)
    return df

if __name__ == "__main__":
    build_trade(None, None)