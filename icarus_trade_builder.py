# Purpose: This script is used to build trades for the Icarus Trading Bot
import pandas as pd
from datetime import datetime, timedelta
from helpers import strategy_helper, tradier, helper
import boto3
import os
import logging
import requests
import json
import pytz
from helpers.constants import PREFIXES, CALL_STRATEGIES, PUT_STRATEGIES, ACTIVE_STRATEGIES, ENDPOINT_NAMES, TRADING_STRATEGIES

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
env = os.getenv("ENV")

trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
model_results_bucket = os.getenv('MODEL_RESULTS_BUCKET')




est = pytz.timezone('US/Eastern')
now = datetime.now(est)

d = now.date() # Monday

def build_trade_inv(event, context):
    year, month, day, hour = format_dates(now)
    trading_strategy = os.getenv("TRADING_STRATEGY")
    logger.info(trading_strategy)
    logger.info(f'Initializing trade builder: {year}-{month}-{day}-{hour}')
    df  = pull_data_inv(trading_strategy,year, month, day, hour)
    df['strategy'] = trading_strategy
    if trading_strategy == "IDXC_1D" or trading_strategy == "IDXP_1D" or trading_strategy == "IDXC" or trading_strategy == "IDXP":
        results_df = process_data_index(df)
    else:
        results_df = process_data(df)
    csv = results_df.to_csv()
    logger.info(f"invalerts_potential_trades/{trading_strategy}/{year}/{month}/{day}/{hour}.csv")
    response = s3.put_object(Body=csv, Bucket=trading_data_bucket, Key=f"invalerts_potential_trades/{trading_strategy}/{year}/{month}/{day}/{hour}.csv")
    return {
        'statusCode': 200
    }

#Non-Explanatory Variables Explained Below:
#CP = Call/Put (used to represent the Call/Put Trend Value)

def pull_data_inv(trading_strategy,year, month, day, hour):
    if env == "DEV":
        dataset = s3.get_object(Bucket="inv-alerts-trading-data", Key=f"classifier_predictions/{trading_strategy}/{year}/{month}/{day}/{hour}.csv")
    else:
        dataset = s3.get_object(Bucket=trading_data_bucket, Key=f"classifier_predictions/{trading_strategy}/{year}/{month}/{day}/{hour}.csv")
    df = pd.read_csv(dataset.get("Body"))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    return df


def process_data(df):
    df['Call/Put'] = df['strategy'].apply(lambda strategy: infer_CP(strategy))
    df['expiry_1wk'] = date_1wk()
    df['expiry_2wk'] = date_2wk()
    df['expiry_1d'] = df['symbol'].apply(lambda x: date_1d(x))
    df['expiry_3d'] = df['symbol'].apply(lambda x: date_3d(x))
    result_df1 = df.apply(lambda row: build_trade_structure_1wk(row), axis=1,result_type='expand')
    result_df2 = df.apply(lambda row: build_trade_structure_2wk(row), axis=1,result_type='expand')
    df['trade_details1wk'] = pd.DataFrame(result_df1, index=df.index)
    df['trade_details2wk'] = pd.DataFrame(result_df2, index=df.index)
    df['sector'] = df['symbol'].apply(lambda Sym: strategy_helper.match_sector(Sym))
    df['sellby_date'] = calculate_sellby_date(d, 3)
    logger.info(f"Data processed successfully: {d}")
    return df

def process_data_index(df):
    df['Call/Put'] = df['strategy'].apply(lambda strategy: infer_CP(strategy))
    df['expiry_1d'] = df['symbol'].apply(lambda x: date_1d(x))
    df['expiry_3d'] = df['symbol'].apply(lambda x: date_3d(x))
    result_df1 = df.apply(lambda row: build_trade_structure_1d(row), axis=1,result_type='expand')
    result_df2 = df.apply(lambda row: build_trade_structure_3d(row), axis=1,result_type='expand')
    df['trade_details1d'] = pd.DataFrame(result_df1, index=df.index)
    df['trade_details3d'] = pd.DataFrame(result_df2, index=df.index)
    df['sector'] = df['symbol'].apply(lambda Sym: strategy_helper.match_sector(Sym))
    df['sellby_date'] = calculate_sellby_date(d, 3)
    logger.info(f"Data processed successfully: {d}")
    return df

def infer_CP(strategy):
    if strategy in CALL_STRATEGIES:   
        return "call"
    elif strategy in PUT_STRATEGIES:
        return "put"
    

def calculate_sellby_date(current_date, trading_days_to_add): #End date, n days later for the data set built to include just trading days, but doesnt filter holiday
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    return current_date

def build_trade_structure_1d(row):
    underlying_price = tradier.call_polygon_last_price(row['symbol'])
    try:
        option_chain = get_option_chain(row['symbol'], row['expiry_1d'], row['Call/Put'])
        contracts_1d = strategy_helper.build_spread(option_chain, 8, row['Call/Put'], underlying_price)
        contracts_1d = smart_spreads_filter(contracts_1d,underlying_price)
        trade_details_1d = helper.bet_sizer(contracts_1d, now, spread_length=3, call_put=row['Call/Put'],strategy=row['strategy'])
    except Exception as e:
        print("FAIL")
        logger.info(f"Could not build spread for {row['symbol']}: {e}")
        print(f"Could not build spread for {row['symbol']}: {e}")
        return pd.DataFrame(), "FALSE"
    return trade_details_1d

def build_trade_structure_3d(row):
    underlying_price = tradier.call_polygon_last_price(row['symbol'])
    try:
        option_chain = get_option_chain(row['symbol'], row['expiry_3d'], row['Call/Put'])
        contracts_3d = strategy_helper.build_spread(option_chain, 8, row['Call/Put'], underlying_price)
        contracts_3d = smart_spreads_filter(contracts_3d,underlying_price)
        trade_details_3d = helper.bet_sizer(contracts_3d, now, spread_length=3, call_put=row['Call/Put'],strategy=row['strategy'])
    except Exception as e:
        logger.info(f"Could not build spread for {row['symbol']}: {e}")
        print(f"Could not build spread for {row['symbol']}: {e}")
        return pd.DataFrame(), "FALSE"
    return trade_details_3d

def build_trade_structure_1wk(row):
    underlying_price = tradier.call_polygon_last_price(row['symbol'])
    try:
        if row['symbol'] in ["IWM","SPY","QQQ"]:
            option_chain = get_option_chain(row['symbol'], row['expiry_1d'], row['Call/Put'])
        else:
            option_chain = get_option_chain(row['symbol'], row['expiry_1wk'], row['Call/Put'])
        contracts_1wk = strategy_helper.build_spread(option_chain, 8, row['Call/Put'], underlying_price)
        contracts_1wk = smart_spreads_filter(contracts_1wk,underlying_price)
        trade_details_1wk = helper.bet_sizer(contracts_1wk, now, spread_length=3, call_put=row['Call/Put'],strategy=row['strategy'])
    except Exception as e:
        print("FAIL")
        logger.info(f"Could not build spread for {row['symbol']}: {e} 1WK")
        print(f"Could not build spread for {row['symbol']}: {e} 1WK")
        return pd.DataFrame(), "FALSE"
    return trade_details_1wk

def build_trade_structure_2wk(row):
    underlying_price = tradier.call_polygon_last_price(row['symbol'])
    try:
        if row['symbol'] in ["IWM","SPY","QQQ"]:
            option_chain = get_option_chain(row['symbol'], row['expiry_3d'], row['Call/Put'])
        else:
            option_chain = get_option_chain(row['symbol'], row['expiry_2wk'], row['Call/Put'])
        contracts_2wk = strategy_helper.build_spread(option_chain, 8, row['Call/Put'], underlying_price)
        contracts_2wk = smart_spreads_filter(contracts_2wk,underlying_price)
        trade_details_2wk = helper.bet_sizer(contracts_2wk, now, spread_length=3, call_put=row['Call/Put'],strategy=row['strategy'])
    except Exception as e:
        trade_details = None
        logger.info(f"Could not build spread for {row['symbol']}: {e} 2WK")
        print(f"Could not build spread for {row['symbol']}: {e} 2WK")
        return pd.DataFrame(), "FALSE"
    return trade_details_2wk

def volume_check(trade_details):
    volumes = []
    for trade in trade_details:
        volumes.append(trade['volume'])
    avg_volume = sum(volumes)/len(trade_details)
    if avg_volume < 40:
        return "False"
    else:
        return "True"

def date_1wk():
    t_1wk = timedelta((11 - d.weekday()) % 7)
    Expiry_Date = (d + t_1wk).strftime('%Y-%m-%d')
    return Expiry_Date 

def date_2wk():
    t_2wk = timedelta((11 - d.weekday()) % 14)
    Expiry_Date = (d + t_2wk).strftime('%Y-%m-%d')
    return Expiry_Date 

def date_1d(symbol):
    if symbol == "IWM":
        date = advance_weekday(3)
        if date.weekday() == 1 or date.weekday() == 3:
            date += timedelta(days=1)
    elif symbol in ["SPY","QQQ"]:
        date = advance_weekday(3)
    else:
        return "NA"
    return date.strftime('%Y-%m-%d')

def date_3d(symbol):
    if symbol == "IWM":
        date = advance_weekday(5)
        if date.weekday() == 1 or date.weekday() == 3:
            date += timedelta(days=1)
    elif symbol in ["SPY","QQQ"]:
        date = advance_weekday(5)
    else:
        return "NA"
    return date.strftime('%Y-%m-%d')


def advance_weekday(days):
    current_date = datetime.now()
    added_days = 0

    while added_days < days:
        current_date += timedelta(days=1)
        # Only increment the added_days counter if the day is a weekday
        if current_date.weekday() < 5:  # 0-4 represents Monday to Friday
            added_days += 1

    return current_date



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

def smart_spreads_filter(contracts,underlying_price):
    new_contracts = []
    for contract in contracts:
        contract['pct_to_money'] = abs(underlying_price - contract['strike'])/underlying_price
        if contract['pct_to_money'] < .1:
            new_contracts.append(contract)
    return new_contracts

def format_dates(now):
    now_str = now.strftime("%Y-%m-%d-%H")
    year, month, day, hour = now_str.split("-")
    hour = int(hour)
    return year, month, day, hour


if __name__ == "__main__":
    build_trade_inv(None, None)