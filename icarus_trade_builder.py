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
from helpers.constants import CALL_STRATEGIES, PUT_STRATEGIES

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

env = os.getenv("ENV")
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
portfolio_strategy = os.getenv("PORTFOLIO_STRATEGY")




est = pytz.timezone('US/Eastern')
now = datetime.now(est)

d = now.date() # Monday

def build_trade_inv(event, context):
    year, month, day, hour = format_dates(now)
    if portfolio_strategy in ["CDVOL_GAIN","CDVOL_LOSE"]:
        trading_strategies = os.getenv("TRADING_STRATEGY").split(",")
    elif portfolio_strategy == "CDVOLBF":
        trading_strategies = [os.getenv("TRADING_STRATEGY")]

    for trading_strategy in trading_strategies:
        logger.info(trading_strategy)
        logger.info(f'Initializing trade builder: {year}-{month}-{day}-{hour}')
        df  = pull_data_inv(trading_strategy,year, month, day, hour)
        df['strategy'] = trading_strategy

        results_df = process_data(df)
        csv = results_df.to_csv()
        s3.put_object(
            Body=csv, Bucket=trading_data_bucket, 
            Key=f"invalerts_potential_trades/{env}/{trading_strategy}/{year}/{month}/{day}/{hour}.csv"
            )
    
    return {
        'statusCode': 200
    }

#Non-Explanatory Variables Explained Below:
#CP = Call/Put (used to represent the Call/Put Trend Value)

def pull_data_inv(trading_strategy,year, month, day, hour):
    if env == "PROD_VAL":
        dataset = s3.get_object(Bucket="inv-alerts-trading-data", Key=f"classifier_predictions/PROD/{trading_strategy}/{year}/{month}/{day}/{hour}.csv")
    else:
        dataset = s3.get_object(Bucket="inv-alerts-trading-data", Key=f"classifier_predictions/{env}/{trading_strategy}/{year}/{month}/{day}/{hour}.csv")
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
    df['trade_details1wk'] = df.apply(lambda row: build_trade_structure_1wk(row), axis=1)
    df['trade_details2wk'] = df.apply(lambda row: build_trade_structure_2wk(row), axis=1)
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

def build_trade_structure_1wk(row):
    underlying_price = tradier.call_polygon_last_price(row['symbol'])
    try:
        if row['symbol'] in ["IWM","SPY","QQQ"]:
            option_chain = get_option_chain(row['symbol'], row['expiry_1d'], row['Call/Put'])
        else:
            option_chain = get_option_chain(row['symbol'], row['expiry_1wk'], row['Call/Put'])
        contracts_1wk = strategy_helper.build_spread(option_chain, 6, row['Call/Put'], underlying_price)
        contracts_1wk = smart_spreads_filter(contracts_1wk,underlying_price)
        trade_details_1wk = helper.bet_sizer(contracts_1wk, now, spread_length=4, call_put=row['Call/Put'],strategy=row['strategy'])
    except Exception as e:
        print("FAIL")
        logger.info(f"Could not build spread for {row['symbol']}: {e} 1WK")
        print(f"Could not build spread for {row['symbol']}: {e} 1WK")
        return [], "FALSE"
    
    return trade_details_1wk

def build_trade_structure_2wk(row):
    underlying_price = tradier.call_polygon_last_price(row['symbol'])
    try:
        if row['symbol'] in ["IWM","SPY","QQQ"]:
            option_chain = get_option_chain(row['symbol'], row['expiry_3d'], row['Call/Put'])
        else:
            option_chain = get_option_chain(row['symbol'], row['expiry_2wk'], row['Call/Put'])
        contracts_2wk = strategy_helper.build_spread(option_chain, 6, row['Call/Put'], underlying_price)
        contracts_2wk = smart_spreads_filter(contracts_2wk,underlying_price)
        trade_details_2wk = helper.bet_sizer(contracts_2wk, now, spread_length=4, call_put=row['Call/Put'],strategy=row['strategy'])
    except Exception as e:
        trade_details = None
        logger.info(f"Could not build spread for {row['symbol']}: {e} 2WK")
        print(f"Could not build spread for {row['symbol']}: {e} 2WK")
        return [], "FALSE"
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
    date = advance_weekday(2)
    return date.strftime('%Y-%m-%d')

def date_3d(symbol):
    date = advance_weekday(4)
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

    parsed_details = []

    for index, value in enumerate(details):
        try:
            value['volume'] = days[index]['volume']
            value['last_price'] = days[index]['close']
            parsed_details.append(value)
        except:
            logger.info(f"Could not find volume or last price for {value['ticker']}")
            continue
            # value['volume'] = 0
            # value['last_price'] = 0

    df = pd.DataFrame(parsed_details)
    return df

def smart_spreads_filter(contracts,underlying_price):
    new_contracts = []
    for contract in contracts:
        contract['pct_to_money'] = abs(underlying_price - contract['strike'])/underlying_price
        if contract['pct_to_money'] < .075:
            new_contracts.append(contract)
    return new_contracts

def format_dates(now):
    now_str = now.strftime("%Y-%m-%d-%H")
    year, month, day, hour = now_str.split("-")
    hour = int(hour)
    return year, month, day, hour


if __name__ == "__main__":
    build_trade_inv(None, None)