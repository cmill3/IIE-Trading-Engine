from datetime import datetime, timedelta, timezone
from helpers.constants import *
import helpers.dynamo_helper as db
import requests
import pandas as pd
import json
import pytz
import boto3
import math
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

est = pytz.timezone('US/Eastern')
date = datetime.now(est)
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
current_date = datetime.now().strftime("%Y-%m-%d")

trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
portfolio_strategy = os.getenv('PORTFOLIO_STRATEGY')
trading_strategy = os.getenv('TRADING_STRATEGY')
env = os.getenv("ENV")

limit = 50000
key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    

def calculate_sellby_date(current_date, trading_days_to_add): #End date, n days later for the data set built to include just trading days, but doesnt filter holiday
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    return current_date

def pull_symbol(symbol):
    return symbol[:-15]
    
def date_and_time():
    return date.year, date.month, date.day, date.hour, date.minute
    
def calculate_dt_features(transaction_date, sell_by):
    transaction_dt = datetime.strptime(transaction_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    sell_by_dt = datetime(int(sell_by[0:4]), int(sell_by[5:7]), int(sell_by[8:10]),20)
    day_diff = transaction_dt - date
    day_diff = abs(day_diff.days)
    return day_diff
    

def polygon_call_stocks(contract, from_stamp, to_stamp, multiplier, timespan):
    try:
        payload={}
        headers = {}
        url = f"https://api.polygon.io/v2/aggs/ticker/{contract}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"
        response = requests.request("GET", url, headers=headers, data=payload)
        res_df = pd.DataFrame(json.loads(response.text)['results'])
        res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
        res_df['date'] = res_df['t'].apply(lambda x:convert_timestamp_est(x))
        res_df['hour'] = res_df['date'].apply(lambda x: x.hour)
        res_df['minute'] = res_df['date'].apply(lambda x: x.minute)
        return res_df
    except Exception as e:  
        logger.info(e)
        return pd.DataFrame()

def get_business_days(transaction_date):
    """
    This function can be used to find the number of days between purchase time and current time for a strategy that doesn't hold through the weekend.
    """
    transaction_dt = datetime.strptime(transaction_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    if date.day == transaction_dt.day:
        return 0
    else:
        transaction_day_of_week = transaction_dt.weekday()
        current_day_of_week = date.weekday()
        return current_day_of_week - transaction_day_of_week

def calculate_floor_pct(row):
    trading_hours = [9,10,11,12,13,14,15]
    from_stamp = row['order_transaction_date'].split('T')[0]
    time_stamp = convert_datestring_to_timestamp_UTC(row['order_transaction_date'])
    prices = polygon_call_stocks(row['underlying_symbol'], from_stamp, current_date, "10", "minute")
    trimmed_df = prices.loc[prices['t'] > time_stamp]
    trimmed_df['t'] = trimmed_df['t'].apply(lambda x: int(x/1000))
    trimmed_df['date'] = trimmed_df['t'].apply(lambda x: convert_timestamp_est(x))
    trimmed_df['time'] = trimmed_df['date'].apply(lambda x: x.time())
    trimmed_df['hour'] = trimmed_df['date'].apply(lambda x: x.hour)
    trimmed_df = trimmed_df.loc[trimmed_df['hour'].isin(trading_hours)]
    trimmed_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]
    trimmed_df = trimmed_df.iloc[1:]
    high_price = trimmed_df['h'].max()
    low_price = trimmed_df['l'].min()
    if len(trimmed_df) == 0:
        return float(row['underlying_purchase_price'])
    if row['trading_strategy'] in PUT_STRATEGIES:
        return low_price
    elif row['trading_strategy'] in CALL_STRATEGIES:
        return high_price
    else:
        return 0
   

def get_derivative_max_value(row):
    trading_hours = [9,10,11,12,13,14,15]
    from_stamp = row['order_transaction_date'].split('T')[0]
    time_stamp = convert_datestring_to_timestamp_UTC(row['order_transaction_date'])
    prices = polygon_call_stocks(f"O:{row['option_symbol']}", from_stamp, current_date, "10", "minute")
    trimmed_df = prices.loc[prices['t'] > time_stamp]
    trimmed_df['t'] = trimmed_df['t'].apply(lambda x: int(x/1000))
    trimmed_df['date'] = trimmed_df['t'].apply(lambda x: convert_timestamp_est(x))
    trimmed_df['time'] = trimmed_df['date'].apply(lambda x: x.time())
    trimmed_df['hour'] = trimmed_df['date'].apply(lambda x: x.hour)
    trimmed_df = trimmed_df.loc[trimmed_df['hour'].isin(trading_hours)]
    trimmed_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]
    trimmed_df = trimmed_df.iloc[1:]
    high_price = trimmed_df['h'].max()
    return high_price


def pull_opened_data_s3(path, bucket,date_prefix):
    dfs = []
    keys = s3.list_objects(Bucket=bucket,Prefix=f"{path}/{date_prefix}")["Contents"]
    for object in keys:
        key = object['Key']
        dataset = s3.get_object(Bucket=bucket,Key=f"{key}")
        df = pd.read_csv(dataset.get("Body"))
        df = format_pending_df(df)
        if len(df) > 0:
            dfs.append(df)
    full_df = pd.concat(dfs)
    full_df.reset_index(drop=True)
    return full_df

def pull_data_s3(path, bucket,date_prefix):
    dfs = []
    keys = s3.list_objects(Bucket=bucket,Prefix=f"{path}/{date_prefix}")["Contents"]
    for object in keys:
        key = object['Key']
        dataset = s3.get_object(Bucket=bucket,Key=f"{key}")
        df = pd.read_csv(dataset.get("Body"))
        if len(df) > 0:
            dfs.append(df)
    full_df = pd.concat(dfs)
    full_df.reset_index(drop=True)
    return full_df

def calculate_date_prefix():
    now = datetime.now()
    date_prefix = now.strftime("%Y/%m/%d")
    return date_prefix

def format_pending_df(df):
    columns = df['Unnamed: 0'].values
    indexes = df.columns.values[1:]

    unpacked_data = []
    for index in indexes:
        data = df[index].values
        unpacked_data.append(data)
    
    formatted_df = pd.DataFrame(unpacked_data,indexes,columns)
    return formatted_df

def build_date():
    date = datetime.now()
    
    if date.day < 10:
        day = "0" + str(date.day)
    else:
        day = str(date.day)
        
    if date.month < 10:
        month = "0" + str(date.month)
    else:
        month = str(date.month)

    temp_year = date.year
    return f"{temp_year}/{month}/{day}/{date.hour}"

def convert_timestamp_est(timestamp):
    # Create a naive datetime object from the UNIX timestamp
    dt_naive = datetime.utcfromtimestamp(timestamp)
    # Convert the naive datetime object to a timezone-aware one (UTC)
    dt_utc = pytz.utc.localize(dt_naive)
    # Convert the UTC datetime to EST
    dt_est = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    return dt_est

def bet_sizer(contracts, date, spread_length, call_put,strategy):
    target_cost = (.004* pull_trading_balance())
    to_stamp = (date - timedelta(days=1)).strftime("%Y-%m-%d")
    from_stamp = (date - timedelta(days=5)).strftime("%Y-%m-%d")
    volumes = []
    # transactions = []
    # contracts_details = []
    for contract in contracts:
        polygon_result = polygon_call_stocks(contract['contract_ticker'],from_stamp, to_stamp,multiplier=1,timespan="day")
        if len(polygon_result) == 0:
            volumes.append(0)
            continue
        contract['avg_volume'], contract['avg_transactions'] = build_volume_features(polygon_result)
        volumes.append(contract['avg_volume'])


    contracts = size_spread_quantities(contracts, target_cost)

    return contracts

def pull_trading_balance():
    return db.get_trading_balance(portfolio_strategy, env)

def calculate_spread_cost(contract):
    cost = (100*float(contract['last_price']))
    print(cost)
    return cost

def build_volume_features(df):
    avg_volume = df['v'].mean()
    avg_transactions = df['n'].mean()
    return avg_volume, avg_transactions

def finalize_trade(contracts_details, spread_cost, target_cost):
    if len(contracts_details) == 1:
        spread_multiplier = math.floor(target_cost/spread_cost)
        return [spread_multiplier]
    elif len(contracts_details) == 2:
        if (1*target_cost) >= spread_cost >= (.9*target_cost):
            return [1,1]
        elif spread_cost > (1*target_cost):
            spread2_cost = calculate_spread_cost(contracts_details[1:])
            if spread2_cost < (1*target_cost):
                return [0,1]
            else:
                contract = contracts_details[0]
                single_contract_cost = 100 * contract['last_price']
                if single_contract_cost > (1*target_cost):
                    contract = contracts_details[1]
                    single_contract_cost = 100 * contract['last_price']
                    if single_contract_cost > (1*target_cost):
                        return [0,0]
                else:
                    return [1,0]
        elif spread_cost < (.9*target_cost):
            spread_multiplier, add_one = add_spread_cost(spread_cost, target_cost, contracts_details)
            if add_one:  
                return [(spread_multiplier+1),spread_multiplier]
            else:
                return [spread_multiplier,spread_multiplier]
        else:
            print("ERROR")
            return [0,0]
    elif len(contracts_details) >= 3:
        contracts_details = contracts_details[0:3]
        if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
            return [1,1,1]
        elif spread_cost > (1*target_cost):
            spread2_cost = calculate_spread_cost(contracts_details[1:])
            if spread2_cost < (1*target_cost):
                return [0,1,1]
            else:
                contract = contracts_details[0]
                single_contract_cost = 100 * contract['last_price']
                if single_contract_cost > (1*target_cost):
                    contract = contracts_details[1]
                    single_contract_cost = 100 * contract['last_price']
                    if single_contract_cost > (1*target_cost):
                        contract = contracts_details[2]
                        single_contract_cost = 100 * contract['last_price']
                        if single_contract_cost > (1*target_cost):
                            return [0,0,0]
                        else:
                            return [0,0,1]
                    else:
                        return [0,1,0]
                else:
                    return [1,0,0] 
        elif spread_cost < (.9*target_cost):
            spread_multiplier, add_one = add_spread_cost(spread_cost, target_cost, contracts_details)
            if add_one:  
                return [(spread_multiplier+1),spread_multiplier,spread_multiplier]
            else:
                return [spread_multiplier,spread_multiplier,spread_multiplier]
        else:
            print("ERROR")
            return [0,0,0]
    else:
        return "NO TRADES"
    
def size_spread_quantities(contracts_details, target_cost):
    adjusted_target_cost = target_cost/100
    adjusted_contracts = contracts_details[1:3]
    spread_length = 2

    spread_candidates = configure_contracts_for_trade_pct_based(adjusted_contracts, adjusted_target_cost, spread_length)

    if len(spread_candidates) == 0:
        return []


    details_df = pd.DataFrame(spread_candidates)
    details_df = details_df.loc[details_df['quantity'] > 0]
    details_df.reset_index(drop=True, inplace=True)
    details_df['spread_position'] = details_df.index
    details = details_df.to_dict(orient='records')
    return details

def configure_contracts_for_trade(contracts_details, target_cost, spread_length):
    spread_candidates = []
    total_cost = 0
    cost_remaining = target_cost
    for contract in contracts_details:
        contract['contract_cost'] = 100 * contract['last_price']    
        if contract['contract_cost'] < cost_remaining:
            spread_candidates.append(contract)
            cost_remaining -= contract['contract_cost']
            total_cost += contract['contract_cost']
        if len(spread_candidates) == spread_length:
            return spread_candidates, total_cost
    return spread_candidates, total_cost

def configure_contracts_for_trade_pct_based(contracts_details, target_cost, spread_length):
    spread_candidates = []
    split_cost = target_cost / spread_length
    for contract in contracts_details:
        cost = float(contract['last_price'])
        contract_quantity = 0
        split_cost_remaining = split_cost
        while split_cost_remaining > 0:
            if (split_cost_remaining - cost) > 0:
                split_cost_remaining = split_cost_remaining - cost
                contract_quantity += 1
            else:
                break

        spread_candidates.append({"contract_ticker": contract['contract_ticker'], "quantity": contract_quantity,"last_price": contract['last_price']})
    return spread_candidates
            
def add_spread_cost(spread_cost, target_cost, contracts_details):
    add_one = False
    spread_multiplier = 1
    total_cost = spread_cost
    if spread_cost == 0:
        return 0, False
    else:
        while total_cost <= (1*target_cost):
            spread_multiplier += 1
            total_cost = spread_cost * spread_multiplier
        
        if total_cost > (1*target_cost):
            spread_multiplier -= 1
            total_cost -= spread_cost

        if total_cost < (.67*target_cost):
            add_one = True

    return spread_multiplier, add_one

def convert_timestamp_est(timestamp):
    # Create a naive datetime object from the UNIX timestamp
    dt_naive = datetime.utcfromtimestamp(timestamp)
    # Convert the naive datetime object to a timezone-aware one (UTC)
    dt_utc = pytz.utc.localize(dt_naive)
    # Convert the UTC datetime to EST
    dt_est = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    return dt_est

def convert_datestring_to_timestamp_UTC(date_string):
    ## takes in date string and converts to timestamp in GMT time
    ## used to filter polygon results
    date_obj = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    timestamp = date_obj.timestamp()
    return timestamp

def log_message_close(row, id, status_code, reason, error,lambda_signifier):
    if status_code == 200:
        log_entry = json.dumps({
            "lambda_signifier": lambda_signifier,
            "log_type": "close_success",
            "order_id": row['order_id'],
            "position_id": row['position_id'],
            "closing_order_id": id,
            "status_code": status_code,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "close_reason": reason
        })
        logger.info(log_entry)
    else:
        log_entry = json.dumps({
            "lambda_signifier": lambda_signifier,
            "log_type": "close_error",
            "order_id": row['order_id'],
            "position_id": row['position_id'],
            "response": error,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        logger.error(log_entry)

def log_message_open(row, id, status_code, error, contract_ticker, option_side,lambda_signifier,detail):
    model_config = pull_model_config(row['strategy'])
    log_entry = json.dumps({
        "lambda_signifier": lambda_signifier,
        "log_type": "open_success",
        "order_id": row['order_id'],
        "position_id": row['position_id'],
        "status_code": status_code,
        "contract_ticker": contract_ticker,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "underlying_purchase_price": row['underlying_purchase_price'],
        "quantity": row['quantity'],
        "strategy": row['strategy'],
        "target_value": model_config['target_value'],
        'underlying_symbol': row['symbol'],
        'option_side': option_side,
        'return_vol_10D': row['return_vol_10D'],
        "spread_position": detail['spread_position'],
    })
    logger.info(log_entry)
    
def log_message_open_error(row, id, status_code, error, contract_ticker, option_side,lambda_signifier):
    log_entry = json.dumps({
        "lambda_signifier": lambda_signifier,
        "log_type": "open_error",
        "order_id": row['order_id'],
        "position_id": row['position_id'],
        "response": error,
        "contract_ticker": contract_ticker,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    logger.error(log_entry)

def pull_model_config(trading_strategy):
    s3 = boto3.client('s3')
    weekday = date.weekday()
    monday = date - timedelta(days=weekday)
    date_prefix = monday.strftime("%Y/%m/%d")
    model_config = s3.get_object(Bucket="inv-alerts-trading-data", Key=f"model_configurations/{date_prefix}.csv")
    model_config = pd.read_csv(model_config.get("Body"))
    model_config = model_config.loc[model_config['strategy'] == trading_strategy]
    return {"target_value": model_config['target_value'].values[0], "strategy": model_config['strategy'].values[0]}

def pull_balance_df():
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket='inv-alerts-trading-data',Prefix=f'trading_balance/{env}')['Contents']
    
    # Sort the objects by last modified date and get the most recent one
    latest_file = sorted(objects, key=lambda x: x['LastModified'], reverse=True)[0]
    
    # Download the most recent file and load it into a pandas DataFrame
    csv_obj = s3.get_object(Bucket='inv-alerts-trading-data', Key=latest_file['Key'])
    df = pd.read_csv(csv_obj.get("Body"))

    return df

if __name__ == "__main__":
    x = convert_datestring_to_timestamp_UTC("2024-03-28T15:09:38.959Z")
    # x = convert_timestamp_est((1711652978959/1000))
    print(x)
    
