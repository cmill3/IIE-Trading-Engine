from datetime import datetime, timedelta
from helpers.constants import CALL_STRATEGIES, PUT_STRATEGIES
import requests
import pandas as pd
import json
import pytz
import boto3
import math
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

now = datetime.now()
dt = now.strftime("%Y-%m-%d_%H:%M:%S")
current_date = now.strftime("%Y-%m-%d")

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
    sym = " ".join(re.findall("[a-zA-Z]+", symbol))
    underlying_symbol = sym[:-1]
    return underlying_symbol
    
def date_and_time():
    year = date.today().year
    month = date.today().month
    day = date.today().day
    hour = datetime.datetime.now().hour
    return year, month, day, hour
    
def calculate_dt_features(transaction_date, sell_by):
    transaction_dt = datetime.strptime(transaction_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    sell_by_dt = datetime(int(sell_by[0:4]), int(sell_by[5:7]), int(sell_by[8:10]),20)
    day_diff = transaction_dt - now
    day_diff = abs(day_diff.days)
    return day_diff
    

def polygon_call(contract, from_stamp, to_stamp, multiplier, timespan):
    payload={}
    headers = {}
    url = f"https://api.polygon.io/v2/aggs/ticker/O:{contract}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"

    response = requests.request("GET", url, headers=headers, data=payload)
    res_df = pd.DataFrame(json.loads(response.text)['results'])
    res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
    res_df['date'] = res_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    return res_df

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
    Returns the number of business days (excluding weekends) between two dates. For now we
    aren't considering market holidays because that is how the training data was generated.
    """
    
    transaction_dt = datetime.strptime(transaction_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    # We aren't inclusive of the transaction date
    days = (now - transaction_dt).days 

    complete_weeks = days // 7
    remaining_days = days % 7

    # Calculate the number of weekend days in the complete weeks
    weekends = complete_weeks * 2

    # Adjust for the remaining days
    if remaining_days:
        start_weekday = transaction_dt.weekday()
        end_weekday = now.weekday()

        if start_weekday <= end_weekday:
            if start_weekday <= 5 and end_weekday >= 5:
                weekends += 2  # Include both Saturdays and Sundays
            elif start_weekday <= 4 and end_weekday >= 4:
                weekends += 1  # Include only Saturdays
        else:
            if start_weekday <= 5:
                weekends += 1  # Include Saturday of the first week

    business_days = days - weekends
    return business_days 

def calculate_floor_pct(row):
   trading_hours = [9,10,11,12,13,14,15]
   from_stamp = row['order_transaction_date'].split('T')[0]
   time_stamp = datetime.strptime(row['order_transaction_date'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
   prices = polygon_call_stocks(row['underlying_symbol'], from_stamp, current_date, "10", "minute")
   trimmed_df = prices.loc[prices['t'] > time_stamp]
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
    time_stamp = datetime.strptime(row['order_transaction_date'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
    prices = polygon_call_stocks(f"O:{row['option_symbol']}", from_stamp, current_date, "10", "minute")
    trimmed_df = prices.loc[prices['t'] > time_stamp]
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

def bet_sizer(contracts, date, spread_length, call_put):
    if call_put == "call":
        target_cost = .0007* pull_trading_balance()
    elif call_put == "put":
        target_cost = .0007* pull_trading_balance()

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
        # transactions.append(contract['avg_transactions'])

    total_vol = sum(volumes)/len(contracts)
    if total_vol < 40:
        vol_check = "False"
    else:
        vol_check = "True"
    spread_cost = calculate_spread_cost(contracts)
    quantities = finalize_trade(contracts, spread_cost, target_cost)
    for index, contract in enumerate(contracts):
        if index < 3:
            try:
                contract['quantity'] = quantities[index]
                print(contract['quantity'])
            except Exception as e:
                logger.info(f"ERROR of {e} for {contract['contract_ticker']}")
                print(contracts)
                print(quantities)
        else:
            contract['quantity'] = 0
    return contracts, vol_check

def pull_trading_balance():
    ### This is hardcoded for now, but will be replaced with a call to the tradier API
    return 100000

def calculate_spread_cost(contracts_details):
    cost = 0
    for contract in contracts_details:
        cost += (100*contract['last_price'])
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

# if __name__ == "__main__":
#     x = calculate_floor_pct({'order_transaction_date': '2023-05-30T18:03:06.294Z', 'underlying_symbol': 'AR', 'trading_strategy': 'day_losers'})
#     print(x)
#     print(type(x))
