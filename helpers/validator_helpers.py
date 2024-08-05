from datetime import timedelta, datetime
import datetime as dt
import pandas as pd
import warnings
import boto3
import pytz
import json
import ast
import requests
from helpers.constants import *
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

s3 = boto3.client('s3')


###########
###########
###########
########### BACKTESTING FUNCTIONS
###########
###########
###########


def pull_data_invalerts(bucket_name, object_key, file_name, prefixes, time_span):
    dfs = []
    for prefix in prefixes:
        try:
            print(f"{object_key}/{prefix}/{file_name}")
            obj = s3.get_object(Bucket=bucket_name, Key=f"{object_key}/{prefix}/{file_name}")
            df = pd.read_csv(obj.get("Body"))
            df['strategy'] = prefix
            dfs.append(df)
        except Exception as e:
            print(f"no file for {prefix} with {e}")
            continue
    data = pd.concat(dfs)
    data = data[data.predictions == 1]
    start_time = datetime.strptime(data['date'].values[0], '%Y-%m-%d')
    end_date = create_end_date_local_data(data['date'].values[-1], time_span)
    datetime_list, datetime_index, results = create_datetime_index(start_time, end_date)
    return data, datetime_list


def create_simulation_data_inv(row,config):
    date_str = row['date'].split(" ")[0]
    start_date = datetime(int(date_str.split("-")[0]),int(date_str.split("-")[1]),int(date_str.split("-")[2]),int(row['hour_est']),0,0)
    if row['strategy'] in ONED_STRATEGIES:
        days_back = 1
    end_date = create_end_date(start_date, days_back)
    trading_aggregates, option_symbols = create_options_aggs_inv(row,start_date,end_date=end_date,spread_length=config['spread_length'],config=config)
    return start_date, end_date, row['symbol'], row['o'], row['strategy'], option_symbols, trading_aggregates

def buy_iterate_sellV2_invalerts(symbol, option_symbol, open_prices, strategy, polygon_df, position_id, trading_date, alert_hour,order_id,config,row,order_num):
    open_price = open_prices[0]
    open_datetime = datetime(int(trading_date.split("-")[0]),int(trading_date.split("-")[1]),int(trading_date.split("-")[2]),int(alert_hour),0,0,tzinfo=pytz.timezone('US/Eastern'))
    print("OPEN DATETIME")
    print(open_datetime)
    print(row)
    print()
    contract_cost = round(open_price * 100,2)

    if strategy in CALL_STRATEGIES:
        contract_type = "calls"
    elif strategy in PUT_STRATEGIES:
        contract_type = "puts"
        
    buy_dict = {"open_price": open_price, "open_datetime": open_datetime, "quantity": row['quantity'], "contract_cost": contract_cost, "option_symbol": option_symbol, "position_id": position_id, "contract_type": contract_type}
    if config['model'] == "CDVOLVARVC":
        try:    
            if strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = tda_CALL_1D_CDVOLVARVC(polygon_df,open_datetime,row['quantity'],config,row,order_num=order_num)
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = tda_PUT_1D_CDVOLVARVC(polygon_df,open_datetime,row['quantity'],config,row,order_num=order_num)
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} CDVOLVARVC")
            print(polygon_df)
            return {}
    
    try:
        sell_dict['position_id'] = position_id
        results_dict = create_results_dict(buy_dict, sell_dict, order_id)
        results_dict['position_id'] = position_id
        # transaction_dict = {"buy_dict": buy_dict, "sell_dict":sell_dict, "results_dict": results_dict}
        buy_dt = buy_dict['open_datetime']
        sell_dt = sell_dict['close_datetime']
        buy_dt = datetime(buy_dt.year,buy_dt.month,buy_dt.day,buy_dt.hour)
        sell_dt = datetime(sell_dt.year,sell_dt.month,sell_dt.day,sell_dt.hour)
        if buy_dt > sell_dt:
            print(f"Date Mismatch for {symbol}")
            print(f"{buy_dt} vs. {sell_dt}")
            # print(sell_dict['close_datetime'])
            print()
    except Exception as e:
        print(f"Error {e} in transaction_dict for {symbol}")
        print(buy_dict)
        print(sell_dict)
        print(results_dict)
        print()
    return results_dict
    # return buy_dict, sell_dict, results_dict, transaction_dict, open_datetime

def simulate_trades_invalerts(row,config):
    positions_list = []
    order_num = row['spread_position']
    alert_hour = row['hour_est']
    trading_date = row['date']
    start_date, end_date, symbol, mkt_price, strategy, option_symbols, enriched_options_aggregates = create_simulation_data_inv(row,config)
    print("AGGS")
    print(enriched_options_aggregates)
    order_dt = start_date.strftime("%m+%d")
    pos_dt = start_date.strftime("%Y-%m-%d")
    position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{pos_dt}-{row['hour_utc']}"

    results = []

    for df in enriched_options_aggregates:
        try:
            open_prices = df['o'].values
            ticker = df.iloc[0]['ticker']
            order_id = f"{order_num}_{order_dt}"
            results_dict = buy_iterate_sellV2_invalerts(symbol, ticker, open_prices, strategy, df, position_id, trading_date, alert_hour, order_id,config,row,order_num)
            results_dict['order_num'] = order_num
            if len(results_dict) == 0:
                logger.info(f"Error in simulate_trades_invalerts for {symbol} and {ticker}")
                continue

            results.append(results_dict)
        except Exception as e:
            print(f"error: {e} in buy_iterate_sellV2_invalerts")
            # logger.info(f"error: {e} in buy_iterate_sellV2_invalerts")
            continue
    
    return {"position_id": f"{position_id}", "transactions": results, "open_datetime": results_dict['open_trade_dt']}
    


#########
#########
#########
######### BACKTEST HELPERS
#########
#########
#########


def create_end_date(date, trading_days_to_add):
    #Trading days only
    while trading_days_to_add > 0:
        date += timedelta(days=1)
        weekday = date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1

    day = date.day
    month = date.month
    year = date.year
    date = datetime(year, month, day, 16, 0)
    return date

def create_end_date_local_data(date, trading_days_to_add):
    #Trading days only
    date = datetime.strptime(date, '%Y-%m-%d')
    # trading_days_to_add = add_days
    while trading_days_to_add > 0:
        date += timedelta(days=1)
        weekday = date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    
    return date

def create_results_dict(buy_dict, sell_dict,order_id):
    price_change = sell_dict['close_price'] - buy_dict['open_price']
    pct_gain = (price_change / buy_dict['open_price']) *100
    total_gain = (price_change*100) * buy_dict['quantity']
    buy_dict['order_id'] = order_id
    sell_dict['order_id'] = order_id
    results_dict = {
                    "price_change":price_change, "pct_gain":pct_gain, "total_gain":total_gain,
                    "open_trade_dt": buy_dict['open_datetime'].strftime('%Y-%m-%d %H:%M'), "close_trade_dt": sell_dict['close_datetime'].strftime('%Y-%m-%d %H:%M'),
                    "sell_info": sell_dict, "buy_info": buy_dict,
                    }
    return results_dict

def create_options_aggs_inv(row,start_date,end_date,spread_length,config):
    options = []
    enriched_options_aggregates = []
    strike = row['contract_ticker']
    print(row)
    try:
        underlying_agg_data = polygon_stockdata_inv(row['symbol'],start_date,end_date)
    except Exception as e:
        print(f"Error: {e} in underlying agg for {row['symbol']} of {row['strategy']}")
        try:
            underlying_agg_data = polygon_stockdata_inv(row['symbol'],start_date,end_date)
        except Exception as e:
            print(f"Error: {e} in underlying agg for 2ND {row['symbol']} of {row['strategy']}")
            try: 
                underlying_agg_data = polygon_stockdata_inv(row['symbol'],start_date,end_date)
            except Exception as e:
                print(f"Error: {e} in underlying agg for FINAL {row['symbol']} of {row['strategy']}")
                return [], []
            
    underlying_agg_data.rename(columns={'o':'underlying_price'}, inplace=True)
    try:
        options_agg_data = polygon_optiondata(row['contract_ticker'], start_date, end_date)
        enriched_df = pd.merge(options_agg_data, underlying_agg_data[['date', 'underlying_price']], on='date', how='left')
        enriched_df.dropna(inplace=True)
        enriched_options_aggregates.append(enriched_df)
        options.append(strike)
    except Exception as e:
        print(f"Error: {e} in options agg for {row['symbol']} of {row['strategy']}")
        print(strike)
    return enriched_options_aggregates, options

def create_datetime_index(start_date, end_date):
    print("DATE TIME INDEX")
    print(start_date)
    print(end_date)
    datetime_index = pd.date_range(start_date, end_date, freq='15min', name = 'Time')
    days = []
    for time in datetime_index:
        convertedtime = time.strftime('%Y-%m-%d %H:%M:%S')
        finaldate = datetime.strptime(convertedtime, '%Y-%m-%d %H:%M:%S')
        days.append(finaldate)
    results = pd.DataFrame(index=days)
    return days, datetime_index, results


def build_options_df(contracts, row):
    if row['symbol'] in ["GOOG","GOOGL","NVDA","AMZN","TSLA"]:
        last_price = get_last_price(row)
        row['o'] = last_price
    df = pd.DataFrame(contracts, columns=['contract_symbol'])
    df['underlying_symbol'] = row['symbol']
    df['option_type'] = row['side']
    try:
        df['strike'] = df.apply(lambda x: extract_strike(x),axis=1)
    except Exception as e:
        print(f"Error: {e} building options df for {row['symbol']}")
        print(df)
        print(contracts)
        return df

    if row['side'] == "P":
        df = df.loc[df['strike']< row['o']].reset_index(drop=True)
        df = df.sort_values('strike', ascending=False)
        # print(df)
        # breakkk
    elif row['side'] == "C":
        df = df.loc[df['strike'] > row['o']].reset_index(drop=True)
        df = df.sort_values('strike', ascending=True)
        # print(df)
        # breakkk
    
    return df


def extract_strike(row):
    str = row['contract_symbol'].split(f"O:{row['underlying_symbol']}")[1]
    if row['option_type'] == 'P':
        str = str.split('P')[1]
    elif row['option_type'] == 'C':
        str = str.split('C')[1]
    strike = str[:-3]
    for i in range(len(strike)):
        if strike[i] == '0':
            continue
        else:
            return int(strike[i:])
        
    return 0


#########
#########
#########
######## MOMENTUM STRATEGIES
#########
#########
#########

def tda_PUT_1D_CDVOLVARVC(polygon_df, simulation_date, quantity, config, row, order_num):
    order_num = order_num + 1
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_pct = (row['target_pct'] * float(row['return_vol_10D']))
    Floor_pct = (-config['volatility_threshold'] * target_pct)
    if order_num > 3:
        order_num = 3
    vc_config = {
        1: 100,
        2: 120,
        3: 140,
    }
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        # Floor_pct -= underlying_gain
        hour = row['date'].hour
        minute = row['date'].minute

        if deriv_pct_change > vc_config[order_num]:
            Floor_pct = (.8*underlying_gain)
            if pct_change >= Floor_pct:
                sell_code = 1
                reason = f"VCSell{order_num}"
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)  
                return sell_dict


        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])
        print({"floor_pct": Floor_pct, "max_value": min_value, "pct_change": pct_change,"current_price": row['underlying_price'],
        "purchase_price": open_price, "option_symbol": row['ticker'], "day_diff": day_diff, "max_deriv_value": max_deriv_value,
        "model": "TDA_CALL_1D_CDVOL", "target_pct": target_pct, "spread_position": order_num, "date": row['date']})        

        sell_code = 0
        if day_diff < 1:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if hour == 15 and minute == 45:
                sell_code = 7
                reason = "End of day, sell."
            elif current_weekday == 4 and hour >= 10:
                sell_code = 8
                reason = "Friday Cutoff"
            elif pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= target_pct:
                Floor_pct = (.8*underlying_gain)
                if pct_change >= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_1D_CDVOLVARVC(polygon_df, simulation_date, quantity, config, row, order_num):
    order_num = order_num + 1
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_pct = (row['target_pct'] * float(row['return_vol_10D']))
    Floor_pct = (-config['volatility_threshold'] * target_pct)
    if order_num > 4:
        order_num = 4
    vc_config = {
        1: 100,
        2: 120,
        3: 140,
        4: 500
    }
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        hour = row['date'].hour
        minute = row['date'].minute
        # Floor_pct += underlying_gain

        if deriv_pct_change > vc_config[order_num]:
            Floor_pct = (.8*underlying_gain)
            if pct_change <= Floor_pct:
                reason = f"VCSell{order_num}"
                sell_code = 1
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)  
                return sell_dict


        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])
        print({"floor_pct": Floor_pct, "max_value": max_value, "pct_change": pct_change,"current_price": row['underlying_price'],
        "purchase_price": open_price, "option_symbol": row['ticker'], "day_diff": day_diff, "max_deriv_value": max_deriv_value,
        "model": "TDA_CALL_1D_CDVOL", "target_pct": target_pct, "spread_position": order_num, "date": row['date']})

        sell_code = 0
        if day_diff < 1:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1 :
            if hour == 15 and minute == 45: 
                sell_code = 7
                reason = "End of day, sell."
            elif current_weekday == 4 and hour >= 10:
                sell_code = 8
                reason = "Friday Cutoff"
            elif pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= target_pct:
                Floor_pct = (.8*underlying_gain)
                if pct_change <= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
            # elif pct_change < (.5*(target_pct)):
            #     sell_code = 5
            #     reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict
        
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict


##########
##########
##########
########## STRATEGY HELPERS
##########
##########
##########

def build_trade_analytics(row, polygon_df, derivative_open_price, index, quantity, sell_code):
    trade_dict = {}
    before_df = polygon_df.iloc[:index]
    after_df = polygon_df.iloc[index:]
    trade_dict['max_value_before'] = before_df['h'].max()
    trade_dict['max_value_before_idx'] = before_df['h'].idxmax()
    trade_dict['max_value_before_date'] = before_df.loc[trade_dict['max_value_before_idx']]['date'].strftime("%Y-%m-%d %H:%M")
    trade_dict['max_value_before_pct_change'] = ((trade_dict['max_value_before'] - derivative_open_price)/derivative_open_price)

    if len(after_df) > 0:
        trade_dict['max_value_after'] = after_df['h'].max()
        trade_dict['max_value_after_idx'] = after_df['h'].idxmax()
        trade_dict['max_value_after_date'] = after_df.loc[trade_dict['max_value_after_idx']]['date'].strftime("%Y-%m-%d %H:%M")
        trade_dict['max_value_after_pct_change'] = ((trade_dict['max_value_after'] - derivative_open_price)/derivative_open_price)
    else:
        trade_dict['max_value_after'] = None
        trade_dict['max_value_after_idx'] = None
        trade_dict['max_value_after_date'] = None
        trade_dict['max_value_after_pct_change'] = None

    trade_dict["close_price"] = row['o']
    trade_dict["close_datetime"] = row['date'].to_pydatetime()
    trade_dict["quantity"] = quantity
    trade_dict["contract_cost"] = (row['o']*100)
    trade_dict["option_symbol"] = row['ticker']
    trade_dict["sell_code"] = sell_code
    return trade_dict


##########
##########
##########
########## POLYGON HELPERS
##########
##########
##########

KEY = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"

def polygon_stockdata_inv(symbol, from_date, to_date):
    # from_date = from_date - timedelta(minutes=15)
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/15/minute/{from_stamp}/{to_stamp}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    stock_df = pd.DataFrame(response_data['results'])
    stock_df['t'] = stock_df['t'].apply(lambda x: int(x/1000))
    stock_df['date'] = stock_df['t'].apply(lambda x: convert_timestamp_est(x))
    stock_df['time'] = stock_df['date'].apply(lambda x: x.time())
    stock_df['hour'] = stock_df['date'].apply(lambda x: x.hour)
    stock_df['minute'] = stock_df['date'].apply(lambda x: x.minute)
    stock_df['ticker'] = symbol

    stock_df = stock_df[stock_df['hour'] < 16]
    stock_df = stock_df.loc[stock_df['time'] >= datetime.strptime("09:30:00", "%H:%M:%S").time()]
    return stock_df

def polygon_optiondata(options_ticker, from_date, to_date):
    #This is for option data
    # from_date = from_date - timedelta(minutes=15)
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)

    url = f"https://api.polygon.io/v2/aggs/ticker/{options_ticker}/range/15/minute/{from_stamp}/{to_stamp}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)
    response_data = json.loads(response.text)

    res_option_df = pd.DataFrame(response_data['results'])
    res_option_df['t'] = res_option_df['t'].apply(lambda x: int(x/1000))
    res_option_df['date'] = res_option_df['t'].apply(lambda x: convert_timestamp_est(x))
    res_option_df['time'] = res_option_df['date'].apply(lambda x: x.time())
    res_option_df['hour'] = res_option_df['date'].apply(lambda x: x.hour)
    res_option_df['ticker'] = options_ticker

    res_option_df =res_option_df[res_option_df['hour'] < 16]
    res_option_df =res_option_df.loc[res_option_df['time'] >= datetime.strptime("09:30:00", "%H:%M:%S").time()]
    return res_option_df

def get_last_price(row):
    aggs = stock_aggs(row['symbol'], row['date'], row['date'])
    agg = aggs.loc[aggs['hour'] == row['hour']]
    agg = agg.loc[agg['minute'] == 0]   
    return agg['o'].values[0]


def stock_aggs(symbol,from_date, to_date):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/15/minute/{from_date}/{to_date}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    stock_df = pd.DataFrame(response_data['results'])
    stock_df['t'] = stock_df['t'].apply(lambda x: int(x/1000))
    stock_df['date'] = stock_df['t'].apply(lambda x: convert_timestamp_est(x))
    stock_df['time'] = stock_df['date'].apply(lambda x: x.time())
    stock_df['hour'] = stock_df['date'].apply(lambda x: x.hour)
    stock_df['minute'] = stock_df['date'].apply(lambda x: x.minute)
    stock_df['ticker'] = symbol

    stock_df = stock_df[stock_df['hour'] < 16]
    stock_df = stock_df.loc[stock_df['time'] >= datetime.strptime("09:30:00", "%H:%M:%S").time()]
    return stock_df

def execute_polygon_call(url):
    session = setup_session_retries()
    response = session.request("GET", url, headers={}, data={})
    return response 

class CustomRetry(Retry):
    def is_retry(self, method, status_code, has_retry_after=False):
        """ Return True if we should retry the request, otherwise False. """
        if status_code != 200:
            return True
        return super().is_retry(method, status_code, has_retry_after)
    
def setup_session_retries(
    retries: int = 3,
    backoff_factor: float = 0.05,
    status_forcelist: tuple = (500, 502, 504),
):
    """
    Sets up a requests Session with retries.
    
    Parameters:
    - retries: Number of retries before giving up. Default is 3.
    - backoff_factor: A factor to use for exponential backoff. Default is 0.3.
    - status_forcelist: A tuple of HTTP status codes that should trigger a retry. Default is (500, 502, 504).

    Returns:
    - A requests Session object with retry configuration.
    """
    retry_strategy = CustomRetry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]),
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

##########
##########
##########
########## STANDARD HELPERS
##########
##########
##########


def get_business_days(transaction_date, current_date):
    """
    Returns the number of business days (excluding weekends) between two dates. For now we
    aren't considering market holidays because that is how the training data was generated.
    """

    days = (current_date - transaction_date).days 
    complete_weeks = days // 7
    remaining_days = days % 7

    # Calculate the number of weekend days in the complete weeks
    weekends = complete_weeks * 2

    # Adjust for the remaining days
    if remaining_days:
        start_weekday = transaction_date.weekday()
        end_weekday = current_date.weekday()

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

def get_day_diff(transaction_date, current_date):
    transaction_dt = datetime(transaction_date.year, transaction_date.month, transaction_date.day)
    current_dt = datetime(current_date.year, current_date.month, current_date.day)

    days_between = 0
    while transaction_dt < current_dt:
        transaction_dt += timedelta(days=1)
        if transaction_dt.weekday() < 5:
            days_between += 1
    return days_between, current_dt.weekday()

def build_spread(chain_df, spread_length, cp):
    contract_list = []
    chain_df = chain_df.loc[chain_df['inTheMoney'] == False].reset_index(drop=True)
    if cp == "calls":
        chain_df = chain_df.iloc[:spread_length]
    if cp == "puts":
        chain_df = chain_df.iloc[-spread_length:]
    if len(chain_df) < spread_length:
        return contract_list
    for index, row in chain_df.iterrows():
        temp_object = {
            "contractSymbol": row['contractSymbol'],
            "strike": row['strike'],
            "lastPrice": row['lastPrice'],
            "bid": row['bid'],
            "ask": row['ask'],
            "quantity": 1
        }
        contract_list.append(temp_object)
    
    return contract_list


def convert_timestamp_est(timestamp):
    # Create a naive datetime object from the UNIX timestamp
    dt_naive = datetime.utcfromtimestamp(timestamp)
    # Convert the naive datetime object to a timezone-aware one (UTC)
    dt_utc = pytz.utc.localize(dt_naive)
    # Convert the UTC datetime to EST
    dt_est = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    return dt_est



###########
###########
###########
########### BACKTESTING HELPERS Pt.2
###########
###########
###########



def configure_trade_data(df,config):
    index = df.loc[df['symbol'].isin(["IWM","SPY","QQQ"])]
    stocks = df.loc[df['symbol'].isin(["IWM","SPY","QQQ"]) == False]


    one = stocks.loc[stocks['prediction_horizon'] == "1"]
    three = stocks.loc[stocks['prediction_horizon'] == "3"]
    one_idx = index.loc[index['prediction_horizon'] == "1"]
    three_idx = index.loc[index['prediction_horizon'] == "3"]

    filt_one = one.loc[one['day_of_week'].isin([2,3])]
    filt_three = three.loc[three['day_of_week'].isin([0,1,2])]

    one_idxF = one_idx.loc[one_idx['day_of_week'].isin([0,1,2,3])]
    three_idxF = three_idx.loc[three_idx['day_of_week'].isin([0,1,2])]

    trade_df = pd.concat([filt_one,filt_three,one_idxF,three_idxF])
    # trade_df = pd.concat([stocks,index])
    return trade_df

def create_portfolio_date_list(start_date, end_date):
    sy, sm, sd = start_date.split('/')
    ey, em, ed = end_date.split('/')
    start_time = datetime(int(sy), int(sm), int(sd), 9, 30)
    end_time = datetime(int(ey), int(em), int(ed), 16, 0)
    end_date = create_end_date(end_time, 4)
    date_list, _, _  = create_datetime_index(start_time, end_date)
    return date_list

def extract_results_dict(positions_list, config, quantities):
    results_dicts = []
    transactions = positions_list['transactions']
    for transaction in transactions:
        try:
            sell_dict = transaction['sell_info']
            buy_dict = transaction['buy_info']
            symbol = sell_dict['option_symbol']
            try:
                option_quantity = quantities[symbol]
            except KeyError as e:
                continue
            results_dicts.append(
            {
                "price_change": transaction['price_change'], "pct_gain": transaction['pct_gain'],
                "total_gain": transaction['total_gain'], "open_trade_dt": transaction['open_trade_dt'], 
                "close_trade_dt": transaction['close_trade_dt'],"max_gain_before": sell_dict['max_value_before_pct_change'],
                "max_gain_after": sell_dict['max_value_after_pct_change'],"option_symbol": sell_dict['option_symbol'],
                "max_value_before_date": sell_dict['max_value_before_date'], "max_value_after_date": sell_dict['max_value_after_date'],
                "max_value_before_idx": sell_dict['max_value_before_idx'], "max_value_after_idx": sell_dict['max_value_after_idx'],
                "sell_code": sell_dict['sell_code'], "quantity": option_quantity,
            })
        except Exception as e:
            print(f"Error: {e} in extracting results dict")
            print(transaction)
            continue
    return results_dicts

# def configure_regression_predictions(backtest_data, config):
#     backtest_data = backtest_data[backtest_data['threeD_stddev50'] > 0]
#     forecast_vols = []
#     for index, row in backtest_data.iterrows():
#         if row['strategy'] in ['BFC','BFP']:
#             forecast_vols.append(abs(row['forecast']/row['threeD_stddev50']))
#         else:
#             forecast_vols.append(abs(row['forecast']/row['oneD_stddev50']))
#     backtest_data['forecast_vol'] = forecast_vols
#     # backtest_data['forecast_vol'] = backtest_data.apply(lambda x: x['forecast']/x['threeD_stddev50'] if x['strategy'in ['BFC','BFP']] else x['forecast']/x['oneD_stddev50'],axis=1)
#     data = backtest_data.loc[backtest_data['forecast_vol'] > config['volatility_threshold']].reset_index(drop=True)
#     return data


###########
###########
###########
########### BACKTESTING ORCHESTRATION
###########
###########
###########


# def build_backtest_data(file_name,strategies,config):
#     full_purchases_list = []
#     full_positions_list = []
#     full_sales_list = []

#     dfs = []
#     for strategy in strategies:
#         name, prediction_horizon = strategy.split(":")
#         data = pd.read_csv(f'/Users/diz/Documents/Projects/backtesting_data/{config["dataset"]}/{name}/{file_name}.csv')
#         data['prediction_horizon'] = prediction_horizon
#         dfs.append(data)
    
#     backtest_data = pd.concat(dfs,ignore_index=True)
#     # backtest_data = backtest_data[backtest_data['probabilities'] > config['probability']]
#     if config['model_type'] == "reg":
#         predictions = configure_regression_predictions(backtest_data,config)
#         filtered_by_date = configure_trade_data(predictions,config)
#     elif config['model_type'] == "cls":
#         predictions = backtest_data.loc[backtest_data['predictions'] == 1]
#         filtered_by_date = configure_trade_data(predictions,config)
    
#     ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
#     positions_list = simulate_trades_invalerts(filtered_by_date,config)
#     full_positions_list.extend(positions_list)

#     return positions_list

# def run_trades_simulation(full_positions_list,start_date,end_date,config,period_cash):
#     full_date_list = create_portfolio_date_list(start_date, end_date)
#     if config['scaling'] == "dynamicscale":
#         portfolio_df, passed_trades_df, positions_taken, positions_dict = simulate_portfolio_DS(
#             full_positions_list, full_date_list,portfolio_cash=period_cash, risk_unit=config['risk_unit'],put_adjustment=config['put_pct'],
#             config=config,results_dict_func=extract_results_dict
#             )
#     elif config['scaling'] == "steadyscale":
#         portfolio_df, passed_trades_df, positions_taken, positions_dict = simulate_portfolio(
#             full_positions_list, full_date_list,portfolio_cash=period_cash, risk_unit=config['risk_unit'],put_adjustment=config['put_pct'],
#             config=config,results_dict_func=helper.extract_results_dict
#             )
#     positions_df = pd.DataFrame.from_dict(positions_taken)
#     return portfolio_df, positions_df

# def backtest_orchestrator(start_date,end_date,file_names,strategies,local_data,config,period_cash):
#     #  build_backtest_data(file_names[0],strategies,config)

#     if not local_data:
#         cpu_count = os.cpu_count()
#         # merged_positions = build_backtest_data(file_names[0],strategies,config)
#         with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
#             # Submit the processing tasks to the ThreadPoolExecutor
#             processed_weeks_futures = [executor.submit(build_backtest_data,file_name,strategies,config) for file_name in file_names]

#         # Step 4: Retrieve the results from the futures
#         processed_weeks_results = [future.result() for future in processed_weeks_futures]

#         merged_positions = []
#         for week_results in processed_weeks_results:
#             merged_positions.extend(week_results)

#         merged_df = pd.DataFrame.from_dict(merged_positions)
#         merged_df.to_csv(f'/Users/charlesmiller/Documents/backtesting_data/merged_positions.csv', index=False)
#     else:
#         merged_positions = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/merged_positions.csv')
#         merged_positions = merged_positions.to_dict('records')

#     full_df = pd.DataFrame.from_dict(merged_positions)
#     portfolio_df, positions_df = run_trades_simulation(merged_positions, start_date, end_date, config, period_cash)
#     return portfolio_df, positions_df, full_df