from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import pytz
import boto3


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
    print(response.text)
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
    except:  
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
   if row['trading_strategy'] in ['bfP','indexP',"bfP_1d","indexP_1d"]:
       return low_price
   elif row['trading_strategy'] in ['bfC','indexC',"bfC_1d","indexC_1d"]:
       return high_price
   else:
        return 0


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
    
    # date_dict ={
    #     'month': month,
    #     'day': day,
    #     'year': str(temp_year)
    #     }
    
    return f"{temp_year}/{month}/{day}/{date.hour}"

def convert_timestamp_est(timestamp):
    # Create a naive datetime object from the UNIX timestamp
    dt_naive = datetime.utcfromtimestamp(timestamp)
    # Convert the naive datetime object to a timezone-aware one (UTC)
    dt_utc = pytz.utc.localize(dt_naive)
    # Convert the UTC datetime to EST
    dt_est = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    return dt_est

# if __name__ == "__main__":
#     x = calculate_floor_pct({'order_transaction_date': '2023-05-30T18:03:06.294Z', 'underlying_symbol': 'AR', 'trading_strategy': 'day_losers'})
#     print(x)
#     print(type(x))
