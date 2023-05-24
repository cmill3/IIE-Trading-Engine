from datetime import datetime, timedelta
import requests
import pandas as pd
import json

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

def calculate_dt_features(transaction_date, sell_by):
    print(transaction_date, sell_by)
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

if __name__ == "__main__":
    day = get_business_days("2023-05-18T17:03:29.283Z")
    print(day)