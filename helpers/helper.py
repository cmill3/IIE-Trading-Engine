import helpers.tradier as trade
from datetime import datetime, timedelta

now = datetime.now()
dt = now.strftime("%Y-%m-%d_%H:%M:%S")
current_date = now.strftime("%Y-%m-%d")
    

def calculate_sellby_date(current_date, trading_days_to_add): #End date, n days later for the data set built to include just trading days, but doesnt filter holiday
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    return current_date

def calculate_hour_features(transaction_date, sell_by):
    transaction_dt = datetime.strptime(transaction_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    sell_by_dt = datetime(int(sell_by[0:4]), int(sell_by[5:7]), int(sell_by[8:10]),20)
    ho = abs((transaction_dt - now).total_seconds()/3600)
    hc = abs((sell_by_dt - now).total_seconds()/3600)
    return ho, hc
    