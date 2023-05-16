import helpers.tradier as trade
from datetime import datetime, timedelta
from trading_algorithms import time_decay_alpha_gainers_v0, time_decay_alpha_ma_v0, time_decay_alpha_losers_v0, time_decay_alpha_maP_v0

now = datetime.now()
dt = now.strftime("%Y-%m-%d_%H:%M:%S")
current_date = now.strftime("%Y-%m-%d")

def date_performance_check(row, base_url, access_token):
    current_price = trade.get_last_price(base_url,access_token,row['underlying_symbol'])
    sell_code, reason = evaluate_performance(current_price, row)
    if sell_code == 2 or current_date > row['sellby_date']:
        # order_dict = {
        #     "contract": row['option_symbol'],
        #     "underlying_symbol": row['underlying_symbol'],
        #     "quantity": row['quantity'], 
        #     "reason": reason,
        # }
        return True, {}
    else:
        return False, {}
    
def evaluate_performance(current_price, row):
    strategy = row['trading_strategy']
    if strategy == 'maP':
        sell_code, reason = time_decay_alpha_maP_v0(row, current_price)
    elif strategy == 'day_losers':
        sell_code, reason = time_decay_alpha_losers_v0(row, current_price)
    elif strategy == 'day_gainers':
        sell_code, reason = time_decay_alpha_gainers_v0(row, current_price)
    elif strategy == 'most_actives':
       sell_code, reason = time_decay_alpha_ma_v0(row, current_price)
    return sell_code, reason
    

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
    