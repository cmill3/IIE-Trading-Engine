import helpers.tradier as trade
from datetime import datetime, timedelta

dt = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
current_date = datetime.now().strftime("%Y-%m-%d")

def date_performance_check(row, base_url, access_token):
    # date_delta = current_date - row['position_open_date']
    current_price = trade.get_last_price(base_url,access_token,row['underlying_symbol'])
    to_sell, reason = evaluate_performance(current_price, row)
    # sellby_date = calculate_sellby_date(current_date, 5)
    print(to_sell, current_date, row['sellby_date'])
    if to_sell or current_date > row['sellby_date']:
        order_dict = {
            "contract": row['option_symbol'],
            "underlying_symbol": row['underlying_symbol'],
            "quantity": row['quantity'], 
            "reason": reason,
        }
        return True, order_dict
    else:
        return False, {}
    
def evaluate_performance(current_price, row):
    price_delta = (float(current_price) - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    print("PRICE_DIFF",price_delta)
    strategy = row['trading_strategy']
    print(strategy)
    if strategy == 'maP':
        if price_delta >= 0.05:
            return True, "hit point of no confidence"
        elif price_delta <= -0.05:
            return True, "hit exit target"
        else:
            print("else")
            return False, "no sale"
    elif strategy == 'day_losers':
        if price_delta >= 0.06:
            return True, "hit point of no confidence"
        elif price_delta <= -0.06:
            return True, "hit exit target"
        else:
            print("else")
            return False, "no sale"
    elif strategy == 'day_gainers' or strategy == 'most_actives':
        if price_delta >= 0.05:
            return True, "hit exit target"
        elif price_delta <= -0.05:
            return True, "hit point of no confidence"
        else:
            print("else")
            return False, "no sale"
    else:
        print("else")
        return False, "no sale"
    

def calculate_sellby_date(current_date, trading_days_to_add): #End date, n days later for the data set built to include just trading days, but doesnt filter holiday
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    return current_date
    