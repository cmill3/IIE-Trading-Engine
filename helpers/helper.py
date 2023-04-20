import helpers.tradier as trade
from datetime import datetime

dt = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
current_date = datetime.now().strftime("%Y-%m-%d")

def date_performance_check(row, base_url, access_token):
    print(row)
    # date_delta = current_date - row['position_open_date']
    current_price = trade.get_last_price(row['underlying_symbol'], base_url, access_token)
    to_sell, reason = evaluate_performance(current_price, row)
    if to_sell or current_date > row['sellby_date']:
        order_dict = {
            "contract": row['contract'],
            "underlying_symbol": row['underlying_symbol'],
            "quantity": row['quantity'], 
            "reason": reason,
        }
        return True, order_dict
    else:
        return False, {}
    
def evaluate_performance(current_price, row):
    price_delta = (current_price - row['purchase_price'])/row['purchase_price']
    strategy = row['trading_strategy']
    if strategy == 'maP' or strategy == 'day_losers':
        if price_delta >= 0.05:
            return True, "hit point of no confidence"
        elif price_delta <= -0.05:
            return True, "hit exit target"
    elif strategy == 'day_gainers' or strategy == 'most_actives':
        if price_delta >= 0.05:
            return True, "hit exit target"
        elif price_delta <= -0.05:
            return True, "hit point of no confidence"
    else:
        return False, "no sale"
    
    