import helpers.tradier as trade
from datetime import datetime

dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
current_date = datetime.now().strftime("%Y-%m-%d")

def date_performance_check(row, base_url, access_token):
    # date_delta = current_date - row['position_open_date']
    current_strike = trade.get_last_price(row['underlying_symbol'], base_url, access_token)
    price_delta = current_strike - row['purchase_strike']
    percent_change = int((price_delta / row['purchase_strike']) * 100)
    
    if percent_change >= 5 or percent_change <= -5 or current_date > row['sellby_date']:
        order_dict = {
            "contract": row['contract'],
            "underlying_symbol": row['underlying_symbol'],
            "quantity": row['quantity'], 
        }
        return True, order_dict
    else:
        return False, {}