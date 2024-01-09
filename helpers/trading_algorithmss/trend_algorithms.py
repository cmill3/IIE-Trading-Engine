from datetime import datetime, timedelta
import logging
from helpers.helper import get_business_days, polygon_call_stocks, calculate_floor_pct, get_derivative_max_value
from helpers.constants import ALGORITHM_CONFIG
import numpy as np  
import math
import ast
import pytz

logger = logging.getLogger()
logger.setLevel(logging.INFO)
est = pytz.timezone('US/Eastern')
now_est = datetime.now(est)

def tda_PUT_3D_stdclsAGG(row, current_price):
    min_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = ALGORITHM_CONFIG[row['trading_strategy']]['target_pct']
    max_deriv_value = get_derivative_max_value(row)
    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100

    underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    Floor_pct = (row['threeD_stddev50'] * ALGORITHM_CONFIG[row['trading_strategy']]['volatility_threshold'])

    if deriv_pct_change > 300:
        sell_code = "VCSell"
        return sell_code
    
    logger.info(f"Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['option_symbol']}")
    day_diff = get_business_days(row['order_transaction_date'])


    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change >= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
    elif day_diff > 3:
        sell_code = 3
        reason = "Held through confidence."
        # sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
        return sell_code, reason
    elif day_diff >= 2:
        if pct_change < (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change < target_pct:
            Floor_pct = (.75*underlying_gain)

        if pct_change > Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
        elif pct_change <= target_pct:
            Floor_pct = (.9*underlying_gain)
            if pct_change >= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
        elif pct_change > (.5*(target_pct)):
            sell_code = 5
            reason = "Failed momentum gate, sell."
        elif now_est.hour == 15:
            sell_code = 7
            reason = "End of day, sell."
        
    return sell_code, reason

def tda_CALL_3D_stdclsAGG(row, current_price):
    max_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = ALGORITHM_CONFIG[row['trading_strategy']]['target_pct']
    max_deriv_value = get_derivative_max_value(row)
    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100

    underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    Floor_pct = (row['threeD_stddev50'] * ALGORITHM_CONFIG[row['trading_strategy']]['volatility_threshold'])

    if deriv_pct_change > 300:
        sell_code = "VCSell"
        return sell_code
    
    logger.info(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['option_symbol']}")
    day_diff = get_business_days(row['order_transaction_date'])


    sell_code = 0
    reason = ""
    if day_diff < 1:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
    elif day_diff > 1:
        sell_code = 3
        reason = "Held through confidence."
        # sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
        return sell_code, reason
    elif day_diff == 1:
        if pct_change > (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change > target_pct:
            Floor_pct = (.75*underlying_gain)

        if pct_change < Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
        elif pct_change >= target_pct:
            Floor_pct = (.9*underlying_gain)
            if pct_change >= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
        elif pct_change < (.5*(target_pct)):
            sell_code = 5
            reason = "Failed momentum gate, sell."
        elif now_est.hour == 15:
            sell_code = 7
            reason = "End of day, sell."
        
    return sell_code, reason

def tda_PUT_1D_stdclsAGG(row, current_price):
    min_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = ALGORITHM_CONFIG[row['trading_strategy']]['target_pct']
    max_deriv_value = get_derivative_max_value(row)
    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100

    underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    Floor_pct = (row['oneD_stddev50'] * ALGORITHM_CONFIG[row['trading_strategy']]['volatility_threshold'])

    if deriv_pct_change > 300:
        sell_code = "VCSell"
        return sell_code
    
    logger.info(f"Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['option_symbol']}")
    day_diff = get_business_days(row['order_transaction_date'])


    sell_code = 0
    reason = ""
    if day_diff < 1:
        if pct_change >= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
    elif day_diff > 1:
        sell_code = 3
        reason = "Held through confidence."
        # sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
        return sell_code, reason
    elif day_diff == 1:
        if pct_change < (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change < target_pct:
            Floor_pct = (.75*underlying_gain)

        if pct_change > Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
        elif pct_change <= target_pct:
            Floor_pct = (.9*underlying_gain)
            if pct_change >= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
        elif pct_change > (.5*(target_pct)):
            sell_code = 5
            reason = "Failed momentum gate, sell."
        elif now_est.hour == 15:
            sell_code = 7
            reason = "End of day, sell."
        
    return sell_code, reason

def tda_CALL_1D_stdclsAGG(row, current_price):
    max_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = ALGORITHM_CONFIG[row['trading_strategy']]['target_pct']
    max_deriv_value = get_derivative_max_value(row)
    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100

    underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    Floor_pct = (row['oneD_stddev50'] * ALGORITHM_CONFIG[row['trading_strategy']]['volatility_threshold'])

    if deriv_pct_change > 300:
        sell_code = "VCSell"
        return sell_code
    
    logger.info(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['option_symbol']}")
    day_diff = get_business_days(row['order_transaction_date'])


    sell_code = 0
    reason = ""
    if day_diff < 1:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
    elif day_diff > 1:
        sell_code = 3
        reason = "Held through confidence."
        # sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
        return sell_code, reason
    elif day_diff == 1:
        if pct_change > (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change > target_pct:
            Floor_pct = (.75*underlying_gain)

        if pct_change < Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
        elif pct_change >= target_pct:
            Floor_pct = (.9*underlying_gain)
            if pct_change >= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
        elif pct_change < (.5*(target_pct)):
            sell_code = 5
            reason = "Failed momentum gate, sell."
        elif now_est.hour == 15:
            sell_code = 7
            reason = "End of day, sell."
        
    return sell_code, reason