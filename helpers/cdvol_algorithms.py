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
current_weekday = now_est.weekday()
hour = now_est.hour

def pc_max_value(row):
    try: 
        max_deriv_value = get_derivative_max_value(row)
    except Exception as e:
        print(f"DERIV CALL FAILED with {e} for {row['order_id']} in PUT_3D_stdclsAGG")
        try:
            max_deriv_value = get_derivative_max_value(row)
        except:
            print(f"DERIV CALL FAILED TWICE with {e} for {row['order_id']} in PUT_3D_stdclsAGG")
            max_deriv_value = float(row['avg_fill_price_open'])
    return max_deriv_value

def tda_PUT_3D_CDVOLAGG(row, current_price,vol):
    min_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = (ALGORITHM_CONFIG[row['trading_strategy']]['target_value'] * float(row['return_vol_10D']))
    max_deriv_value = pc_max_value(row)

    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100
    underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    Floor_pct = (vol)* float(row['return_vol_10D'])

    if deriv_pct_change > 400:
        sell_code = "VCSell"
        return sell_code, "VCSell"
    
    day_diff = get_business_days(row['order_transaction_date'])
    logger.info(f"Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {open_price} for {row['option_symbol']} day_diff: {day_diff} in PUT_3D_CDVOLAGG")


    sell_code = 0
    reason = ""
    if day_diff < 3:
        if current_weekday == 4 and hour > 12: 
            sell_code = 2
            reason = f"Friday sell. {pct_change} {Floor_pct}"
        elif pct_change >= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
    elif day_diff > 3:
        sell_code = 3
        reason = "Held through confidence."
        # sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
        return sell_code, reason
    elif day_diff == 3:
        if hour == 15 or (current_weekday == 4 and hour >= 12):
            sell_code = 7
            reason = "End of day, sell."
        elif pct_change > Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
        elif pct_change <= target_pct:
            Floor_pct = (.95*underlying_gain)
            if pct_change >= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
        elif pct_change >= (.5*(target_pct)):
            sell_code = 5
            reason = "Failed momentum gate, sell."

    return sell_code, reason

def tda_CALL_3D_CDVOLAGG(row, current_price,vol):
    max_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = (ALGORITHM_CONFIG[row['trading_strategy']]['target_value'] * float(row['return_vol_10D']))
    max_deriv_value = pc_max_value(row)

    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100
    underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    Floor_pct = (-vol) * float(row['return_vol_10D'])

    if deriv_pct_change > 400:
        sell_code = "VCSell"
        return sell_code, "VCSell"
    
    # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {open_price} for {row['option_symbol']}")
    day_diff = get_business_days(row['order_transaction_date'])
    logger.info(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {open_price} for {row['option_symbol']} day_diff {day_diff} in CALL_3D_CDVOLAGG")

    sell_code = 0
    reason = ""
    if day_diff < 3:
        if current_weekday == 4 and hour > 12: 
            sell_code = 2
            reason = f"Friday sell. {pct_change} {Floor_pct}"
        elif pct_change <= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
    elif day_diff > 3:
        sell_code = 3
        reason = "Held through confidence."
        # sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
        return sell_code, reason
    elif day_diff == 3:
        if hour == 15 or (current_weekday == 4 and hour >= 12):
            sell_code = 7
            reason = "End of day, sell."
        elif pct_change < Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
        elif pct_change >= target_pct:
            Floor_pct = (.95*underlying_gain)
            if pct_change <= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
        elif pct_change < (.5*(target_pct)):
            sell_code = 5
            reason = "Failed momentum gate, sell."

    return sell_code, reason

def tda_PUT_1D_CDVOLAGG(row, current_price,vol):
    min_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = (ALGORITHM_CONFIG[row['trading_strategy']]['target_value'] * float(row['return_vol_10D']))
    max_deriv_value = pc_max_value(row)

    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100
    underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    # Floor_pct = (row['oneD_stddev50'] * ALGORITHM_CONFIG[row['trading_strategy']]['volatility_threshold'])
    Floor_pct = (vol)* float(row['return_vol_10D'])

    if deriv_pct_change > 400:
        sell_code = "VCSell"
        return sell_code, "VCSell"
    
    day_diff = get_business_days(row['order_transaction_date'])
    logger.info(f"Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {open_price} for {row['option_symbol']} day_diff: {day_diff} in PUT_1D_CDVOLAGG")

    sell_code = 0
    reason = ""
    if day_diff < 1:
        if current_weekday == 4 and hour > 12: 
            sell_code = 2
            reason = f"Friday sell. {pct_change} {Floor_pct}"
        elif pct_change >= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
    elif day_diff > 1:
        sell_code = 3
        reason = "Held through confidence."
        # sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
        return sell_code, reason
    elif day_diff == 1:
        if hour == 15 or (current_weekday == 4 and hour >= 12):
            sell_code = 7
            reason = "End of day, sell."
        elif pct_change > Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
        elif pct_change <= target_pct:
            Floor_pct = (.95*underlying_gain)
            if pct_change >= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
        elif pct_change >= (.5*(target_pct)):
            sell_code = 5
            reason = "Failed momentum gate, sell."

    return sell_code, reason

def tda_CALL_1D_CDVOLAGG(row, current_price,vol):
    max_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = (ALGORITHM_CONFIG[row['trading_strategy']]['target_value'] * float(row['return_vol_10D']))
    max_deriv_value = pc_max_value(row)

    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100
    underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    Floor_pct = (-vol)* float(row['return_vol_10D'])

    if deriv_pct_change > 400:
        sell_code = "VCSell"
        return sell_code, "VCSell"
    
    day_diff = get_business_days(row['order_transaction_date'])
    logger.info(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {open_price} for {row['option_symbol']} day_diff: {day_diff} in CALL_1D_CDVOLAGG")

    sell_code = 0
    reason = ""
    if day_diff < 1:
        if current_weekday == 4 and hour > 12: 
            sell_code = 2
            reason = f"Friday sell. {pct_change} {Floor_pct}"
        elif pct_change <= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
    elif day_diff > 1:
        sell_code = 3
        reason = "Held through confidence."
        # sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
        return sell_code, reason
    elif day_diff == 1 :
        if hour == 15 or (current_weekday == 4 and hour >= 12):
            sell_code = 7
            reason = "End of day, sell."
        elif pct_change < Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
        elif pct_change >= target_pct:
            Floor_pct = (.95*underlying_gain)
            if pct_change <= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
        elif pct_change < (.5*(target_pct)):
            sell_code = 5
            reason = "Failed momentum gate, sell."

    return sell_code, reason