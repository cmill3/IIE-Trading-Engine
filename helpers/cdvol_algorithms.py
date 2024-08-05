from datetime import datetime, timedelta
import logging
from helpers.helper import get_business_days, calculate_floor_pct, get_derivative_max_value, pull_model_config
import pytz
import warnings
import json
warnings.filterwarnings('ignore')

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

def TDA_PUT_1D_CDVOL(row, current_price,vol):
    model_config = pull_model_config(row['trading_strategy'])
    min_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = (model_config['target_value'] * float(row['return_vol_10D']))
    max_deriv_value = pc_max_value(row)
    minute = now_est.minute

    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100
    underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    Floor_pct = (-vol)* target_pct
    spread_position = float(row['spread_position'])
    vc_config = determine_vc_config(row)
    day_diff = get_business_days(row['order_transaction_date'])

    log_entry = json.dumps({
        "floor_pct": Floor_pct, "min_value": min_value, "pct_change": pct_change,"current_price": current_price,
        "purchase_price": open_price, "option_symbol": row['option_symbol'], "day_diff": day_diff, "max_deriv_value": max_deriv_value,
        "model": "TDA_PUT_1D_CDVOL", "target_pct": target_pct, "spread_position": spread_position
    })
    logger.info(log_entry)

    if deriv_pct_change > vc_config[spread_position]:
        Floor_pct = (.8*underlying_gain)
        if pct_change >= Floor_pct:
            sell_code = 1
            reason = f"VCSell{spread_position}"
            logger.info(f"{row['position_id']} {row['spread_position']} {reason} {pct_change} {Floor_pct}")
            return sell_code, f"VCSell{spread_position}"
    

    sell_code = 0
    reason = ""
    if day_diff < 1:
        if pct_change >= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
            logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
    elif day_diff > 1:
        sell_code = 3
        reason = "Held through confidence."
        logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
        return sell_code, reason
    elif day_diff == 1:
        if hour == 15 and minute == 45: 
            sell_code = 7
            reason = "End of day, sell."
            logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
        elif current_weekday == 4 and hour >= 10:
            sell_code = 8
            reason = "Friday Cutoff"
            logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
        elif pct_change > Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
            logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
        elif pct_change <= target_pct:
            Floor_pct = (.8*underlying_gain)
            if pct_change >= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
                logger.info(f"{row['position_id']} {row['spread_position']} {reason} {pct_change} {Floor_pct}")

    return sell_code, reason

def TDA_CALL_1D_CDVOL(row, current_price,vol):
    model_config = pull_model_config(row['trading_strategy'])
    max_value = calculate_floor_pct(row)
    open_price = row['underlying_purchase_price']
    target_pct = (model_config['target_value'] * float(row['return_vol_10D']))
    max_deriv_value = pc_max_value(row)
    minute = now_est.minute

    deriv_pct_change = ((max_deriv_value - float(row['avg_fill_price_open']))/float(row['avg_fill_price_open']))*100
    underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
    pct_change = (current_price - float(open_price))/float(open_price)
    Floor_pct = (-vol)* target_pct
    spread_position = float(row['spread_position']) 
    vc_config = determine_vc_config(row)
    day_diff = get_business_days(row['order_transaction_date'])

    log_entry = json.dumps({
        "floor_pct": Floor_pct, "max_value": max_value, "pct_change": pct_change,"current_price": current_price,
        "purchase_price": open_price, "option_symbol": row['option_symbol'], "day_diff": day_diff, "max_deriv_value": max_deriv_value,
        "model": "TDA_CALL_1D_CDVOL", "target_pct": target_pct, "spread_position": spread_position
    })
    logger.info(log_entry)

    if deriv_pct_change > vc_config[spread_position]:
        Floor_pct = (.8*underlying_gain)
        if pct_change <= Floor_pct:
            reason = f"VCSell{spread_position}"
            sell_code = 1
            logger.info(f"{row['position_id']} {row['spread_position']} {reason} {pct_change} {Floor_pct}")
            return sell_code, f"VCSell{spread_position}"
    
    # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {open_price} for {row['option_symbol']}")

    sell_code = 0
    reason = ""
    if day_diff < 1:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
            logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
    elif day_diff > 1:
        sell_code = 3
        reason = "Held through confidence."
        logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
        return sell_code, reason
    elif day_diff == 1 :
        if hour == 15 and minute == 45: 
            sell_code = 7
            reason = "End of day, sell."
            logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
        elif current_weekday == 4 and hour >= 10:
            sell_code = 8
            reason = "Friday Cutoff"
            logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
        elif pct_change < Floor_pct:
            sell_code = 4
            reason = "Hit point of no confidence, sell."
            logger.info(f"{row['position_id']} {row['spread_position']} {reason}")
        elif pct_change >= target_pct:
            Floor_pct = (.8*underlying_gain)
            if pct_change <= Floor_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
                logger.info(f"{row['position_id']} {row['spread_position']} {reason} {pct_change} {Floor_pct}")

    return sell_code, reason

def determine_vc_config(row):
    if current_weekday == 0:
        vc_config = {
            0: 80,
            1: 100,
            2: 120,
            3: 140
        }
    elif current_weekday == 1:
        vc_config = {
            0: 90,
            1: 110,
            2: 120,
            3: 150
        }
    elif current_weekday >= 2:
        vc_config = {
            0: 100,
            1: 120,
            2: 140,
            3: 160
        }
    return vc_config