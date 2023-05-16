import datetime
import logging
from helper import calculate_hour_features

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def time_decay_alpha_gainers_v0(row, current_price):
    Ipct = -2
    Tpct = 5
    pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    ho, hc = calculate_hour_features(row['transaction_date'], row['sellby_date'])
    sell_code = 0
    reason = ""

    if ho < 8:
        if pct_change < Tpct and pct_change > Ipct:
            sell_code = 0
            reason = "Not enough movement, hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Tpct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
    elif ho >= 8:
        if pct_change < Ipct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Tpct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif (pct_change * .5(hc)) < ho:
            sell_code = 2
            reason = "Failed momentum gate, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        else:
            sell_code = 0
            reason = "Not enough movement, hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")

    return sell_code, reason

def time_decay_alpha_ma_v0(row, current_price):
    Ipct = -2
    Tpct = 5
    pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    ho, hc = calculate_hour_features(row['transaction_date'], row['sellby_date'])
    sell_code = 0
    reason = ""

    if ho < 8:
        if pct_change < Tpct and pct_change > Ipct:
            sell_code = 0
            reason = "Not enough movement, hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Tpct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
    elif ho >= 8:
        if pct_change < Ipct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Tpct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif (pct_change * .5(hc)) < ho:
            sell_code = 2
            reason = "Failed momentum gate, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        else:
            sell_code = 0
            reason = "Hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")

    return sell_code, reason

def time_decay_alpha_maP_v0(row, current_price):
    Ipct = -2
    Tpct = 5
    pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) * -1
    ho, hc = calculate_hour_features(row['transaction_date'], row['sellby_date'])
    sell_code = 0
    reason = ""

    if ho < 8:
        if pct_change < Tpct and pct_change > Ipct:
            sell_code = 0
            reason = "Not enough movement, hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Tpct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
    elif ho >= 8:
        if pct_change < Ipct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Tpct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif (pct_change * .5(hc)) < ho:
            sell_code = 2
            reason = "Failed momentum gate, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        else:
            sell_code = 0
            reason = "Hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")

    return sell_code, reason

def time_decay_alpha_losers_v0(row, current_price):
    Ipct = -2.5
    Tpct = 6
    pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) * -1
    ho, hc = calculate_hour_features(row['transaction_date'], row['sellby_date'])
    sell_code = 0
    reason = ""

    if ho < 8:
        if pct_change < Tpct and pct_change > Ipct:
            sell_code = 0
            reason = "Not enough movement, hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Tpct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
    elif ho >= 8:
        if pct_change < Ipct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Tpct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif (pct_change * .5(hc)) < ho:
            sell_code = 2
            reason = "Failed momentum gate, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        else:
            sell_code = 0
            reason = "Hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")

    return sell_code, reason

