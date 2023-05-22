from datetime import datetime, timedelta
import logging
from helpers.helper import calculate_hour_features, polygon_call    

logger = logging.getLogger()
logger.setLevel(logging.INFO)

### TRADING ALGORITHMS ###

def time_decay_alpha_gainers_v0(row, current_price):
    Ipct = -2
    Tpct = 5
    pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    ho, hc = calculate_hour_features(row['order_transaction_date'], row['sellby_date'])
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
        elif (pct_change * (.5*(hc))) < ho:
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
    ho, hc = calculate_hour_features(row['order_transaction_date'], row['sellby_date'])
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
        elif (pct_change * (.5*(hc))) < ho:
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
    ho, hc = calculate_hour_features(row['order_transaction_date'], row['sellby_date'])
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
        elif (pct_change * (.5*(hc))) < ho:
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
    ho, hc = calculate_hour_features(row['order_transaction_date'], row['sellby_date'])
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
        elif (pct_change * (.5*(hc))) < ho:
            sell_code = 2
            reason = "Failed momentum gate, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        else:
            sell_code = 0
            reason = "Hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")

    return sell_code, reason


### BET SIZING FUNCTIONS ###

def bet_sizer(contracts, date):
    target_cost = (.0175* pull_trading_balance())
    to_stamp = date.strftime("%Y-%m-%d")
    from_stamp = (date - timedelta(days=2)).strftime("%Y-%m-%d")
    # contracts_details = []
    for contract in contracts:
        polygon_result = polygon_call(contract['contractSymbol'],from_stamp, to_stamp,30,"minute")
        contract['avg_volume'], contract['avg_transactions'] = build_volume_features(polygon_result)

    spread_cost = calculate_spread_cost(contracts)
    sized_contracts = finalize_trade(contracts, spread_cost, target_cost)
    if sized_contracts != None:
        sized_spread_cost = calculate_spread_cost(sized_contracts)
    return sized_contracts

def pull_trading_balance():
    ### This is hardcoded for now, but will be replaced with a call to the tradier API
    return 50000

def calculate_spread_cost(contracts_details):
    cost = 0
    for contract in contracts_details:
        cost += (100*contract['lastPrice'])
    return cost

def build_volume_features(df):
    avg_volume = df['v'].mean()
    avg_transactions = df['n'].mean()
    return avg_volume, avg_transactions

def finalize_trade(contracts_details, spread_cost, target_cost):
    if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
        return contracts_details
    elif spread_cost > (1.1*target_cost):
        spread2_cost = calculate_spread_cost(contracts_details[0:2])
        if spread2_cost < (1.1*target_cost):
            return contracts_details[0:2]
        else:
            single_contract_cost = 100 * contracts_details[0]['lastPrice']
            if single_contract_cost > (1.1*target_cost):
                return []
            else:
                return contracts_details[0:1]    
    elif spread_cost < (.9*target_cost):
        if (2*spread_cost) < (.9*target_cost):
            return (contracts_details * 2)
        elif (1.1*target_cost) >= (2*spread_cost) >= (.9*target_cost):
            return (contracts_details * 2)
        elif (2*spread_cost) > (1.1*target_cost):
            spread2_cost = calculate_spread_cost(contracts_details[0:2])
            if (spread2_cost + spread_cost) < (1.1*target_cost):
                return (contracts_details + contracts_details[0:2])
        else:
            single_contract_cost = 100 * contracts_details[0]['lastPrice']
            if (single_contract_cost + spread_cost) > (1.1*target_cost):
                return contracts_details
            else:
                return (contracts_details[0:1] + contracts_details)


    
