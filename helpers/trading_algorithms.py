from datetime import datetime, timedelta
import logging
from helpers.helper import get_business_days, polygon_call, calculate_floor_pct  
import numpy as np  

logger = logging.getLogger()
logger.setLevel(logging.INFO)

### TRADING ALGORITHMS ###

def time_decay_alpha_gainers_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = .05
    pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    Floor_pct = ((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']) - .02)
    if type(Floor_pct) == float:
        Floor_pct = -0.02
    day_diff = get_business_days(row['order_transaction_date'])
    print(f'day_diff: {day_diff} + {row["underlying_symbol"]} + {row["trading_strategy"]} + pct_change: {pct_change} + Floor_pct: {Floor_pct} + Target_pct: {Target_pct}')
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']} pct_change: {pct_change}")
    elif day_diff >= 2:
        if pct_change < Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change < (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        else:
            sell_code = 0
            reason = "Hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")

    return sell_code, reason

def time_decay_alpha_ma_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = .05
    pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    Floor_pct = ((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']) - .02)
    print(f'HERE: {Floor_pct}')
    print(type(Floor_pct))
    if type(Floor_pct) == float:
        Floor_pct = -0.02
    day_diff = get_business_days(row['order_transaction_date'])
    print(f'day_diff: {day_diff} + {row["underlying_symbol"]} + {row["trading_strategy"]} + pct_change: {pct_change} + Floor_pct: {Floor_pct} + Target_pct: {Target_pct}')
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']} pct_change: {pct_change}")
    elif day_diff >= 2:
        if pct_change < Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change < (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        else:
            sell_code = 0
            reason = "Hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")

    return sell_code, reason

def time_decay_alpha_maP_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = .05
    pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) * -1
    Floor_pct = ((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']) - .02) * -1
    if type(Floor_pct) == float:
        Floor_pct = -0.02
    day_diff = get_business_days(row['order_transaction_date'])
    print(f'day_diff: {day_diff} + {row["underlying_symbol"]} + {row["trading_strategy"]} + pct_change: {pct_change} + Floor_pct: {Floor_pct} + Target_pct: {Target_pct}')
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']} pct_change: {pct_change}")
    elif day_diff >= 2:
        if pct_change < Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change < (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        else:
            sell_code = 0
            reason = "Hold."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")

    return sell_code, reason

def time_decay_alpha_losers_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = .06
    pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) * -1
    Floor_pct = ((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']) - .025) * -1
    print(f'HERE: {Floor_pct}')
    print(type(Floor_pct))
    if type(Floor_pct) == float:
        Floor_pct = -0.025
    day_diff = get_business_days(row['order_transaction_date'])
    print(f'day_diff: {day_diff} + {row["underlying_symbol"]} + {row["trading_strategy"]} + pct_change: {pct_change} + Floor_pct: {Floor_pct} + Target_pct: {Target_pct} + {row["order_transaction_date"]}')
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']} pct_change: {pct_change}")
    elif day_diff >= 2:
        if pct_change < Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change >= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
            logger.info(f"{reason} POSITION_ID: {row['position_id']}")
        elif pct_change < (.5*(Target_pct)):
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
        spread_cost, spread_multiplier, contracts_details = add_spread_cost(spread_cost, target_cost, contracts_details)
        return contracts_details
            
def add_spread_cost(spread_cost, target_cost, contracts_details):
    print("spread_cost",spread_cost,target_cost)
    spread_multiplier = 1
    total_cost = spread_cost
    if spread_cost == 0:
        print(contracts_details)
        return 0, 0, []
    else:
        while total_cost <= (1.1*target_cost):
            spread_multiplier += 1
            total_cost = spread_cost * spread_multiplier
        
        if total_cost > (1.1*target_cost):
            spread_multiplier -= 1
            total_cost -= spread_cost

        if total_cost < (.67*target_cost):
            sized_contracts = contracts_details + contracts_details[0]
        else:
            sized_contracts = contracts_details * spread_multiplier

    return spread_cost, spread_multiplier, sized_contracts


    
