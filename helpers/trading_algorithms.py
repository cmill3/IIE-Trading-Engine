from datetime import datetime, timedelta
import logging
from helpers.helper import get_business_days, polygon_call_stocks, calculate_floor_pct  
import numpy as np  

logger = logging.getLogger()
logger.setLevel(logging.INFO)

### TRADING ALGORITHMS ###

### INV ALERTS STRATEGIES ###

def time_decay_alpha_gainers_v0_inv(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = .032
    pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']))
    Floor_pct = ((float(max_value) - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']) - .0125)

    if type(Floor_pct) == float:
        Floor_pct = -0.0125

    if pct_change > (2*Target_pct):
        Floor_pct += 0.0105
    elif pct_change > Target_pct:
        Floor_pct += 0.008

    print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")
    # logger.info(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['ticker']}")
    day_diff = get_business_days(row['order_transaction_date'])
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
    elif day_diff >= 2:
        if pct_change < Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
        elif pct_change >= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
        elif pct_change < (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
        else:
            sell_code = 0
            reason = "Hold."

    return sell_code, reason

def time_decay_alpha_ma_v0_inv(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = .0285
    pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) - .01)

    if type(Floor_pct) == float:
        Floor_pct = -0.02

    if pct_change > (2*Target_pct):
        Floor_pct += 0.0085
    elif pct_change > Target_pct:
        Floor_pct += 0.006

    print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")    
    day_diff = get_business_days(row['order_transaction_date'])
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
    elif day_diff >= 2:
        if pct_change < Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
        elif pct_change >= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
        elif pct_change < (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
        else:
            sell_code = 0
            reason = "Hold."

        
    return sell_code, reason

def time_decay_alpha_bfC_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = .025
    pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) - .015)

    if type(Floor_pct) == float:
        Floor_pct = -0.015

    if pct_change > (2*Target_pct):
        Floor_pct += 0.0125
    elif pct_change > Target_pct:
        Floor_pct += 0.008

    print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")    
    day_diff = get_business_days(row['order_transaction_date'])
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
    elif day_diff >= 2:
        if pct_change < Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
        elif pct_change >= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
        elif pct_change < (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
        else:
            sell_code = 0
            reason = "Hold."

        
    return sell_code, reason

def time_decay_alpha_bfP_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = -.025
    pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']))
    Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) + .015)

    if type(Floor_pct) == float:
        Floor_pct = 0.015
    if pct_change < (2*Target_pct):
        Floor_pct -= 0.0125
    elif pct_change < Target_pct:
        Floor_pct -= 0.008

    print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")    
    day_diff = get_business_days(row['order_transaction_date'])
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change >= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
    elif day_diff >= 2:
        if pct_change > Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
        elif pct_change <= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
        elif pct_change > (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
        else:
            sell_code = 0
            reason = "Hold."

        
    return sell_code, reason


def time_decay_alpha_indexC_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = .015
    pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) - .01)

    if type(Floor_pct) == float:
        Floor_pct = -0.01
    if pct_change > (2*Target_pct):
        Floor_pct += 0.004
    elif pct_change > Target_pct:
        Floor_pct += 0.002

    print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")
    day_diff = get_business_days(row['order_transaction_date'])
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
    elif day_diff >= 2:
        if pct_change < Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
        elif pct_change >= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
        elif pct_change < (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
        else:
            sell_code = 0
            reason = "Hold."

        
    return sell_code, reason

def time_decay_alpha_indexP_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = -.015
    pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']))
    Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) + .01)

    if type(Floor_pct) == float:
        Floor_pct = 0.01
    if pct_change < (2*Target_pct):
            Floor_pct -= 0.004
    elif pct_change < Target_pct:
        Floor_pct -= 0.002

    print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")
    day_diff = get_business_days(row['order_transaction_date'])
    sell_code = 0
    reason = ""
    if day_diff < 2:
        if pct_change >= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
    elif day_diff >= 2:
        if pct_change > Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
        elif pct_change <= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
        elif pct_change > (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
        else:
            sell_code = 0
            reason = "Hold."

        
    return sell_code, reason

# def time_decay_alpha_bfC_1d_v0(row, current_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = .025
#     pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) - .015)

#     if type(Floor_pct) == float:
#         Floor_pct = -0.015

#     if pct_change > (2*Target_pct):
#         Floor_pct += 0.0125
#     elif pct_change > Target_pct:
#         Floor_pct += 0.008

#     print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")    
#     day_diff = get_business_days(row['order_transaction_date'])
#     sell_code = 0
#     reason = ""
#     if day_diff < 2:
#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = "Hit exit target, sell."
#     elif day_diff >= 2:
#         if pct_change < Floor_pct:
#             sell_code = 2
#             reason = "Hit point of no confidence, sell."
#         elif pct_change >= Target_pct:
#             sell_code = 2
#             reason = "Hit exit target, sell."
#         elif pct_change < (.5*(Target_pct)):
#             sell_code = 2
#             reason = "Failed momentum gate, sell."
#         else:
#             sell_code = 0
#             reason = "Hold."

        
#     return sell_code, reason

# def time_decay_alpha_bfP_1d_v0(row, current_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = -.025
#     pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']))
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) + .015)

#     if type(Floor_pct) == float:
#         Floor_pct = 0.015
#     if pct_change < (2*Target_pct):
#         Floor_pct -= 0.0125
#     elif pct_change < Target_pct:
#         Floor_pct -= 0.008

#     print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")    
#     day_diff = get_business_days(row['order_transaction_date'])
#     sell_code = 0
#     reason = ""
#     if day_diff < 2:
#         if pct_change >= Floor_pct:
#             sell_code = 2
#             reason = "Hit exit target, sell."
#     elif day_diff >= 2:
#         if pct_change > Floor_pct:
#             sell_code = 2
#             reason = "Hit point of no confidence, sell."
#         elif pct_change <= Target_pct:
#             sell_code = 2
#             reason = "Hit exit target, sell."
#         elif pct_change > (.5*(Target_pct)):
#             sell_code = 2
#             reason = "Failed momentum gate, sell."
#         else:
#             sell_code = 0
#             reason = "Hold."

        
#     return sell_code, reason


def time_decay_alpha_bfC_1d_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = .015
    pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
    Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) - .01)

    if type(Floor_pct) == float:
        Floor_pct = -0.01
    if pct_change > (2*Target_pct):
        Floor_pct += 0.004
    elif pct_change > Target_pct:
        Floor_pct += 0.002

    print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")
    day_diff = get_business_days(row['order_transaction_date'])
    sell_code = 0
    reason = ""
    if day_diff < 1:
        if pct_change <= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
    elif day_diff >= 1:
        if pct_change < Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
        elif pct_change >= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
        elif pct_change < (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
        else:
            sell_code = 0
            reason = "Hold."

        
    return sell_code, reason

def time_decay_alpha_bfP_1d_v0(row, current_price):
    max_value = calculate_floor_pct(row)
    Target_pct = -.015
    pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']))
    Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) + .01)

    if type(Floor_pct) == float:
        Floor_pct = 0.01
    if pct_change < (2*Target_pct):
            Floor_pct -= 0.004
    elif pct_change < Target_pct:
        Floor_pct -= 0.002

    print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")
    day_diff = get_business_days(row['order_transaction_date'])
    sell_code = 0
    reason = ""
    if day_diff < 1:
        if pct_change >= Floor_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
    elif day_diff >= 1:
        if pct_change > Floor_pct:
            sell_code = 2
            reason = "Hit point of no confidence, sell."
        elif pct_change <= Target_pct:
            sell_code = 2
            reason = "Hit exit target, sell."
        elif pct_change > (.5*(Target_pct)):
            sell_code = 2
            reason = "Failed momentum gate, sell."
        else:
            sell_code = 0
            reason = "Hold."

        
    return sell_code, reason


### BET SIZING FUNCTIONS ###

def bet_sizer(contracts, date, spread_length, call_put):
    if call_put == "call":
        target_cost = (.01* pull_trading_balance())
    elif call_put == "put":
        target_cost = (.01* pull_trading_balance()) * .75

    to_stamp = (date - timedelta(days=1)).strftime("%Y-%m-%d")
    from_stamp = (date - timedelta(days=5)).strftime("%Y-%m-%d")
    volumes = []
    # transactions = []
    # contracts_details = []
    for contract in contracts:
        polygon_result = polygon_call_stocks(contract['contract_ticker'],from_stamp, to_stamp,multiplier=1,timespan="day")
        if len(polygon_result) == 0:
            volumes.append(0)
            continue
        contract['avg_volume'], contract['avg_transactions'] = build_volume_features(polygon_result)
        volumes.append(contract['avg_volume'])
        # transactions.append(contract['avg_transactions'])

    total_vol = sum(volumes)/len(contracts)
    if total_vol < 40:
        vol_check = "False"
    else:
        vol_check = "True"
    spread_cost = calculate_spread_cost(contracts)
    quantities = finalize_trade_v2(contracts, spread_cost, target_cost)
    for index, contract in enumerate(contracts):
        try:
            contract['quantity'] = quantities[index]
            print(contract['quantity'])
        except:
            print("ERROR")
            print(contracts)
            print(quantities)
    # if sized_contracts != None:
    #     sized_spread_cost = calculate_spread_cost(sized_contracts)
    # else:
    #     print(contracts)
    return contracts, vol_check

def pull_trading_balance():
    ### This is hardcoded for now, but will be replaced with a call to the tradier API
    return 100000

def calculate_spread_cost(contracts_details):
    cost = 0
    for contract in contracts_details:
        cost += (100*contract['last_price'])
    return cost

def build_volume_features(df):
    avg_volume = df['v'].mean()
    avg_transactions = df['n'].mean()
    return avg_volume, avg_transactions

# def finalize_trade(contracts_details, spread_cost, target_cost):
#     if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
#         return contracts_details
#     elif spread_cost > (1.1*target_cost):
#         spread2_cost = calculate_spread_cost(contracts_details[0:2])
#         if spread2_cost < (1.1*target_cost):
#             return contracts_details[0:2]
#         else:
#             single_contract_cost = 100 * contracts_details[0]['last_price']
#             if single_contract_cost > (1.1*target_cost):
#                 return []
#             else:
#                 return contracts_details[0:1]    
#     elif spread_cost < (.9*target_cost):
#         spread_cost, spread_multiplier, contracts_details = add_spread_cost(spread_cost, target_cost, contracts_details)
#         return contracts_details

def finalize_trade_v2(contracts_details, spread_cost, target_cost):
    if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
        return [1,1,1]
    elif spread_cost > (1.1*target_cost):
        spread2_cost = calculate_spread_cost(contracts_details[1:])
        if spread2_cost < (1.1*target_cost):
            return [0,1,1]
        else:
            contract = contracts_details[0]
            single_contract_cost = 100 * contract['last_price']
            if single_contract_cost > (1.1*target_cost):
                contract = contracts_details[1]
                single_contract_cost = 100 * contract['last_price']
                if single_contract_cost > (1.1*target_cost):
                    contract = contracts_details[2]
                    single_contract_cost = 100 * contract['last_price']
                    if single_contract_cost > (1.1*target_cost):
                        return [0,0,0]
                    else:
                        return [0,0,1]
                else:
                    return [0,1,0]
            else:
                return [1,0,0] 
    elif spread_cost < (.9*target_cost):
        spread_multiplier, add_one = add_spread_cost(spread_cost, target_cost, contracts_details)
        if add_one:  
            return [(spread_multiplier+1),spread_multiplier,spread_multiplier]
        else:
            return [spread_multiplier,spread_multiplier,spread_multiplier]
    else:
        print("ERROR")
        return [0,0,0]
            
def add_spread_cost(spread_cost, target_cost, contracts_details):
    add_one = False
    spread_multiplier = 1
    total_cost = spread_cost
    if spread_cost == 0:
        return 0, False
    else:
        while total_cost <= (1.1*target_cost):
            spread_multiplier += 1
            total_cost = spread_cost * spread_multiplier
        
        if total_cost > (1.1*target_cost):
            spread_multiplier -= 1
            total_cost -= spread_cost

        if total_cost < (.67*target_cost):
            add_one = True

    return spread_multiplier, add_one


    
