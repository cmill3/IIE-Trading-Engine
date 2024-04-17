# from datetime import datetime, timedelta
# import logging
# from helpers.helper import get_business_days, calculate_floor_pct  
# import math

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# ### TRADING ALGORITHMS ###

# ### INV ALERTS STRATEGIES ###
# def time_decay_alpha_bfC_v0(row, current_price, derivative_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = .025
#     pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) - .015)
#     derivative_gain = (derivative_price - float(row['last_fill_price_open']))/float(row['last_fill_price_open'])

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

#     if derivative_gain > 1.5:
#         sell_code = 1
#         reason = "Derivative value capture, sell."
#     elif day_diff < 2:
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

# def time_decay_alpha_bfP_v0(row, current_price, derivative_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = -.025
#     pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']))
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) + .015)
#     derivative_gain = (derivative_price - float(row['last_fill_price_open']))/float(row['last_fill_price_open'])

    
#     if derivative_gain > 1.5:
#         sell_code = 1
#         reason = "Derivative value capture, sell."
#     elif type(Floor_pct) == float:
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

# def time_decay_alpha_indexC_v0(row, current_price, derivative_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = .015
#     pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) - .01)
#     derivative_gain = (derivative_price - float(row['last_fill_price_open']))/float(row['last_fill_price_open'])

#     if type(Floor_pct) == float:
#         Floor_pct = -0.01

#     if pct_change > (2*Target_pct):
#         Floor_pct += 0.004
#     elif pct_change > Target_pct:
#         Floor_pct += 0.001

#     print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")    
#     day_diff = get_business_days(row['order_transaction_date'])
#     sell_code = 0
#     reason = ""
#     if derivative_gain > 1.5:
#         sell_code = 1
#         reason = "Derivative value capture, sell."
#     elif day_diff < 2:
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

# def time_decay_alpha_indexP_v0(row, current_price, derivative_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = -.015
#     pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']))
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) + .01)
#     derivative_gain = (derivative_price - float(row['last_fill_price_open']))/float(row['last_fill_price_open'])

#     if type(Floor_pct) == float:
#         Floor_pct = 0.01
#     if pct_change < (2*Target_pct):
#         Floor_pct -= 0.004
#     elif pct_change < Target_pct:
#         Floor_pct -= 0.001

#     print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")    
#     day_diff = get_business_days(row['order_transaction_date'])
#     sell_code = 0
#     reason = ""

#     if derivative_gain > 1.5:
#         sell_code = 1
#         reason = "Derivative value capture, sell."
#     elif day_diff < 2:
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


# def time_decay_alpha_indexC_1d_v0(row, current_price, derivative_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = .009
#     pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) - .009)
#     derivative_gain = (derivative_price - float(row['last_fill_price_open']))/float(row['last_fill_price_open'])

#     if type(Floor_pct) == float:
#         Floor_pct = -0.007
#     if pct_change > (2*Target_pct):
#         Floor_pct += 0.0015
#     elif pct_change > Target_pct:
#         Floor_pct += 0.001

#     print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")
#     day_diff = get_business_days(row['order_transaction_date'])
#     sell_code = 0
#     reason = ""

#     if derivative_gain > 1.5:
#         sell_code = 1
#         reason = "Derivative value capture, sell."
#     elif day_diff < 2:
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

# def time_decay_alpha_indexP_1d_v0(row, current_price, derivative_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = -.009
#     pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']))
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) + .007)
#     derivative_gain = (derivative_price - float(row['last_fill_price_open']))/float(row['last_fill_price_open'])

#     if type(Floor_pct) == float:
#         Floor_pct = 0.007
#     if pct_change < (2*Target_pct):
#             Floor_pct -= 0.0015
#     elif pct_change < Target_pct:
#         Floor_pct -= 0.001

#     print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")
#     day_diff = get_business_days(row['order_transaction_date'])
#     sell_code = 0
#     reason = ""

#     if derivative_gain > 1.5:
#         sell_code = 1
#         reason = "Derivative value capture, sell."
#     elif day_diff < 2:
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

# def time_decay_alpha_bfC_1d_v0(row, current_price, derivative_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = .015
#     pct_change = (current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) - .01)
#     derivative_gain = (derivative_price - float(row['last_fill_price_open']))/float(row['last_fill_price_open'])

#     if type(Floor_pct) == float:
#         Floor_pct = -0.01
#     if pct_change > (2*Target_pct):
#         Floor_pct += 0.004
#     elif pct_change > Target_pct:
#         Floor_pct += 0.002

#     print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")
#     day_diff = get_business_days(row['order_transaction_date'])
#     sell_code = 0
#     reason = ""

#     if derivative_gain > 1.5:
#         sell_code = 1
#         reason = "Derivative value capture, sell."
#     elif day_diff < 1:
#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = "Hit exit target, sell."
#     elif day_diff >= 1:
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

# def time_decay_alpha_bfP_1d_v0(row, current_price, derivative_price):
#     max_value = calculate_floor_pct(row)
#     Target_pct = -.015
#     pct_change = ((current_price - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price']))
#     Floor_pct = (((max_value - float(row['underlying_purchase_price']))/float(row['underlying_purchase_price'])) + .01)
#     derivative_gain = (derivative_price - float(row['last_fill_price_open']))/float(row['last_fill_price_open'])

#     if type(Floor_pct) == float:
#         Floor_pct = 0.01
#     if pct_change < (2*Target_pct):
#             Floor_pct -= 0.004
#     elif pct_change < Target_pct:
#         Floor_pct -= 0.002

#     print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {current_price} purchase_price: {row['underlying_purchase_price']} for {row['underlying_symbol']}")
#     day_diff = get_business_days(row['order_transaction_date'])
#     sell_code = 0
#     reason = ""

#     if derivative_gain > 1.5:
#         sell_code = 1
#         reason = "Derivative value capture, sell."
#     elif day_diff < 1:
#         if pct_change >= Floor_pct:
#             sell_code = 2
#             reason = "Hit exit target, sell."
#     elif day_diff >= 1:
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


# ### BET SIZING FUNCTIONS ###




    
