from TradierCred import ACCOUNTID, ACCESSTOKEN, PAPER_ACCESSTOKEN, PAPER_ACCOUNTID
from operator import index
import requests
import pandas as pd
import numpy as np
import datetime
import json
from pprint import pprint
import logging
import urllib3
from typing import Dict
from urllib3.exceptions import InsecureRequestWarning
from fake_useragent import UserAgent
import csv
import subprocess
import pandas_market_calendars as mcal
import boto3

df = pd.read_csv('/Users/ogdiz/Projects/APE-Executor/Trade_Builder/TB_PostProcessing_Data.csv', usecols=['Sym', 'Trading Strategy','Call/Put', 'Strike Price', 'Option Name','Contract Expiry @ 1 Week', 'Contract Expiry @ 2 Weeks'])
df.dropna(inplace = True)
df = df.reset_index()

urllib3.disable_warnings(category=InsecureRequestWarning)

"""Variables for manipulation below"""
Contract_Trade_Status = [] #This will either be approved or denied based on the account value being above the 10k point arbitrarily defined
Ticker_Symbol = df['Sym'] #Ticker Symbol
CP_Label = df['Call/Put']
Strike_Price = df['Strike Price']
Option_Name = df['Option Name']
Contract_Expiry = df['Contract Expiry @ 2 Weeks'] #Contract Expiry Date
Trading_Strategy = df['Trading_Strategy']
Qty_of_Contracts = []
order_side = "buy_to_open"
order_type = "market"
duration = "GTC"
cash_balance = []
acct_balance_min = 10000
successful_trades = []
failed_trades = []
trade_open_outcome = []
open_order_id = []
order_status = []
trade_datetime = []
trade_date = []
order_print_quantity_placed = []
order_print_quantity_executed = []
order_print_average_fill_price = []
order_print_last_fill_price = []
order_print_transaction_dt = []
order_print_created_dt = []
position_ids = []
transaction_ids = []
d = datetime.datetime.now() #Today's date
datetime_csv = d.strftime("%Y-%m-%d-%H-%M-%S")
# t_minus_3_days = timedelta((11 - d.weekday()) % 5) #This is a function to get 3 weekdays ago
# PNL_Start_Date = (d - t_minus_3_days).strftime('%Y-%m-%d') #PNL Start date is 3 weekdays ago as the start PNL

# """Testing Variables"""
# Testing_Ticker = "AAPL"
# Testing_Option_Symbol = "SPY230616C00450000"
# order_side = "buy_to_open"
# month = "2023-06-16"
# strike = "450"
# opt_type = "Call"
# order_type = "market"
# outsideRTH = False #this is whether or not we want the trades to be placed outside "regular trading hours" which will be false for our case right now
# duration = "GTC"
# quantity = "1"

"""Trade/Account Functions"""

def acct_balance(Trade_Type: str, account_id: str) -> dict:
    if Trade_Type == "LIVE":
        response = requests.get(f'https://api.tradier.com/v1/accounts/{ACCOUNTID}/balances', params=account_id, headers={'Authorization': f'Bearer {ACCESSTOKEN}', 'Accept': 'application/json'})
        print(response.status_code)
        if response.status_code == 200:
            response_json = response.json()
            option_buying_power = response_json['balances']['margin']['option_buying_power']
            cash_balance.append(int(option_buying_power))    
            return response
        else:
            print("Buying power pull for live trader failed")
            return response
    elif Trade_Type == "PAPER":
        response = requests.get(f'https://sandbox.tradier.com/v1/accounts/{PAPER_ACCOUNTID}/balances', params=account_id, headers={'Authorization': f'Bearer {PAPER_ACCESSTOKEN}', 'Accept': 'application/json'})
        print(response.status_code)
        if response.status_code == 200:
            response_json = response.json()
            option_buying_power = response_json['balances']['margin']['option_buying_power']
            cash_balance.append(int(option_buying_power))    
            return response
        else:
            print("Buying power for paper trader pull failed")
            return response
    else:
        return "Account Balance pull unsuccessful"
     
# acct_balance("PAPER", PAPER_ACCOUNTID)

"""The portion that takes the account balance and allocates a certain amount will be below - currently unweighted**"""
def account_value_checkpoint(x) -> dict:
    print(type(x))
    print(cash_balance)
    if  x >= acct_balance_min:
        Qty_of_Contracts.append(1)
        Contract_Trade_Status.append("Approved")
        return Contract_Trade_Status
    elif x < acct_balance_min:
        Contract_Trade_Status.append("Declined - Insufficient Funds")
        Qty_of_Contracts.append(0)
        return Contract_Trade_Status
    else:
        Qty_of_Contracts.append(0)
        Contract_Trade_Status.append("Account Value Checkpoint Processing Error")
        print("Account Value Checkpoint failed")
        return "Account Value Checkpoint failed"


# account_value_checkpoint(cash_balance)
# print(Qty_of_Contracts)
# print(Contract_Trade_Status)

"""Functions that will search options for the contract ID (might need to use search --> strikes to find the contract we want, but fortunately the strike date and price will come from TB"""

def option_lookup(symbol: str) -> dict:

     response = requests.get('https://api.tradier.com/v1/markets/options/lookup', params={"underlying": symbol}, json=None, verify=False, headers={'Authorization': f'Bearer {ACCESSTOKEN}', 'Accept': 'application/json'})
     print(response.status_code)
     print(response.json())
    #  conid_init.append(conid_json.json()[0]['conid'])
    #  conid_init.append(conid_json.json())
     """This is not the cleanest method, but it works for now - reason being if the Symbol returns more than one contract type, then the
     first one there will be what appends to the conid value - for instance "SPY" has dozens of options available under that value, but
     the first value is for the SPDR ETF, so the [0] will work here -- this has opportunity to be troublesome in the future"""
     return

#option_lookup(Testing_Ticker)

def verify_contract(symbol: str) -> dict:

    response = requests.post('https://api.tradier.com/v1/markets/quotes', params={"symbols": symbol}, headers={'Authorization': f'Bearer {ACCESSTOKEN}', 'Accept': 'application/json'})
    print(response.status_code)
    print(response.content)
    # print(response.json())
    return

# verify_contract(Testing_Option_Symbol)

"""Function that will place the order"""

def place_order(Trade_Type: str, account_id: str, symbol: str, option_symbol: str, side: str, quantity: str, order_type: str, duration: str) -> Dict:

    if Trade_Type == "LIVE":
     
        response = requests.post(f'https://api.tradier.com/v1/accounts/{ACCOUNTID}/orders', 
            params={"account_id": account_id, "class": "Option", "symbol": symbol, "option_symbol": option_symbol, "side": side, "quantity": quantity, "type": order_type, "duration": duration}, 
            json=None, verify=False, headers={'Authorization': f'Bearer {ACCESSTOKEN}', 'Accept': 'application/json'})
    elif Trade_Type == "PAPER":
        response = requests.post(f'https://sandbox.tradier.com/v1/accounts/{PAPER_ACCOUNTID}/orders', 
           params={"account_id": account_id, "class": "Option", "symbol": symbol, "option_symbol": option_symbol, "side": order_side, "quantity": quantity,"type": order_type, "duration": duration}, 
           json=None, verify=False, headers={'Authorization': f'Bearer {PAPER_ACCESSTOKEN}', 'Accept': 'application/json'})
    else:
        print("Trade type (Live or Paper) must be selected in order to place a trade to the right account")
    
    if response.status_code == 200:
        response_json = json.loads(response.json())
        id = response_json['order']['id']
        open_order_id.append(id)
        print("Order placement for " + option_symbol + " is successful!")
        successful_trades.append(option_symbol)
        trade_open_outcome.append("Success")
        order_status.append("Open")
        print(response.status_code)
        print(response.content)
        return response.status_code
    else:
        print("Order placement for " + option_symbol + " has failed. Review option contract availability and code.")
        open_order_id.append("None")
        failed_trades.append(option_symbol)
        trade_open_outcome.append("Failed")
        order_status.append("Failed")
        print(response.status_code)
        print(response.content)
        return response.status_code
    
def Create_DateTime(): #This is to create the date time of the trade/attempt
    d = datetime.datetime.now() #Today's date
    datetime_of_trade = d.strftime("%Y-%m-%d %H:%M:%S")
    date_of_trade = d.strftime("%Y-%m-%d")
    trade_datetime.append(datetime_of_trade)
    trade_date.append(date_of_trade)
    return
    
def get_order_info(Trade_Type: str, account_id: str, order_id: str):
    if Trade_Type == "LIVE":
        response = requests.post(f'https://api.tradier.com/v1/accounts/{ACCOUNTID}/orders/{order_id}', 
            params={"account_id": account_id, "id": order_id, "includeTags": True}, 
            json=None, verify=False, headers={'Authorization': f'Bearer {ACCESSTOKEN}', 'Accept': 'application/json'})
    elif Trade_Type == "PAPER":
        response = requests.post(f'https://sandbox.tradier.com/v1/accounts/{PAPER_ACCOUNTID}/orders/{order_id}', 
           params={"account_id": account_id, "id": order_id, "includeTags": True}, 
           json=None, verify=False, headers={'Authorization': f'Bearer {PAPER_ACCESSTOKEN}', 'Accept': 'application/json'})
    else:
        print("Trade type (Live or Paper) must be selected in order to place a trade to the right account")

    if response.status_code == 200:
        response_json = json.loads(response.json())
        exec_quantity = response_json['order']['exec_quantity']
        average_fill_price = response_json['order']['avg_fill_price']
        last_fill_price = response_json['order']['last_fill_price']
        transaction_dt = response_json['order']['transaction_date']
        created_dt = response_json['order']['create_date']
        order_print_quantity_executed.append(exec_quantity)
        order_print_average_fill_price.append(average_fill_price)
        order_print_last_fill_price.append(last_fill_price)
        order_print_created_dt.append(created_dt)
        order_print_transaction_dt.append(transaction_dt)
        print(response.status_code)
        print(response.content)
        return response.status_code
    else:
        print("Order information for order_id:" + order_id + " has failed. Review option contract availability and code.")
        print(response.status_code)
        print(response.content)
        return response.status_code
    
#place_order("PAPER", PAPER_ACCOUNTID, Testing_Ticker, Testing_Option_Symbol, order_side, quantity, order_type, duration)

def create_ids(underlying, option_name, trading_strategy, datetime):
    position_id = underlying + "_" + trading_strategy + "_" + datetime
    transaction_id = option_name + "_" + datetime
    position_ids.append(position_id)
    transaction_ids.append(transaction_ids)

def create_dynamo_record(Trade_Type, order_id, datetime_stamp, transaction_id, position_id, trading_strategy, option_name, 
                         option_side, strike_price, contract_expiry, open_outcome, avg_fill_price_open, last_fill_price_open,
                         qty_placed_open, qty_executed_open, order_created_datetime, order_transaction_datetime, bp_balance, order_status):    
    ddb = boto3.resource('dynamodb','us-east-1')
    table1 = ddb.Table('icarus-transaction-table')
    table2 = ddb.Table('icarus-orders-table')
    table3 = ddb.Table('icarus-positions-table')

    ## FILL IN
    item_1={
        'transaction_id': transaction_id,
        'trade_type': Trade_Type,
        'order_id': order_id,
        'datetimestamp': datetime_stamp,
        'position_id': position_id,
        'trading_strategy': trading_strategy,
        'option_name': option_name,
        'trade_open_outcome': open_outcome,
        'avg_fill_price_open': avg_fill_price_open,
        'last_fill_price_open': last_fill_price_open,
        'qty_placed_open': qty_placed_open,
        'qty_executed_open': qty_executed_open,
        'order_creation_dt': order_created_datetime,
        'order_transaction_dt': order_transaction_datetime,
        'closing_order_id': None,
        'avg_fill_price_open': None,
        'last_fill_price_open': None,
        'qty_placed_open': None,
        'qty_executed_open': None,
        'order_status': order_status
    }
    item_2={
        'order_id': order_id,
        'trade_type': Trade_Type,
        'datetimestamp': datetime_stamp,
        'transaction_id': transaction_id,
        'position_id': position_id,
        'trading_strategy': trading_strategy,
        'option_name': option_name,
        'option_side': option_side,
        'strike_price': strike_price,
        'two_week_contract_expiry': contract_expiry,
        'trade_open_outcome': open_outcome,
        'avg_fill_price_open': avg_fill_price_open,
        'last_fill_price_open': last_fill_price_open,
        'qty_placed_open': qty_placed_open,
        'qty_executed_open': qty_executed_open,
        'order_creation_dt': order_created_datetime,
        'order_transaction_dt': order_transaction_datetime,
        'trade_close_outcome': None,
        'closing_order_id': None,
        'avg_fill_price_open': None,
        'last_fill_price_open': None,
        'qty_placed_open': None,
        'qty_executed_open': None,
        'PITM_Balance': bp_balance,
        'order_status': order_status
    }
    item_3={
        'position_id': position_id,
        'trade_type': Trade_Type,
        'order_ids': order_id,
        'datetimestamp': datetime_stamp,
        'transaction_ids': transaction_id,
        'trading_strategy': trading_strategy,
        'option_names': option_name,
        'two_week_contract_expiry': contract_expiry,
        'closing_order_ids': None,
        'position_order_status': order_status
    }

    print(item_1)
    response = table1.put_item(
            Item=item_1
        )
    print(item_2)
    response = table2.put_item(
            Item=item_2
        )   
    print(item_3)
    response = table3.put_item(
            Item=item_3
        )

    return response

"""Opportunity to review trade if we change the 'confirmed' from always true to send text/slack"""

# for i, row in df.iterrows():
#     acct_balance("PAPER", PAPER_ACCOUNTID)
#     account_value_checkpoint(cash_balance[i])
#     verify_contract(Option_Name[i])
#     place_order("PAPER", PAPER_ACCOUNTID, Ticker_Symbol[i], Option_Name[i], order_side, Qty_of_Contracts, order_type, duration)
#     Create_DateTime()
#     if open_order_id != "None":
#         get_order_info("PAPER", PAPER_ACCOUNTID, open_order_id[i])
#     else:
#         order_print_quantity_executed.append("None")
#         order_print_average_fill_price.append("None")
#         order_print_last_fill_price.append("None")
#         order_print_created_dt.append("None")
#         order_print_transaction_dt.append("None")

df_outcome = pd.DataFrame(trade_open_outcome, columns = ['Trade_Open_Outcome'])
df_status = pd.DataFrame(order_status, columns= ['Position_Status'])
df_balance = pd.DataFrame(cash_balance, columns= ['Balance_ToT'])
df_contractqty = pd.DataFrame(Qty_of_Contracts, columns= ['Qty_Contracts_Traded'])
df_datetime = pd.DataFrame(trade_datetime, columns= ['Datetime_of_Trade'])
df_date = pd.DataFrame(trade_date, columns= ['Date_of_Trade'])
df_orderid = pd.DataFrame(open_order_id, columns= ['Open_OrderID'])
df_order_qty_executed = pd.DataFrame(order_print_quantity_executed, columns= ['Contract_Qty_Executed'])
df_order_average_fill_price = pd.DataFrame(order_print_average_fill_price, columns= ['Contract_Avg_Fill_Price'])
df_order_last_fill_price = pd.DataFrame(order_print_last_fill_price, columns= ['Contract_Last_Fill_Price'])
df_order_created_datetime = pd.DataFrame(order_print_created_dt, columns= ['Contract_Created_DT'])
df_order_transaction_datetime = pd.DataFrame(order_print_transaction_dt, columns= ['Contract_Transaction_DT'])

df_final = pd.concat([df, df_outcome, df_status, df_balance, df_contractqty, df_date, df_datetime, df_orderid, df_order_average_fill_price, df_order_last_fill_price, df_order_qty_executed, df_order_created_datetime, df_order_transaction_datetime], axis=1)

# for i, in df_final.iterrows():
#     create_ids(Ticker_Symbol[i], Option_Name[i], Trading_Strategy[i], df_datetime[i])
#     create_dynamo_record("PAPER", open_order_id[i], trade_datetime[i], transaction_ids[i], position_ids[i], 
#                          Trading_Strategy[i], Option_Name[i], CP_Label[i], Strike_Price[i], Contract_Expiry[i],
#                          trade_open_outcome[i], order_print_average_fill_price[i], order_print_last_fill_price[i],
#                          Qty_of_Contracts[i], order_print_quantity_executed[i], order_print_created_dt[i], 
#                          order_print_transaction_dt[i], cash_balance[i], order_status[i])
    
def Live_or_Paper(Trade_Type):
    if Trade_Type == "Paper":
        for i, row in df.iterrows():
            acct_balance("PAPER", PAPER_ACCOUNTID)
            account_value_checkpoint(cash_balance[i])
            verify_contract(Option_Name[i])
            place_order("PAPER", PAPER_ACCOUNTID, Ticker_Symbol[i], Option_Name[i], order_side, Qty_of_Contracts, order_type, duration)
            Create_DateTime()
            if open_order_id != "None":
                get_order_info("PAPER", PAPER_ACCOUNTID, open_order_id[i])
            else:
                order_print_quantity_executed.append("None")
                order_print_average_fill_price.append("None")
                order_print_last_fill_price.append("None")
                order_print_created_dt.append("None")
                order_print_transaction_dt.append("None")
        for i, in df_final.iterrows():
            create_ids(Ticker_Symbol[i], Option_Name[i], Trading_Strategy[i], df_datetime[i])
            create_dynamo_record("PAPER", open_order_id[i], trade_datetime[i], transaction_ids[i], position_ids[i], 
                         Trading_Strategy[i], Option_Name[i], CP_Label[i], Strike_Price[i], Contract_Expiry[i],
                         trade_open_outcome[i], order_print_average_fill_price[i], order_print_last_fill_price[i],
                         Qty_of_Contracts[i], order_print_quantity_executed[i], order_print_created_dt[i], 
                         order_print_transaction_dt[i], cash_balance[i], order_status[i])
    elif Trade_Type == "Live":
        for i, row in df.iterrows():
            acct_balance("LIVE", PAPER_ACCOUNTID)
            account_value_checkpoint(cash_balance[i])
            verify_contract(Option_Name[i])
            place_order("LIVE", PAPER_ACCOUNTID, Ticker_Symbol[i], Option_Name[i], order_side, Qty_of_Contracts, order_type, duration)
            Create_DateTime()
            if open_order_id != "None":
                get_order_info("LIVE", PAPER_ACCOUNTID, open_order_id[i])
            else:
                order_print_quantity_executed.append("None")
                order_print_average_fill_price.append("None")
                order_print_last_fill_price.append("None")
                order_print_created_dt.append("None")
                order_print_transaction_dt.append("None")
        for i, in df_final.iterrows():
            create_ids(Ticker_Symbol[i], Option_Name[i], Trading_Strategy[i], df_datetime[i])
            create_dynamo_record("LIVE", open_order_id[i], trade_datetime[i], transaction_ids[i], position_ids[i], 
                         Trading_Strategy[i], Option_Name[i], CP_Label[i], Strike_Price[i], Contract_Expiry[i],
                         trade_open_outcome[i], order_print_average_fill_price[i], order_print_last_fill_price[i],
                         Qty_of_Contracts[i], order_print_quantity_executed[i], order_print_created_dt[i], 
                         order_print_transaction_dt[i], cash_balance[i], order_status[i])
    else:
        print("Live or Paper Trading Type must be selected")
   
Live_or_Paper("Paper")

        
df_transaction_id = pd.DataFrame(transaction_ids, columns= ['Transaction_ID'])
df_position_id = pd.DataFrame(position_ids, columns= ['Position_ID'])
df_final_csv = pd.concat([df, df_outcome, df_status, df_balance, df_contractqty, df_date, df_datetime, df_orderid, df_order_average_fill_price, df_order_last_fill_price, df_order_qty_executed, df_order_created_datetime, df_order_transaction_datetime, df_transaction_id, df_position_id], axis=1)

df_final_csv.to_csv(f'/Users/ogdiz/Projects/APE-Executor/Portfolio-Manager-v0/PM-Tradier/Executor_Exports/PM-Exec-{datetime_csv}.csv')