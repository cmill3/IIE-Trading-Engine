from helpers.credentials import ACCOUNTID, ACCESSTOKEN, PAPER_ACCESSTOKEN, PAPER_ACCOUNTID, PAPER_BASE_URL, LIVE_BASE_URL   
from operator import index
import requests
import pandas as pd
import datetime
import json
from helpers import dynamo_helper as db
import logging
import urllib3
from typing import Dict
from urllib3.exceptions import InsecureRequestWarning
import boto3
import os

trading_mode = os.getenv('TRADING_MODE')
d = datetime.datetime.now() #Today's date

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

def run_executor(event, context):
    base_url, access_token, account_id = get_tradier_credentials()
    data = pull_trades()
    execute_trade(data, base_url, access_token, account_id)

def get_tradier_credentials():
    if trading_mode == "PAPER":
        base_url = PAPER_BASE_URL
        access_token = PAPER_ACCESSTOKEN
        account_id = PAPER_ACCOUNTID
    elif trading_mode == "LIVE":
        base_url = LIVE_BASE_URL
        access_token = ACCESSTOKEN
        account_id = ACCOUNTID
    return base_url, account_id, access_token
"""Trade/Account Functions"""

def get_account_balance(base_url: str, account_id: str, access_token:str) -> dict:
    try:
        response = requests.get(f'{base_url}accounts/{account_id}/balances', params=account_id, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
         
        if response.status_code == 200:
            response_json = response.json()
            option_buying_power = response_json['balances']['margin']['option_buying_power']
            cash_balance.append(int(option_buying_power))    
            return response
        else:
            print("Buying power pull for live trader failed")
            return response
    except:
        return "Account Balance pull unsuccessful"
     
# acct_balance("PAPER", PAPER_ACCOUNTID)

"""The portion that takes the account balance and allocates a certain amount will be below - currently unweighted**"""
def account_value_checkpoint(current_balance) -> dict:
    if  current_balance >= acct_balance_min:
        return True
    else:
        return False

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
    return response

# verify_contract(Testing_Option_Symbol)

"""Function that will place the order"""

def place_order(base_url: str, account_id: str, access_token:str, symbol: str, option_symbol: str, side: str, quantity: str, order_type: str, duration: str) -> Dict:

    response = requests.post(f'{base_url}{account_id}/orders', 
            params={"account_id": account_id, "class": "Option", "symbol": symbol, "option_symbol": option_symbol, "side": side, "quantity": quantity, "type": order_type, "duration": duration}, 
            json=None, verify=False, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})    
    
    if response.status_code == 200:
        response_json = json.loads(response.json())
        id = response_json['order']['id']
        successful_trades.append(option_symbol)
        trade_open_outcome.append("Success")
        order_status.append("Open")
        return open_order_id
    else:
        open_order_id.append("None")
        failed_trades.append(option_symbol)
        trade_open_outcome.append("Failed")
        order_status.append("Failed")
        return "failed"
    
    
def get_order_info(base_url: str, account_id: str, access_token:str, order_id: str):
    
    response = requests.post(f'{base_url}{account_id}/orders/{order_id}', 
        params={"account_id": account_id, "id": order_id, "includeTags": True}, 
        json=None, verify=False, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})

    if response.status_code == 200:
        response_json = json.loads(response.json())
        exec_quantity = response_json['order']['exec_quantity']
        average_fill_price = response_json['order']['avg_fill_price']
        last_fill_price = response_json['order']['last_fill_price']
        transaction_dt = response_json['order']['transaction_date']
        created_dt = response_json['order']['create_date']
        return {"average_fill_price": average_fill_price, "last_fill_price": last_fill_price, "exec_quantity": exec_quantity, "transaction_dt": transaction_dt, "created_dt": created_dt, "status": "Success"}
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
    
def execute_trade(data, account_id):
    transaction_data = []
    # failed_trades = []
    for i, row in data.iterrows():
        account_balance = get_account_balance(trading_mode, account_id)
        is_valid = account_value_checkpoint(account_balance)
        if is_valid:
            contract_valid = verify_contract(row["contract_name"])
            if contract_valid:
                open_order_id = place_order(trading_mode, account_id, row['symbol'], row['option_contract'], order_side, Qty_of_Contracts, order_type, duration)
                if open_order_id != "None":
                    order_info_obj = get_order_info(trading_mode, account_id, open_order_id)
                    order_info_obj["pm_data"] = row.to_dict()
                    transaction_data.append(order_info_obj)
                    db.create_dynamo_record(trading_mode, row, order_info_obj, open_order_id, trade_datetime[i], transaction_ids[i], position_ids[i], 
                        Trading_Strategy[i], Option_Name[i], CP_Label[i], Strike_Price[i], Contract_Expiry[i],
                        trade_open_outcome[i],Qty_of_Contracts[i] cash_balance[i], order_status[i])
                else:
                    temp = {
                        "quantity_executed": "none"}
                    order_print_quantity_executed.append("None")
                    order_print_average_fill_price.append("None")
                    order_print_last_fill_price.append("None")
                    order_print_created_dt.append("None")
                    order_print_transaction_dt.append("None")
                    failed_trades.append(temp)
        
        for trade_obj in executed_trades.iterrows():
            create_ids(trade_obj["symbol"], row["option_contract"], row["trading_strategy"], d.strftime("%Y-%m-%d %H:%M:%S"))
            dynamo_response = db.create_dynamo_record(trade_obj)
   
        
df_transaction_id = pd.DataFrame(transaction_ids, columns= ['Transaction_ID'])
df_position_id = pd.DataFrame(position_ids, columns= ['Position_ID'])
df_final_csv = pd.concat([df, df_outcome, df_status, df_balance, df_contractqty, df_date, df_datetime, df_orderid, df_order_average_fill_price, df_order_last_fill_price, df_order_qty_executed, df_order_created_datetime, df_order_transaction_datetime, df_transaction_id, df_position_id], axis=1)

df_final_csv.to_csv(f'/Users/ogdiz/Projects/APE-Executor/Portfolio-Manager-v0/PM-Tradier/Executor_Exports/PM-Exec-{datetime_csv}.csv')


db.create_dynamo_record(trading_mode, open_order_id[i], trade_datetime[i], transaction_ids[i], position_ids[i], 
                        Trading_Strategy[i], Option_Name[i], CP_Label[i], Strike_Price[i], Contract_Expiry[i],
                        trade_open_outcome[i], order_print_average_fill_price[i], order_print_last_fill_price[i],
                        Qty_of_Contracts[i], order_print_quantity_executed[i], order_print_created_dt[i], 
                        order_print_transaction_dt[i], cash_balance[i], order_status[i])