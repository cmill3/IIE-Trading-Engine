from TradierCred import ACCOUNTID, ACCESSTOKEN, PAPER_ACCESSTOKEN, PAPER_ACCOUNTID
from operator import index
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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
from yahooquery import Ticker

#Ideally this would be a database of all of our active trades and filter through
df = pd.read_csv('/Users/ogdiz/Projects/APE-Executor/Portfolio-Manager-v0/PM-Tradier/Executor_Exports', usecols=['Sym', 'Call/Put', 'Strike Price', 'Option Name', 'Trade_Open_Outcome', 'Position_Status', 'Qty_Contracts_Traded', 'Date_of_Trade', 'Datetime_of_Trade', 'Open_OrderID'])
df.dropna(inplace = True)
df = df.reset_index()

urllib3.disable_warnings(category=InsecureRequestWarning)

option_symbol = df['Sym']
option_side = df['Call/Put']
strike_price = df['Strike Price']
option_name = df['Option Name']
trade_open_outcome = df['Trade_Open_Outcome']
position_status = df['Position_status']
contract_qty = df['Qty_Contracts_Traded']
datetime_position_opened = df['Datetime_of_Trade']
date_position_opened = df['Date_of_Trade']
open_orderid = df['Open_OrderID']

#Variables to save positions to be closed in
closed_option_name = []
closed_option_symbol = []
closed_contract_qty = []
closed_open_orderid = []
closed_orderid = []
closed_datetime_position_closed = []
closed_datetime_position_opened = []

order_side = "sell_to_close"
order_type = "market"
duration = "gtc"




cash_balance = []
active_positions = []
pnl = []
pnl_percentage = []
trade_close_outcome = []
successful_trades = []
failed_trades = []



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
    
def acct_positions(Trade_Type: str, account_id: str) -> dict:
    if Trade_Type == "LIVE":
        response = requests.get(f'https://api.tradier.com/v1/accounts/{ACCOUNTID}/positions', params=account_id, headers={'Authorization': f'Bearer {ACCESSTOKEN}', 'Accept': 'application/json'})
        print(response.status_code)
        if response.status_code == 200:
            response_json = response.json()
            option_buying_power = response_json['balances']['margin']['option_buying_power']
            active_positions.append(int(option_buying_power))    
            return response
        else:
            print("Buying power pull for live trader failed")
            return response
    elif Trade_Type == "PAPER":
        response = requests.get(f'https://sandbox.tradier.com/v1/accounts/{PAPER_ACCOUNTID}/positions', params=account_id, headers={'Authorization': f'Bearer {PAPER_ACCESSTOKEN}', 'Accept': 'application/json'})
        print(response.status_code)
        if response.status_code == 200:
            response_json = response.json()
            option_buying_power = response_json['balances']['margin']['option_buying_power']
            active_positions.append(int(option_buying_power))    
            return response
        else:
            print("Buying power for paper trader pull failed")
            return response
    else:
        return "Account Balance pull unsuccessful"
    
def position_pnl(Trade_Type:str, account_id:str, symbol:str, sortby = 'openDate', sort = "asc") -> dict:
    if Trade_Type == "LIVE":
        response = requests.get(f'https://api.tradier.com/v1/accounts/{ACCOUNTID}/gainloss', 
                                params={'account_id': account_id, 'sortBy': sortby, 'sort': sort, 'symbol':symbol}, 
                                headers={'Authorization': f'Bearer {ACCESSTOKEN}', 'Accept': 'application/json'})
    elif Trade_Type == "PAPER":
        response = requests.get(f'https://sandbox.tradier.com/v1/accounts/{PAPER_ACCOUNTID}/gainloss',
                                params={'account_id': account_id, 'sortBy': sortby, 'sort': sort, 'symbol':symbol}, 
                                headers={'Authorization': f'Bearer {PAPER_ACCESSTOKEN}', 'Accept': 'application/json'})
        print(response.status_code)
    else:
        return "PnL pull unsuccessful"
    if response.status_code == 200:
        response_json = response.json()
        position_pnl = response_json['gainloss']['opened_position']['gain_loss']
        position_pnl_percent = response_json['gainloss']['opened_position']['gain_loss_percentage']
        pnl.append(int(position_pnl))
        pnl_percentage.append(int(position_pnl_percent))
        return response
    else:
        print("Pnl pull for option failed - review positions with a 0000 value for pnl and pnl percentages")
        pnl.append(0000)
        pnl_percentage.append(0000)
        return response
    
def end_date_trading_days_only(from_date, add_days, num_days_list): #End date, n days later for the data set built to include just trading days, but doesnt filter holidays
    trading_days_to_add = add_days
    current_date = from_date
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    num_days_list.append(current_date)
    
def date_performance_check(sym, option_name, purchase_strike, open_date, quantity, status, open_orderid):
    opt_name = option_name
    sym_name = sym
    pos_open_date = open_date
    d = datetime.datetime.now() #Today's date
    current_date = d.strftime("%Y-%m-%d")
    date_delta = current_date - pos_open_date
    three_day_baseline = []
    end_date_trading_days_only(pos_open_date, 3, three_day_baseline)
    option_status = status
    quantity_of_contracts = quantity
    current_strike = Ticker[sym_name]
    price_delta = current_strike - purchase_strike
    percent_change = int((price_delta / purchase_strike) * 100)
    if option_status == "Open":
        if percent_change >= -5 or percent_change <=10 or date_delta >= three_day_baseline[0]:
            closed_option_symbol.append(sym_name)
            closed_option_name.append(closed_option_name)
            closed_contract_qty.append(quantity_of_contracts)
            print("Position for option " + opt_name + " has been exited due to a 10% downturn or three day limit met.")
            return
        else:
            return
    else:
        return
        #Check if it has been 3 days or a 10% downturn and prepare to exit position if conditional is true
        #If condition is false, then keep it moving

def position_exit(Trade_Type: str, account_id: str, symbol: str, option_symbol: str, side: str, quantity: str, order_type: str, duration: str) -> dict:
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
        print("Order placement for " + option_symbol + " is successful!")
        successful_trades.append(option_symbol)
        trade_close_outcome.append("Success")
        print(response.status_code)
        print(response.content)
        return response.status_code
    else:
        print("Order placement for " + option_symbol + " has failed. Review option contract availability and code.")
        failed_trades.append(option_symbol)
        trade_close_outcome.append("Failed")
        print(response.status_code)
        print(response.content)
        return response.status_code

for i, row in df.iterrows():
    date_performance_check

df_closed_option_name = pd.DataFrame(closed_option_name, columns = ['Option_Name'])
df_closed_option_symbol = pd.DataFrame(closed_option_symbol, columns= ['Option_Symbol'])
df_closed_contract_qty = pd.DataFrame(closed_contract_qty, columns= ['Contract_Qty'])

df_exitlist = pd.concat([df_closed_option_name, df_closed_option_symbol, df_closed_contract_qty], axis=1)

for i, row in df_exitlist.iterrows():
    position_exit("PAPER", PAPER_ACCOUNTID, closed_option_symbol[i], closed_option_name[i], order_side, closed_contract_qty[i], order_type, duration)