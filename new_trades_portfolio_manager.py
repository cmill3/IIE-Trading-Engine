from TradierCred import ACCOUNTID, ACCESSTOKEN, PAPER_ACCESSTOKEN, PAPER_ACCOUNTID, PAPER_BASE_URL, LIVE_BASE_URL   
import requests
import pandas as pd
from datetime import datetime, timedelta
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from yahooquery import Ticker
import boto3
import os

s3 = boto3.client('s3')
trading_mode = os.getenv('TRADING_MODE')
urllib3.disable_warnings(category=InsecureRequestWarning)

d = datetime.datetime.now() #Today's date
current_date = d.strftime("%Y-%m-%d")


def manage_portfolio(event, context):
    access_token, base_url, account_id = get_tradier_credentials()
    new_trades_df = pull_data()
    open_trades_df = get_account_positions(access_token, base_url, account_id)
    orders_to_close = evaluate_open_trades(open_trades_df, access_token, base_url, account_id)
    
    account_balance = get_account_balance(access_token, base_url, account_id)


def pull_data():
    keys = s3.list_objects(Bucket="yqalerts-potential-trades")["Contents"]
    key = keys[-1]['Key']
    dataset = s3.get_object(Bucket="yqalerts-potential-trades", Key=key)
    df = pd.read_csv(dataset.get("Body"))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    return df


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

def evaluate_open_trades(df):
    orders_to_close = []
    for index, row in df.iterrows():
        close_order, order_dict = date_performance_check(row)
        if close_order:
            orders_to_close.append(order_dict)
    return orders_to_close


def get_tradier_credentials():
    if trading_mode == "PAPER":
        base_url = PAPER_BASE_URL
        access_token = PAPER_ACCESSTOKEN
        account_id = PAPER_ACCOUNTID
    elif trading_mode == "LIVE":
        base_url = LIVE_BASE_URL
        access_token = ACCESSTOKEN
        account_id = ACCOUNTID
    return base_url, access_token, account_id

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
    
def get_account_positions(base_url: str, account_id: str, access_token: str) -> dict:
    try:
        response = requests.get(f'{base_url}accounts/{account_id}positions', params=account_id, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
        if response.status_code == 200:
            response_json = response.json()
            option_buying_power = response_json['balances']['margin']['option_buying_power']
            active_positions.append(int(option_buying_power))    
            return response
        else:
            print("Buying power pull for live trader failed")
            return response
    except:
        return "Account Positions pull unsuccessful"
    
def get_account_positions(base_url: str, account_id: str, access_token: str) -> dict:
    try:
        response = requests.get(f'{base_url}accounts/{account_id}positions', params=account_id, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
        if response.status_code == 200:
            response_json = response.json()
            option_buying_power = response_json['balances']['margin']['option_buying_power']
            active_positions.append(int(option_buying_power))    
            return response
        else:
            print("Buying power pull for live trader failed")
            return response
    except:
        return "Account Positions pull unsuccessful"
    
def get_last_price(base_url: str, account_id: str, access_token: str, symbol:str) -> dict:
    try:
        response = requests.get(f'{base_url}markets/quotes', params={'symbols': symbol, 'greeks': 'false'}, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
        if response.status_code == 200:
            response_json = response.json()
            last_price = response_json['quotes']['quote']['last']    
            return last_price
        else:
            print("Buying power pull for live trader failed")
            return response
    except:
        return "Account Positions pull unsuccessful"

def calculate_sellby_date(current_date, trading_days_to_add): #End date, n days later for the data set built to include just trading days, but doesnt filter holiday
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    return current_date
    
def date_performance_check(row, base_url, account_id, access_token):
    date_delta = current_date - row['position_open_date']
    # sellby_date = calculate_sellby_date(row['position_open_date'], 3)
    current_strike = get_last_price(row['underlying_symbol'], base_url, account_id, access_token)
    price_delta = current_strike - row['purchase_strike']
    percent_change = int((price_delta / row['purchase_strike']) * 100)
    
    if percent_change >= 5 or percent_change <= -5 or date_delta >= 3:
        order_dict = {
            "contract": row['contract'],
            "underlying_symbol": row['underlying_symbol'],
            "quantity": row['quantity'], 
        }
        print("Position for option " + option_name + " has been exited due to a 10% downturn or three day limit met.")
        return True, order_dict
    else:
        return False, {}

def position_exit(base_url: str, access_token: str, account_id: str, symbol: str, option_symbol: str, side: str, quantity: str, order_type: str, duration: str) -> dict:
    response = requests.post(f'{base_url}{account_id}/orders', 
                                 params={"account_id": account_id, "class": "Option", "symbol": symbol, "option_symbol": option_symbol, "side": side, "quantity": quantity, "type": order_type, "duration": duration}, 
                                 json=None, verify=False, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
 
    if response.status_code == 200:
        successful_trades.append(option_symbol)
        trade_close_outcome.append("Success")
        return response.status_code
    else:
        print("Order placement for " + option_symbol + " has failed. Review option contract availability and code.")
        failed_trades.append(option_symbol)
        trade_close_outcome.append("Failed")
        return response.status_code

for i, row in df.iterrows():
    date_performance_check

df_closed_option_name = pd.DataFrame(closed_option_name, columns = ['Option_Name'])
df_closed_option_symbol = pd.DataFrame(closed_option_symbol, columns= ['Option_Symbol'])
df_closed_contract_qty = pd.DataFrame(closed_contract_qty, columns= ['Contract_Qty'])

df_exitlist = pd.concat([df_closed_option_name, df_closed_option_symbol, df_closed_contract_qty], axis=1)

for i, row in df_exitlist.iterrows():
    position_exit("PAPER", PAPER_ACCOUNTID, closed_option_symbol[i], closed_option_name[i], order_side, closed_contract_qty[i], order_type, duration)