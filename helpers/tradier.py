import requests
import json
import os
import pandas as pd
from datetime import datetime
from helpers.credentials import CM3PROD_ACCOUNTID, CM3PROD_ACCESSTOKEN, DEV_ACCESSTOKEN, DEV_ACCOUNTID, PAPER_BASE_URL, LIVE_BASE_URL, PRODVAL_ACCOUNTID, PRODVAL_ACCESSTOKEN, DIZPROD_ACCESSTOKEN, DIZPROD_ACCOUNTID

# base_url = os.getenv("BASE_URL")
# account_id = os.getenv("ACCOUNT_ID")
# access_token = os.getenv("ACCESS_TOKEN")
now = datetime.now()

def get_account_balance(base_url: str, account_id: str, access_token:str) -> dict:
    try:
        response = requests.get(f'{base_url}accounts/{account_id}/balances', params=account_id, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
         
        if response.status_code == 200:
            response_json = response.json()
            buying_power = response_json['balances']['margin']['option_buying_power']
            return buying_power
        else:
            # print("Buying power pull for live trader failed")
            return response
    except:
        return "Account Balance pull unsuccessful"
    
def get_tradier_credentials(env: str):
    if env == "DEV":
        base_url = PAPER_BASE_URL
        access_token = DEV_ACCESSTOKEN
        account_id = DEV_ACCOUNTID
    elif env == "PROD_VAL":
        base_url = PAPER_BASE_URL
        access_token = PRODVAL_ACCESSTOKEN
        account_id = PRODVAL_ACCOUNTID
    elif env == "DIZ_PROD":
        base_url = LIVE_BASE_URL
        access_token = DIZPROD_ACCESSTOKEN
        account_id = DIZPROD_ACCOUNTID
    elif env == "CM3_PROD":
        base_url = LIVE_BASE_URL
        access_token = CM3PROD_ACCESSTOKEN
        account_id = CM3PROD_ACCOUNTID

    return base_url, account_id, access_token


def option_lookup(symbol: str) -> dict:

     response = requests.get('https://api.tradier.com/v1/markets/options/lookup', params={"underlying": symbol}, json=None, verify=False, headers={'Authorization': f'Bearer {ACCESSTOKEN}', 'Accept': 'application/json'})
     
    #  conid_init.append(conid_json.json()[0]['conid'])
    #  conid_init.append(conid_json.json())
     """This is not the cleanest method, but it works for now - reason being if the Symbol returns more than one contract type, then the
     first one there will be what appends to the conid value - for instance "SPY" has dozens of options available under that value, but
     the first value is for the SPDR ETF, so the [0] will work here -- this has opportunity to be troublesome in the future"""
     return

def verify_contract(symbol: str, base_url:str, access_token: str) -> dict:

    response = requests.post(f'{base_url}markets/quotes', params={"symbols": symbol}, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
    # response = response.json()
    return response.status_code


def place_order(base_url: str, account_id: str, access_token:str, symbol: str, option_symbol: str, quantity: str, order_type: str, duration: str, position_id:str):
    if "O:" in option_symbol:
        option_symbol = option_symbol.split("O:")[1]
    response = requests.post(f'{base_url}accounts/{account_id}/orders', 
            data={"class": 'option', "symbol": symbol, "option_symbol": option_symbol, "side": "buy_to_open", "quantity": quantity, "type": order_type, "duration": duration, "tag": position_id}, 
            headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
    json_response = response.json()
    if response.status_code == 200:
        id = json_response['order']['id']
        # successful_trades.append(option_symbol)
        return id, response.status_code, json_response
    else:
        json_response = response.json()
        return "None", response.status_code, json_response
    

def get_order_info(base_url: str, account_id: str, access_token: str, order_id: str):
    
    response = requests.get(f'{base_url}/accounts/{account_id}/orders/{order_id}', 
        params={"includeTags": 'true'}, 
        headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})

    if response.status_code == 200:
        response_json = response.json()
        exec_quantity = response_json['order']['exec_quantity']
        average_fill_price = response_json['order']['avg_fill_price']
        last_fill_price = response_json['order']['last_fill_price']
        transaction_dt = response_json['order']['transaction_date']
        created_dt = response_json['order']['create_date']
        return {"id":response_json['order']['id'],"average_fill_price": average_fill_price, "last_fill_price": last_fill_price, 
                "exec_quantity": exec_quantity, "transaction_date": transaction_dt, "created_date": created_dt, 
                "option_symbol": response_json['order']['option_symbol'],"status":response_json['order']['status'],
                # "position_id": response_json['order']['tag']
                }
    else:
        print(f"Order information for order_id: {order_id} has failed. Review option contract availability and code.")
        return "Order not filled"
    
# def get_order_info(base_url: str, account_id: str, access_token:str, order_id: str):
    
#     response = requests.post(f'{base_url}{account_id}/orders/{order_id}', 
#         params={"account_id": account_id, "id": order_id, "includeTags": True}, 
#         json=None, verify=False, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})

#     if response.status_code == 200:
#         response_json = json.loads(response.json())
#         exec_quantity = response_json['order']['exec_quantity']
#         average_fill_price = response_json['order']['avg_fill_price']
#         last_fill_price = response_json['order']['last_fill_price']
#         transaction_dt = response_json['order']['transaction_date']
#         created_dt = response_json['order']['create_date']
#         return {"average_fill_price": average_fill_price, "last_fill_price": last_fill_price, "exec_quantity": exec_quantity, "transaction_date": transaction_dt, "created_date": created_dt, "status": "Success"}
#     else:
#         print("Order information for order_id:" + order_id + " has failed. Review option contract availability and code.")
#         return response.status_code
    

def get_account_positions(base_url: str, account_id: str, access_token: str) -> dict:
    try:
        response = requests.get(f'{base_url}accounts/{account_id}/positions', params=account_id, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
        response_json = response.json()
        if response.status_code == 200:
            if response_json['positions'] == "null":
                return "No Positions"
            positions_list = response_json['positions']['position']
            return positions_list
        else:
            print("Buying power pull for live trader failed")
            return response
    except Exception as e:
        return e
    
# def get_last_price(base_url: str, access_token: str, symbol:str) -> dict:
#     try:
#         response = requests.get(f'{base_url}markets/quotes', params={'symbols': symbol, 'greeks': 'false'}, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
#         if response.status_code == 200:
#             response_json = response.json()
#             last_price = response_json['quotes']['quote']['last']    
#             return last_price
#         else:
#             print("Buying power pull for live trader failed")
#             return response
#     except Exception as e:
#         return print(e)

def call_polygon_last_price(symbol):
    today = datetime.today().strftime('%Y-%m-%d')
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}?adjusted=true&sort=asc&limit=50000&apiKey={key}"
    response = requests.request("GET", url, headers={}, data={})

    response_data = json.loads(response.text)
    try:
        results = response_data['results']
    except:
        return None

    results_df = pd.DataFrame.from_dict(results)
    last_price = results_df['c'].iloc[-1]

    return last_price

def call_polygon_price_reconciliation(symbol,dt):
    timestamp_seconds = dt.timestamp()
    timestamp_milliseconds = int(timestamp_seconds * 1000)

    today = datetime.today().strftime('%Y-%m-%d')
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/30/minute/{today}/{today}?adjusted=true&sort=asc&limit=50000&apiKey={key}"
    response = requests.request("GET", url, headers={}, data={})

    response_data = json.loads(response.text)
    try:
        results = response_data['results']
    except:
        return None

    results_df = pd.DataFrame.from_dict(results)
    results_df = results_df[results_df['t'] <= timestamp_milliseconds]
    last_price = results_df['c'].iloc[-1]

    return last_price
    
def position_exit(base_url: str, account_id: str, access_token: str, symbol: str, option_symbol: str, side: str, quantity: str, order_type: str, duration: str, position_id: str) -> dict:
    print(base_url, account_id, access_token, symbol, option_symbol)
    response = requests.post(f'{base_url}accounts/{account_id}/orders', 
                                 params={"account_id": account_id, "class": "Option", "symbol": symbol, "option_symbol": option_symbol, "side": "sell_to_close", "quantity": quantity, "type": order_type, "duration": duration, "tag": position_id}, 
                                 json=None, verify=False, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
    if response.status_code == 200:
        json_response = response.json()
        try:
            id = json_response['order']['id']
            return id, response.status_code, None
        except:
            return None, response.status_code, json_response
    else:   
        return None, response.status_code, response.text


def get_account_orders(base_url: str, account_id: str, access_token: str) -> dict:
    try:
        response = requests.get(f'{base_url}accounts/{account_id}/orders', params={'includeTags': 'true'},headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
        response_json = response.json()
        if response.status_code == 200:
            if response_json['orders'] == "null":
                return "No Positions"
            positions_list = response_json['orders']['order']
            return positions_list
        else:
            print("Buying power pull for live trader failed")
            return response
    except Exception as e:
        return e
