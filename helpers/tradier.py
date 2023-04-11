import requests
import json
from credentials import ACCOUNTID, ACCESSTOKEN, PAPER_ACCESSTOKEN, PAPER_ACCOUNTID, PAPER_BASE_URL, LIVE_BASE_URL   

def get_account_balance(base_url: str, account_id: str, access_token:str) -> dict:
    try:
        response = requests.get(f'{base_url}accounts/{account_id}/balances', params=account_id, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
         
        if response.status_code == 200:
            response_json = response.json()
            buying_power = response_json['balances']['margin']['option_buying_power']
            return buying_power
        else:
            print("Buying power pull for live trader failed")
            return response
    except:
        return "Account Balance pull unsuccessful"
    
def get_tradier_credentials(trading_mode: str):
    if trading_mode == "PAPER":
        base_url = PAPER_BASE_URL
        access_token = PAPER_ACCESSTOKEN
        account_id = PAPER_ACCOUNTID
    elif trading_mode == "LIVE":
        base_url = LIVE_BASE_URL
        access_token = ACCESSTOKEN
        account_id = ACCOUNTID
    return base_url, account_id, access_token


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

def verify_contract(symbol: str, access_token: str) -> dict:

    response = requests.post('https://api.tradier.com/v1/markets/quotes', params={"symbols": symbol}, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
    return response


def place_order(base_url: str, account_id: str, access_token:str, symbol: str, option_symbol: str, side: str, quantity: str, order_type: str, duration: str) -> Dict:

    response = requests.post(f'{base_url}{account_id}/orders', 
            params={"account_id": account_id, "class": "Option", "symbol": symbol, "option_symbol": option_symbol, "side": side, "quantity": quantity, "type": order_type, "duration": duration}, 
            json=None, verify=False, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})    
    
    if response.status_code == 200:
        response_json = json.loads(response.json())
        id = response_json['order']['id']
        # successful_trades.append(option_symbol)
        return id, "Success", "Open"
    else:
        return "None", "Failed", "Failed"
    

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
        return {"average_fill_price": average_fill_price, "last_fill_price": last_fill_price, "exec_quantity": exec_quantity, "transaction_date": transaction_dt, "created_date": created_dt, "status": "Success"}
    else:
        print("Order information for order_id:" + order_id + " has failed. Review option contract availability and code.")
        return response.status_code
    

## NEEDS WORK
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
    
def get_last_price(base_url: str, access_token: str, symbol:str) -> dict:
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
    
def position_exit(base_url: str, account_id: str, access_token: str, symbol: str, option_symbol: str, side: str, quantity: str, order_type: str, duration: str) -> dict:
    response = requests.post(f'{base_url}{account_id}/orders', 
                                 params={"account_id": account_id, "class": "Option", "symbol": symbol, "option_symbol": option_symbol, "side": side, "quantity": quantity, "type": order_type, "duration": duration}, 
                                 json=None, verify=False, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
 
    if response.status_code == 200:
        return "Success"
    else:
        print("Order placement for " + option_symbol + " has failed. Review option contract availability and code.")
        return "Failed"