import helpers.tradier as trade
import helpers
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

order_side = "sell_to_close"
order_type = "market"
duration = "gtc"


def manage_portfolio(event, context):
    access_token, base_url, account_id = trade.get_tradier_credentials()
    new_trades_df = pull_data()
    open_trades_df = trade.get_account_positions(base_url, account_id, access_token)
    orders_to_close = evaluate_open_trades(open_trades_df, base_url, account_id, access_token)
    trade_response = close_orders(orders_to_close, base_url, account_id, access_token)
    account_balance = trade.get_account_balance(access_token, base_url, account_id, access_token)


def pull_data():
    keys = s3.list_objects(Bucket="yqalerts-potential-trades")["Contents"]
    key = keys[-1]['Key']
    dataset = s3.get_object(Bucket="yqalerts-potential-trades", Key=key)
    df = pd.read_csv(dataset.get("Body"))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    return df

def evaluate_open_trades(df,base_url, access_token):
    orders_to_close = []
    for index, row in df.iterrows():
        close_order, order_dict = date_performance_check(row, base_url, access_token)
        if close_order:
            orders_to_close.append(order_dict)
    return orders_to_close

def close_orders(orders_list):
    for order in orders_list:
        trade.position_exit(order['underlying_symbol'], order['contract'], order_side, order['quantity'], order_type, duration)

def calculate_sellby_date(current_date, trading_days_to_add): #End date, n days later for the data set built to include just trading days, but doesnt filter holiday
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    return current_date
    
def date_performance_check(row, base_url, access_token):
    # date_delta = current_date - row['position_open_date']
    sellby_date = calculate_sellby_date(row['position_open_date'], 3)
    current_strike = trade.get_last_price(row['underlying_symbol'], base_url, access_token)
    price_delta = current_strike - row['purchase_strike']
    percent_change = int((price_delta / row['purchase_strike']) * 100)
    
    if percent_change >= 5 or percent_change <= -5 or current_date > sellby_date:
        order_dict = {
            "contract": row['contract'],
            "underlying_symbol": row['underlying_symbol'],
            "quantity": row['quantity'], 
        }
        return True, order_dict
    else:
        return False, {}

for i, row in df_exitlist.iterrows():
    position_exit("PAPER", PAPER_ACCOUNTID, closed_option_symbol[i], closed_option_name[i], order_side, closed_contract_qty[i], order_type, duration)