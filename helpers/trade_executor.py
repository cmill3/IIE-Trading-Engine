import helpers.tradier as trade
import pandas as pd
from helpers import dynamo_helper as db
import os 
import boto3
from datetime import datetime
import ast

s3 = boto3.client('s3')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')

dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
order_side = "buy_to_open"
order_type = "market"
duration = "GTC"
acct_balance_min = 20000


def run_executor(data, trading_mode):
    base_url, access_token, account_id = trade.get_tradier_credentials(trading_mode)
    execute_new_trades(data, base_url, access_token, account_id, trading_mode)

     
#The portion that takes the account balance and allocates a certain amount will be below - currently unweighted**
def account_value_checkpoint(current_balance) -> dict:
    if  current_balance >= acct_balance_min:
        return True
    else:
        return False
    
def execute_new_trades(data, base_url, account_id, access_token, trading_mode):
    transaction_data = []
    for i, row in data.iterrows():
        account_balance = trade.get_account_balance(base_url, account_id, access_token)
        is_valid = account_value_checkpoint(account_balance)
        if is_valid:
            orders_list = []
            position_id = f"{row['symbol']}_{row['strategy']}_{dt}"
            row['trade_details'] = ast.literal_eval(row['trade_details'])
            for detail in row['trade_details']:
                transactions_list = []
                print(detail)
                status_code = trade.verify_contract(detail["contractSymbol"],base_url,access_token)
                if status_code == 200:
                    open_order_id, trade_result, status, response = trade.place_order(base_url, account_id, access_token, row['symbol'], 
                                                                            detail["contractSymbol"], order_side, detail['quantity'], 
                                                                            order_type, duration)
                    print(response)
                    orders_list.append(open_order_id)
                    if open_order_id != "None":
                        order_info_obj = detail.get_order_info(trading_mode, account_id, open_order_id)
                        transaction_id = f'{detail["contractSymbol"]}_{dt}'
                        order_info_obj['transaction_id'] = transaction_id
                        transactions_list.append(transaction_id)
                        order_info_obj['position_id'] = position_id
                        order_info_obj["pm_data"] = row.to_dict()
                        order_info_obj['order_id'] = open_order_id
                        order_info_obj['account_balance'] = account_balance
                        order_info_obj['order_status'] = status
                        order_info_obj['trade_result'] = trade_result
                        db.create_new_dynamo_record_transaction(order_info_obj, trading_mode)
                        db.create_new_dynamo_record_order(order_info_obj, [transaction_id], trading_mode)
                    else:
                        order_info_obj = {}
                        order_info_obj['status'] = "failed"
                        order_info_obj["pm_data"] = row.to_dict()
        db.create_new_dynamo_record_position(order_info_obj, transactions_list, orders_list, trading_mode)
        transaction_data.append(order_info_obj)
        
    # for trade_obj in transaction_data:
    #     full_order_list = []
    #     response, order_items = db.create_dynamo_record(trade_obj, trading_mode)
    #     full_order_list.append(order_items)

    
    df = pd.DataFrame.from_dict(transaction_data)
    final_csv = pd.to_csv(df)

    date = datetime.now().strftime("%Y-%m-%d_%H")
    s3.put_object(Bucket=trading_data_bucket, Key=f"orders_data/{date}.csv", Body=final_csv)
    return "process complete"

def pull_open_orders_df():
    keys = s3.list_objects(Bucket=trading_data_bucket, Prefix="open_orders_data/")["Contents"]
    query_key = keys[-1]['Key']
    data = s3.get_object(Bucket=trading_data_bucket, Key=query_key)
    df = pd.read_csv(data.get("Body"))
    return df
