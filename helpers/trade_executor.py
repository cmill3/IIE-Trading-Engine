import helpers.tradier as trade
import pandas as pd
from helpers import dynamo_helper as db
import os 
import boto3
from datetime import datetime
import time
import ast

s3 = boto3.client('s3')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')

dt = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
order_type = "market"
duration = "GTC"
acct_balance_min = 20000


def run_executor(data, trading_mode):
    base_url, access_token, account_id = trade.get_tradier_credentials(trading_mode)
    order_ids = execute_new_trades(data, base_url, access_token, account_id, trading_mode)
    return order_ids

     
#The portion that takes the account balance and allocates a certain amount will be below - currently unweighted**
def account_value_checkpoint(current_balance) -> dict:
    if  current_balance >= acct_balance_min:
        return True
    else:
        return False
    
def execute_new_trades(data, base_url, account_id, access_token, trading_mode):
    # transaction_data = []
    full_transactions_data = {}
    failed_transactions = []
    order_ids = []
    print(data)
    for i, row in data.iterrows():
        account_balance = trade.get_account_balance(base_url, account_id, access_token)
        is_valid = account_value_checkpoint(account_balance)
        if is_valid:
            orders_list = []
            position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{dt}"
            if len(row['trade_details']) == 0:
                print('no contracts to trade')
                continue
            row['trade_details'] = ast.literal_eval(row['trade_details'])
            for detail in row['trade_details']:
                try: 
                    # is_valid = trade.verify_contract(detail["contractSymbol"],base_url,access_token)
                    # print(is_valid)
                    open_order_id, trade_result, status, status_code = trade.place_order(base_url, account_id, access_token, row['symbol'], 
                                                                            detail["contractSymbol"], detail['quantity'], 
                                                                            order_type, duration, position_id)
                    
                    if status_code == 200:
                        orders_list.append(open_order_id)
                    else:
                        trade_data = row.to_dict()
                        trade_data['response'] = status_code
                        failed_transactions.append(trade_data)
                        continue
                except Exception as e:
                    print(e)
                    trade_data = row.to_dict()
                    trade_data['response'] = is_valid
                    failed_transactions.append(trade_data)
                    continue

        row_data = row.to_dict()
        row_data['orders'] = orders_list
        row_data['purchase_price'] = trade.get_last_price(base_url, access_token, row['symbol'])
        full_transactions_data[position_id] = row_data
        
    # for trade_obj in transaction_data:
    #     full_order_list = []
    #     response, order_items = db.create_dynamo_record(trade_obj, trading_mode)
    #     full_order_list.append(order_items)

    
    df = pd.DataFrame.from_dict(full_transactions_data)
    final_csv = df.to_csv()

    failed_df = pd.DataFrame(failed_transactions)
    failed_csv = failed_df.to_csv()

    date = datetime.now().strftime("%Y/%m/%d/%H_%M")
    s3.put_object(Bucket=trading_data_bucket, Key=f"orders_data/{date}.csv", Body=final_csv)
    s3.put_object(Bucket=trading_data_bucket, Key=f"pending_orders/{date}.csv", Body=final_csv)
    s3.put_object(Bucket=trading_data_bucket, Key=f"failed_orders/{date}.csv", Body=failed_csv)
    return order_ids

# def pull_open_orders_df():
#     keys = s3.list_objects(Bucket=trading_data_bucket, Prefix="open_orders_data/")["Contents"]
#     query_key = keys[-1]['Key']
#     data = s3.get_object(Bucket=trading_data_bucket, Key=query_key)
#     df = pd.read_csv(data.get("Body"))
#     return df

def close_orders(orders_df,  base_url, account_id,access_token, trading_mode):
    position_ids = orders_df['position_id'].unique()
    total_transactions = []
    for index, row in orders_df.iterrows():
        print("CLOSING TIME")
        print(row)
        id, status_code, status, result = trade.position_exit(base_url, account_id, access_token, row['underlying_symbol'], row['option_symbol'], 'sell_to_close', row['qty_executed_open'], order_type, duration, row['position_id'])
        if status_code == 200:
            # transaction_id = f'{row["option_name"]}_{dt}'
            # transactions = row['transaction_ids']
            # transactions.append(transaction_id)
            row_data = row.to_dict()
            # row_data['transaction_ids'] = transactions
            # row_data['closing_transaction'] = transaction_id
            row_data['closing_order_id'] = id
            total_transactions.append(row_data)

    time.sleep(25)
    db_success = db.process_closed_orders(total_transactions, base_url, access_token, account_id, position_ids, trading_mode)
    return db_success


# if __name__ == "__main__":
#     db_success = db.process_opened_orders(full_transactions_data, base_url, access_token, account_id,trading_mode)
