import helpers.tradier as trade
import pandas as pd
from helpers import dynamo_helper as db
import os 
import boto3
from datetime import datetime
import time
import ast
import logging
from helpers.trading_algorithms import *


s3 = boto3.client('s3')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
user = os.getenv("USER")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

now = datetime.now()
dt = now.strftime("%Y-%m-%dT%H-%M-%S")
dt_posId = now.strftime("%Y-%m-%dT%H-%M")
dt = now.strftime("%Y-%m-%d_%H:%M:%S")
current_date = now.strftime("%Y-%m-%d")

order_type = "market"
duration = "GTC"
acct_balance_min = 20000


def run_executor(data, trading_mode, base_url, account_id, access_token, table, current_positions):
    order_ids = execute_new_trades(data, base_url, account_id, access_token, trading_mode, table, current_positions)
    return order_ids

     
#The portion that takes the account balance and allocates a certain amount will be below - currently unweighted**
def account_value_checkpoint(current_balance) -> dict:
    if  current_balance >= acct_balance_min:
        return True
    else:
        return False
    
def execute_new_trades(data, base_url, account_id, access_token, trading_mode, table, current_positions):
    # transaction_data = []
    full_transactions_data = {}
    failed_transactions = []
    orders_list = []
    accepted_orders = []
    logger.info(current_positions)
    for i, row in data.iterrows():
        position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{dt_posId}"
        pos_id = f"{row['symbol']}{(row['strategy'].replace('_',''))}"
                                    
        if pos_id in current_positions:
            logger.info(pos_id)
            is_valid = False
            logger.info(is_valid)
        else:
            logger.info(pos_id)
            is_valid = True
            logger.info(is_valid)

        if is_valid:
            try:
                if row['strategy'] in ['bfC','bfP']:
                    row['trade_details'] = ast.literal_eval(row['trade_details2wk'])
                elif row['strategy'] in ['bfC_1d','bfP_1d']:
                    if now.date().weekday() <= 2:
                        row['trade_details'] = ast.literal_eval(row['trade_details1wk'])
                    else:
                        row['trade_details'] = ast.literal_eval(row['trade_details2wk'])
                elif row['strategy'] in ['indexC','indexP']:
                    row['trade_details'] = ast.literal_eval(row['trade_details3d'])
                elif row['strategy'] in ['indexC_1d','indexP_1d']:
                    row['trade_details'] = ast.literal_eval(row['trade_details1d'])
            except Exception as e:
                logger.info(f'Failed to parse trade_details: {e}')
                continue
            
            for detail in row['trade_details']:
                if detail['quantity'] == 0:
                    continue
                try: 
                    open_order_id, status_code, json_response = trade.place_order(base_url, account_id, access_token, row['symbol'], 
                                                                            detail["contract_ticker"], detail['quantity'], 
                                                                            order_type, duration, position_id)
                    
                    if status_code == 200:
                        row['order_id'] = open_order_id
                        row['position_id'] = position_id
                        underlying_purchase_price = trade.call_polygon_last_price(row['symbol'])
                        row['underlying_purchase_price'] = underlying_purchase_price
                        orders_list.append(open_order_id)
                        accepted_orders.append({"order_id": open_order_id, "position_id": position_id, "symbol": row['symbol'], "strategy": row['strategy'], "sellby_date":row['sellby_date'],"Call/Put":row['Call/Put'],"underlying_purchase_price": underlying_purchase_price})
                        logger.info(f'Place order executed: {open_order_id}')
                    else:
                        trade_data = row.to_dict()
                        trade_data['response'] = status_code
                        failed_transactions.append(trade_data)
                        logger.info(f'Place order did not return 200: {detail["contract_ticker"]} json:{json_response}')
                        continue
                except Exception as e:
                    logger.info(f'Place order failed: {e}')
                    trade_data = row.to_dict()
                    trade_data['response'] = is_valid
                    failed_transactions.append(trade_data)
                    continue

            row_data = row.to_dict()
            row_data['orders'] = orders_list
            row_data['purchase_price'] = trade.call_polygon_last_price(row['symbol'])
            full_transactions_data[position_id] = row_data
        

    df = pd.DataFrame.from_dict(full_transactions_data)
    final_csv = df.to_csv()

    accepted_df = pd.DataFrame.from_dict(accepted_orders)
    accepted_csv = accepted_df.to_csv()

    failed_df = pd.DataFrame(failed_transactions)
    failed_csv = failed_df.to_csv()

    date = datetime.now().strftime("%Y/%m/%d/%H_%M")
    s3.put_object(Bucket=trading_data_bucket, Key=f"orders_data/{user}/{date}.csv", Body=final_csv)
    s3.put_object(Bucket=trading_data_bucket, Key=f"accepted_orders_data/{user}/{date}.csv", Body=accepted_csv)
    s3.put_object(Bucket=trading_data_bucket, Key=f"pending_orders/{user}/{date}.csv", Body=final_csv)
    s3.put_object(Bucket=trading_data_bucket, Key=f"failed_orders/{user}/{date}.csv", Body=failed_csv)


    time.sleep(15)
    pending_orders = process_dynamo_orders(accepted_df, base_url, account_id, access_token, trading_mode, table)
    pending_df = pd.DataFrame.from_dict(pending_orders)
    pending_csv = pending_df.to_csv()
    s3.put_object(Bucket=trading_data_bucket, Key=f"pending_orders_enriched/{user}/{date}.csv", Body=pending_csv)
    return accepted_df

def process_dynamo_orders(formatted_df, base_url, account_id, access_token, trading_mode, table):
    # for index, row in formatted_df.iterrows():
    pending_orders = db.process_opened_ordersv2(formatted_df, base_url, account_id, access_token, trading_mode, table)
    # response = update_currently_open_orders(fulfilled_orders, [])
    pending_df = pd.DataFrame.from_dict(pending_orders)
    return pending_df

def close_orders(orders_df,  base_url, account_id,access_token, trading_mode, table, close_table):
    position_ids = orders_df['position_id'].unique()
    accepted_orders = []
    rejected_orders = []

    for index, row in orders_df.iterrows():
        id, status_code, error_json = trade.position_exit(base_url, account_id, access_token, row['underlying_symbol'], row['option_symbol'], 'sell_to_close', row['qty_executed_open'], order_type, duration, row['position_id'])
        print(status_code)
        print(error_json)
        if error_json == None:
            row_data = row.to_dict()
            row_data['closing_order_id'] = id
            accepted_orders.append(row_data)
            logger.info(f'Close order succesful {row["option_symbol"]} id:{id}')
        else:
            row_data = row.to_dict()
            row_data['response'] = error_json
            rejected_orders.append(row_data)
            logger.info(f'Close order did not return 200: {row["option_symbol"]} json:{error_json}')

    date = datetime.now().strftime("%Y/%m/%d/%H_%M")

    accepted_df = pd.DataFrame.from_dict(accepted_orders)
    accepted_csv = accepted_df.to_csv()
    rejected_df = pd.DataFrame.from_dict(rejected_orders)
    rejected_csv = rejected_df.to_csv()
    
    s3.put_object(Bucket=trading_data_bucket, Key=f"accepted_closed_orders_data/{user}/{date}.csv", Body=accepted_csv)
    s3.put_object(Bucket=trading_data_bucket, Key=f"rejected_closed_orders_data/{user}/{date}.csv", Body=rejected_csv)

    time.sleep(25)
    closed_orders = db.process_closed_orders(accepted_df, base_url, account_id, access_token, position_ids, trading_mode, table, close_table)

    closed_df = pd.DataFrame.from_dict(closed_orders)
    csv = closed_df.to_csv()
    s3_response = s3.put_object(Bucket=trading_data_bucket, Key=f"enriched_closed_orders_data/{user}/{date}.csv", Body=csv)
    return s3_response

def date_performance_check(row):
    last_price = trade.call_polygon_last_price(row['underlying_symbol'])
    if user == "inv":
        sell_code, reason = evaluate_performance_inv(last_price, row)
    logger.info(f'Performance check: {row["option_symbol"]} sell_code:{sell_code} reason:{reason}')
    if sell_code == 2 or current_date > row['sellby_date']:
        # order_dict = {
        #     "contract": row['option_symbol'],
        #     "underlying_symbol": row['underlying_symbol'],
        #     "quantity": row['quantity'], 
        #     "reason": reason,
        # }
        return 2, reason
    else:
        return 0, reason

def evaluate_performance_inv(current_price, row):
    strategy = row['trading_strategy']
    if strategy == 'indexC':
        sell_code, reason = time_decay_alpha_indexC_v0(row, current_price)
    elif strategy == 'indexP':
        sell_code, reason = time_decay_alpha_indexP_1d_v0(row, current_price)
    elif strategy == 'indexC_1d':
        sell_code, reason = time_decay_alpha_indexC_v0(row, current_price)
    elif strategy == 'indexP_1d':
        sell_code, reason = time_decay_alpha_indexP_1d_v0(row, current_price)
    elif strategy == 'bfC':
        sell_code, reason = time_decay_alpha_bfC_v0(row, current_price)
    elif strategy == 'bfP':
       sell_code, reason = time_decay_alpha_bfP_v0(row, current_price)
    elif strategy == 'bfP_1d':
        sell_code, reason = time_decay_alpha_bfP_1d_v0(row, current_price)
    elif strategy == 'bfC_1d':
        sell_code, reason = time_decay_alpha_bfC_1d_v0(row, current_price)
    return sell_code, reason


if __name__ == "__main__":
    trading_mode = "PAPER"
    account_id = "VA72174659"
    access_token = "ld0Mx4KbsOBYwmJApdowZdFcIxO7"
    base_url = "https://sandbox.tradier.com/v1/"
    dataset = s3.get_object(Bucket="icarus-trading-data", Key="accepted_orders_data/2023/06/22/16_07.csv")
    df = pd.read_csv(dataset.get("Body"))
    pending_orders = process_dynamo_orders(df, base_url, account_id, access_token, trading_mode)
    pending_df = pd.DataFrame.from_dict(pending_orders)
