import helpers.tradier as trade
import pandas as pd
from helpers import dynamo_helper as db
import os 
import boto3
from datetime import datetime
import time
import ast
import logging
from helpers.constants import *
from helpers.trend_algorithms import *
from helpers.cdvol_algorithms import *
from helpers.helper import log_message_open, log_message_close, log_message_open_error



s3 = boto3.client('s3')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
env = os.getenv("ENV")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

now = datetime.now()
dt = now.strftime("%Y-%m-%dT%H-%M-%S")
dt_posId = now.strftime("%Y-%m-%dT%H-%M")
current_date = now.strftime("%Y-%m-%d")
date = datetime.now().strftime("%Y/%m/%d/%H_%M")

order_type = "market"
duration = "GTC"
acct_balance_min = 20000

#The portion that takes the account balance and allocates a certain amount will be below - currently unweighted**
def account_value_checkpoint(current_balance) -> dict:
    if  current_balance >= acct_balance_min:
        return True
    else:
        return False
    
def execute_new_trades(data,table, lambda_signifier):
    base_url, account_id, access_token = trade.get_tradier_credentials(env)
    # transaction_data = []
    positions_data = []
    failed_transactions = []
    orders_list = []
    accepted_orders = []
    for i, row in data.iterrows():
        is_valid = False
        position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{dt_posId}"
        pos_id = f"{row['symbol']}{(row['strategy'].replace('_',''))}"
        spread_start = ALGORITHM_CONFIG[row['strategy']]['spread_start']
        spread_length = ALGORITHM_CONFIG[row['strategy']]['spread_length']
                                    
        if row['strategy'] in THREED_STRATEGIES and now.date().weekday() <= 2:
            is_valid = True
        elif row['strategy'] in ONED_STRATEGIES and now.date().weekday() <= 3:
            is_valid = True

        if is_valid:
            try: 
                row['trade_details'] = ast.literal_eval(row['trade_details1wk'])
            except:
                continue
            all_trades = row['trade_details']
            trades = row['trade_details'][spread_start:(spread_length+spread_start)]
            if row['strategy'] in CALL_STRATEGIES:
                option_side = "call"
            elif row['strategy'] in PUT_STRATEGIES:
                option_side = "put"

            for detail in trades:
                if detail['quantity'] == 0:
                    continue
                try: 
                    open_order_id, status_code, json_response = trade.place_order(base_url, account_id, access_token, row['symbol'], 
                                                                            detail["contract_ticker"], detail['quantity'], 
                                                                            order_type, duration, position_id)
                    
                    if status_code == 200:
                        row['order_id'] = open_order_id
                        row['position_id'] = position_id
                        row['quantity'] = detail['quantity']
                        underlying_purchase_price = trade.call_polygon_last_price(row['symbol'])
                        row['underlying_purchase_price'] = underlying_purchase_price
                        log_message_open(row, open_order_id, status_code, json_response,detail['contract_ticker'], option_side,lambda_signifier)
                        orders_list.append(open_order_id)
                        accepted_orders.append({"order_id": open_order_id, "position_id": position_id, "symbol": row['symbol'], "strategy": row['strategy'], "sellby_date":row['sellby_date'],"Call/Put":row['Call/Put'],"underlying_purchase_price": underlying_purchase_price,'return_vol_10D':row['return_vol_10D']})
                        positions_data.append({"position_id": position_id, "underlying_symbol": row['symbol'], "strategy": row['strategy'], "sellby_date":row['sellby_date'],"all_contracts":all_trades,"underlying_purchase_price": underlying_purchase_price,'return_vol_10D':row['return_vol_10D']})        
                    else:
                        trade_data = row.to_dict()
                        trade_data['response'] = status_code
                        failed_transactions.append(trade_data)
                        log_message_open_error(row, open_order_id, status_code, json_response,detail['contract_ticker'],None,lambda_signifier)
                        underlying_purchase_price = 0
                        continue
                except Exception as e:
                    logger.info(f'Place order failed: {e}')
                    trade_data = row.to_dict()
                    trade_data['response'] = is_valid
                    failed_transactions.append(trade_data)
                    continue
            

    positions_df = pd.DataFrame.from_dict(positions_data)
    accepted_df = pd.DataFrame.from_dict(accepted_orders)
    failed_df = pd.DataFrame(failed_transactions)


    s3.put_object(Bucket=trading_data_bucket, Key=f"orders_data/{env}/{date}.csv", Body=positions_df.to_csv())
    s3.put_object(Bucket=trading_data_bucket, Key=f"accepted_orders_data/{env}/{date}.csv", Body=accepted_df.to_csv())
    s3.put_object(Bucket=trading_data_bucket, Key=f"failed_orders/{env}/{date}.csv", Body=failed_df.to_csv())

    # time.sleep(2)

    # processed_df = db.process_opened_ordersv2(accepted_df, base_url, account_id, access_token, env, table)
    # s3.put_object(Bucket=trading_data_bucket, Key=f"processed_orders_enriched/{env}/{date}.csv", Body=processed_df.to_csv())
    return accepted_df

def process_dynamo_orders(formatted_df, base_url, account_id, access_token, table):
    processed_df = db.process_opened_ordersv2(formatted_df, base_url, account_id, access_token, env, table)
    return processed_df


def close_orders(orders_df,  base_url, account_id, access_token, env, table, close_table, lambda_signifier):
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
            log_message_close(row, id, status_code, error_json,lambda_signifier)
            logger.info(f'Close order succesful {row["option_symbol"]} close order id:{id} open order id:{row["order_id"]} for {row["position_id"]}')
        else:
            row_data = row.to_dict()
            row_data['response'] = error_json
            rejected_orders.append(row_data)
            log_message_close(row, id, status_code, error_json,lambda_signifier)

    date = datetime.now().strftime("%Y/%m/%d/%H_%M")

    accepted_df = pd.DataFrame.from_dict(accepted_orders)
    accepted_csv = accepted_df.to_csv()
    rejected_df = pd.DataFrame.from_dict(rejected_orders)
    rejected_csv = rejected_df.to_csv()
    
    s3.put_object(Bucket=trading_data_bucket, Key=f"accepted_closed_orders_data/{env}/{date}.csv", Body=accepted_csv)
    s3.put_object(Bucket=trading_data_bucket, Key=f"rejected_closed_orders_data/{env}/{date}.csv", Body=rejected_csv)

    time.sleep(5)
    closed_orders = db.process_closed_orders(accepted_df, base_url, account_id, access_token, position_ids, env, table, close_table)
    closed_df = pd.DataFrame.from_dict(closed_orders)
    csv = closed_df.to_csv()

    s3_response = s3.put_object(Bucket=trading_data_bucket, Key=f"enriched_closed_orders_data/{env}/{date}.csv", Body=csv)
    return "done"

def close_order(row, env, lambda_signifier, reason):
    base_url, account_id, access_token = trade.get_tradier_credentials(env)
    id, status_code, error_json = trade.position_exit(base_url, account_id, access_token, row['underlying_symbol'], row['option_symbol'], 'sell_to_close', row['qty_executed_open'], order_type, duration, row['position_id'])
    if status_code == 200:
        row['closing_order_id'] = id
        log_message_close(row, id, status_code, reason,error_json,lambda_signifier)
        logger.info(f'Close order succesful {row["option_symbol"]} close order id:{id} open order id:{row["order_id"]} for {row["position_id"]}')
    else:
        row = row.to_dict()
        row['response'] = error_json
        log_message_close(row, id, status_code, error_json,lambda_signifier)
        return None
    return id

def date_performance_check(row, env, lambda_signifier):
    last_price = trade.call_polygon_last_price(row['underlying_symbol'])
    derivative_price = trade.call_polygon_last_price(f"O:row['option_symbol']")
    if derivative_price is None:
        derivative_price = 0
    sell_code, reason = evaluate_performance_inv(last_price, derivative_price, row)
    if sell_code != 0:
        order_data = close_order(row, env, lambda_signifier, reason)
        return order_data
    else:
        return None

def evaluate_performance_inv(current_price, derivative_price, row):
    if row['trading_strategy'] in CDVOL_STRATEGIES:
        if row['trading_strategy'] in THREED_STRATEGIES and row['trading_strategy'] in CALL_STRATEGIES:
            sell_code, reason = tda_CALL_3D_CDVOLAGG(row, current_price,vol=.5)
        elif row['trading_strategy'] in THREED_STRATEGIES and row['trading_strategy'] in PUT_STRATEGIES:
            sell_code, reason = tda_PUT_3D_CDVOLAGG(row, current_price,vol=.5)
        elif row['trading_strategy'] in ONED_STRATEGIES and row['trading_strategy'] in CALL_STRATEGIES:
            sell_code, reason = tda_CALL_1D_CDVOLAGG(row, current_price,vol=.5)
        elif row['trading_strategy'] in ONED_STRATEGIES and row['trading_strategy'] in PUT_STRATEGIES:
            sell_code, reason = tda_PUT_1D_CDVOLAGG(row, current_price,vol=.5)
    else:
        sell_code, reason = 1, "untracked"
    return sell_code, reason


if __name__ == "__main__":
    env = "prod_val"
    account_id = "VA72174659"
    access_token = "ld0Mx4KbsOBYwmJApdowZdFcIxO7"
    base_url = "https://sandbox.tradier.com/v1/"
    dataset = s3.get_object(Bucket="icarus-trading-data", Key="accepted_orders_data/2023/06/22/16_07.csv")
    df = pd.read_csv(dataset.get("Body"))
    pending_orders = process_dynamo_orders(df, base_url, account_id, access_token, env)
    pending_df = pd.DataFrame.from_dict(pending_orders)
