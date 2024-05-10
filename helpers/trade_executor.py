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
import warnings
warnings.filterwarnings('ignore')



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
orders_table = os.getenv("TABLE")
close_table = os.getenv("CLOSE_TABLE")
portfolio_strategy = os.getenv("PORTFOLIO_STRATEGY")

order_type = "market"
duration = "GTC"
    
def execute_new_trades(data,table, lambda_signifier):
    base_url, account_id, access_token = trade.get_tradier_credentials(env)
    trading_balance = db.get_trading_balance(portfolio_strategy,env)
    positions_data = []
    failed_transactions = []
    orders_list = []
    accepted_orders = []
    for i, row in data.iterrows():
        is_valid = False
        position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{dt_posId}"
                                    
        if (row['strategy'] in THREED_STRATEGIES and now.date().weekday() <= 2) or (row['strategy'] in ONED_STRATEGIES and now.date().weekday() <= 3):
            is_valid = True

        if is_valid:
            try: 
                if now.date().weekday() < 2:
                    row['trade_details'] = ast.literal_eval(row['trade_details1wk'])
                else:
                    row['trade_details'] = ast.literal_eval(row['trade_details2wk'])
            except:
                continue
            all_trades = row['trade_details']
            trades = pd.DataFrame.from_dict(all_trades)
            if len(trades) == 0:
                continue
            trades = trades.loc[trades['quantity'] != 0]
            if row['strategy'] in CALL_STRATEGIES:
                option_side = "call"
            elif row['strategy'] in PUT_STRATEGIES:
                option_side = "put"

            for _, detail in trades.iterrows():
                try: 
                    open_order_id, status_code, json_response = trade.place_order(base_url, account_id, access_token, row['symbol'], 
                                                                            detail["contract_ticker"], detail['quantity'], 
                                                                            order_type, duration, position_id)
                    
                    if status_code == 200:
                        trading_balance, underlying_purchase_price = process_open_order(row,open_order_id,position_id,json_response,status_code,lambda_signifier,detail,option_side)
                        orders_list.append(open_order_id)
                        accepted_orders.append({"order_id": open_order_id, "position_id": position_id, "symbol": row['symbol'], "strategy": row['strategy'], "sellby_date":row['sellby_date'],"Call/Put":row['Call/Put'],"underlying_purchase_price": underlying_purchase_price,'return_vol_10D':row['return_vol_10D'],"spread_position":detail['spread_position']})
                        positions_data.append({"position_id": position_id, "underlying_symbol": row['symbol'], "strategy": row['strategy'], "sellby_date":row['sellby_date'],"all_contracts":all_trades,"underlying_purchase_price": underlying_purchase_price,'return_vol_10D':row['return_vol_10D'],"spread_position":detail['spread_position']})        
                    else:
                        trade_data = row.to_dict()
                        trade_data['response'] = status_code
                        failed_transactions.append(trade_data)
                        log_message_open_error(row, open_order_id, status_code, json_response,detail['contract_ticker'],None,lambda_signifier)
                        underlying_purchase_price = 0
                        continue
                except Exception as e:
                    print(f'New Trade Failed: {e}')
                    trade_data = row.to_dict()
                    trade_data['response'] = is_valid
                    failed_transactions.append(trade_data)
                    continue
            
    logger.info(f'Final Balance: {trading_balance}')
    positions_df = pd.DataFrame.from_dict(positions_data)
    accepted_df = pd.DataFrame.from_dict(accepted_orders)
    failed_df = pd.DataFrame(failed_transactions)

    balance_dict = {"strategy_name": f"{portfolio_strategy}-DEV", "balance": trading_balance, "date": datetime.now().strftime("%Y/%m/%d/%H/%M")}
    df = pd.DataFrame([balance_dict])
    s3.put_object(Bucket='inv-alerts-trading-data', Key=f'trading_balance/{env}/{datetime.now().strftime("%Y/%m/%d/%H/%M")}', Body=df.to_csv(index=False))
    s3.put_object(Bucket=trading_data_bucket, Key=f"orders_data/{env}/{date}.csv", Body=positions_df.to_csv())
    s3.put_object(Bucket=trading_data_bucket, Key=f"accepted_orders_data/{env}/{date}.csv", Body=accepted_df.to_csv())
    s3.put_object(Bucket=trading_data_bucket, Key=f"failed_orders/{env}/{date}.csv", Body=failed_df.to_csv())
    return accepted_df

def process_open_order(row,open_order_id,position_id,json_response, status_code,lambda_signifier,detail,option_side):
    base_url, account_id, access_token = trade.get_tradier_credentials(env)
    try:
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, open_order_id)
        underlying_purchase_price = trade.call_polygon_last_price(row['symbol'])

        row['order_id'] = open_order_id
        row['position_id'] = position_id
        row['underlying_purchase_price'] = underlying_purchase_price
        row['quantity'] = order_info_obj['exec_quantity']

        log_message_open(row, open_order_id, status_code, json_response,detail['contract_ticker'], option_side,lambda_signifier,detail)
        capital_spent = (float(order_info_obj['exec_quantity']) * float(order_info_obj['average_fill_price'])) * 100
        logger.info(f'Capital spent: {capital_spent}')
        db.create_new_dynamo_record_order_logmessage(order_info_obj,underlying_purchase_price,row, env, orders_table,detail)
        new_balance = db.update_trading_balance(capital_spent,portfolio_strategy,env,action_type="open")
    except Exception as e:
        logger.info(f'Place Order Failed: {e}')
        logger.info(row)
        return 0, 0

    return new_balance, underlying_purchase_price

# def process_dynamo_orders(formatted_df, base_url, account_id, access_token, table):
#     processed_df = db.process_opened_ordersv2(formatted_df, base_url, account_id, access_token, env, table)
#     return processed_df

def close_order(row, env, lambda_signifier, reason):
    base_url, account_id, access_token = trade.get_tradier_credentials(env)
    closing_id, status_code, error_json = trade.position_exit(base_url, account_id, access_token, row['underlying_symbol'], row['option_symbol'], 'sell_to_close', row['qty_executed_open'], order_type, duration, row['position_id'])
    if status_code == 200:
        row['closing_order_id'] = closing_id
        close_order_info_obj = trade.get_order_info(base_url, account_id, access_token, closing_id)
        open_order_info_obj = trade.get_order_info(base_url, account_id, access_token, row['order_id'])
        try:
            capital_return = float(close_order_info_obj['exec_quantity']) * float(close_order_info_obj['average_fill_price']) * 100
            log_message_close(row, closing_id, status_code, reason, error_json,lambda_signifier)
            db.delete_order_record(row['order_id'],orders_table)
            create_response = db.create_new_dynamo_record_closed_order_logmessage(close_order_info_obj, open_order_info_obj, row['order_id'], env, close_table, reason,row)
            logger.info(f'Close order succesful {row["option_symbol"]} close order id:{closing_id} open order id:{row["order_id"]} for {row["position_id"]}')
        except Exception as e:
            logger.info(f"FAILURE IN CLOSED ORDER for {row['option_symbol']}")
            logger.info(e)
            logger.info(close_order_info_obj)
            logger.info(open_order_info_obj)
            return None, 0

    else:
        row = row.to_dict()
        row['response'] = error_json
        log_message_close(row, closing_id, status_code, reason, error_json,lambda_signifier)
        return None, 0
    return closing_id, capital_return

def date_performance_check(row, env, lambda_signifier):
    last_price = trade.call_polygon_last_price(row['underlying_symbol'])
    derivative_price = trade.call_polygon_last_price(f"O:row['option_symbol']")
    if derivative_price is None:
        derivative_price = 0
    sell_code, reason = evaluate_performance_inv(last_price, derivative_price, row)
    if sell_code != 0:
        closing_order_id, capital_return = close_order(row, env, lambda_signifier, reason)
        return closing_order_id, capital_return
    else:
        return None, None
    
def evaluate_performance_inv(current_price, derivative_price, row):
    if row['trading_strategy'] in CDVOL_STRATEGIES:
        if row['trading_strategy'] in THREED_STRATEGIES and row['trading_strategy'] in CALL_STRATEGIES:
            sell_code, reason = tda_CALL_3D_CDVOLAGG(row, current_price,vol=.4)
        elif row['trading_strategy'] in THREED_STRATEGIES and row['trading_strategy'] in PUT_STRATEGIES:
            sell_code, reason = tda_PUT_3D_CDVOLAGG(row, current_price,vol=.4)
        elif row['trading_strategy'] in ONED_STRATEGIES and row['trading_strategy'] in CALL_STRATEGIES:
            sell_code, reason = tda_CALL_1D_CDVOLAGG(row, current_price,vol=.4)
        elif row['trading_strategy'] in ONED_STRATEGIES and row['trading_strategy'] in PUT_STRATEGIES:
            sell_code, reason = tda_PUT_1D_CDVOLAGG(row, current_price,vol=.4)
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
