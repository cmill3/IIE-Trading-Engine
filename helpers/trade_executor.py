import tradier as trade
import pandas as pd
from helpers import dynamo_helper as db

order_side = "buy_to_open"
order_type = "market"
duration = "GTC"
acct_balance_min = 20000


def run_executor(data, trading_mode):
    base_url, access_token, account_id = trade.get_tradier_credentials(trading_mode)
    execute_trade(data, base_url, access_token, account_id)

     
#The portion that takes the account balance and allocates a certain amount will be below - currently unweighted**
def account_value_checkpoint(current_balance) -> dict:
    if  current_balance >= acct_balance_min:
        return True
    else:
        return False
    
def create_ids(underlying, option_name, trading_strategy, datetime):
    position_id = underlying + "_" + trading_strategy + "_" + datetime
    transaction_id = option_name + "_" + datetime
    return position_id, transaction_id
    
def execute_trade(data, account_id, trading_mode):
    transaction_data = []
    for i, row in data.iterrows():
        account_balance = trade.get_account_balance(trading_mode, account_id)
        is_valid = account_value_checkpoint(account_balance)
        if is_valid:
            contract_valid = trade.verify_contract(row["contract_name"])
            if contract_valid:
                open_order_id = trade.place_order(trading_mode, account_id, row['symbol'], row['option_contract'], order_side, row['quantity'], order_type, duration)
                if open_order_id != "None":
                    order_info_obj = trade.get_order_info(trading_mode, account_id, open_order_id)
                    order_info_obj['position_id'], order_info_obj['transaction_id'] = create_ids(trade_obj["symbol"], row["option_contract"], row["trading_strategy"], order_info_obj['created_date'])
                    order_info_obj["pm_data"] = row.to_dict()
                    order_info_obj['order_id'] = open_order_id
                    order_info_obj['account_balance'] = account_balance
                    transaction_data.append(order_info_obj)
                    db.create_dynamo_record(trading_mode, order_info_obj)
                else:
                    order_info_obj['status'] = "failed"
                    order_info_obj["pm_data"] = row.to_dict()
        
    for trade_obj in transaction_data:
        db.create_dynamo_record(trade_obj)
   
    df_final_csv.to_csv(f'/Users/ogdiz/Projects/APE-Executor/Portfolio-Manager-v0/PM-Tradier/Executor_Exports/PM-Exec-{datetime_csv}.csv')
