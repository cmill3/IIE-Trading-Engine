import boto3
import pandas as pd
import os

ddb = boto3.resource('dynamodb','us-east-1')
transactions_table = ddb.Table('icarus-transaction-table')
orders_table = ddb.Table('icarus-orders-table')
positions_table = ddb.Table('icarus-positions-table')
client = boto3.client('dynamodb')

execution_strategy = os.getenv("EXECUTION_STRATEGY")


def get_open_trades_by_orderid(order_id_list):
    response = client.batch_get_item(
        RequestItems={
            'icarus-orders-table': {
                'Keys': [{'order_id': {'S': id}} for id in order_id_list]
            }
        }
    )
    items = response['Responses']['icarus-orders-table']
    result_df = pd.DataFrame(items)

    return result_df

def create_new_dynamo_record_position(order_info_obj,transaction_ids, order_ids, trading_mode):
    pm_data = order_info_obj['pm_data']    
    position_item = {
        'position_id': order_info_obj['position_id'],
        'trading_mode': trading_mode,
        'order_ids': order_ids,
        'transaction_ids': transaction_ids,
        'trading_strategy': pm_data['strategy'],
        'two_week_contract_expiry': pm_data['expiry_2wk'],
        'position_order_status': order_info_obj['order_status']
    }
    response = positions_table.put_item(
            Item=position_item
        )   
    return response, position_item

def close_dynamo_record_position(order_info_obj,transaction_ids):
    pm_data = order_info_obj['pm_data']    
    position_item = {
        'position_id': order_info_obj['position_id'],
        'transaction_ids': transaction_ids,
        'position_order_status': order_info_obj['order_status']
    }

    response = positions_table.put_item(
            Item=position_item
        )   
    return response, position_item


def create_new_dynamo_record_order(order_info_obj,transaction_ids, trading_mode):    
    pm_data = order_info_obj['pm_data']
    order_item ={
        'order_id': order_info_obj['order_id'],
        'trading_mode': trading_mode,
        'execution_strategy': execution_strategy,
        # 'datetimestamp': datetime_stamp,
        'transaction_ids': transaction_ids,
        'position_id': order_info_obj['position_id'],
        'trading_strategy': pm_data['strategy'],
        'option_name': order_info_obj['option_name'],
        'option_side': pm_data['Call/Put'],
        'strike_price': pm_data['strike'],
        'two_week_contract_expiry': pm_data['expiry_2wk'],
        'trade_open_outcome': order_info_obj['trade_result'],
        'avg_fill_price_open': order_info_obj['avg_fill_price_open'],
        'last_fill_price_open': order_info_obj['last_fill_price_open'],
        'qty_executed_open': order_info_obj['exec_quantity'],
        'order_creation_date': order_info_obj['order_created_datetime'],
        'order_transaction_date': order_info_obj['order_transaction_datetime'],
        'closed_creation_date': order_info_obj['order_created_datetime'],
        'order_transaction_date': order_info_obj['order_transaction_datetime'],
        'order_status': order_info_obj['order_status']
    }
    print(order_item)
    response = orders_table.put_item(
            Item=order_item
        )   
    return response, order_item

def close_dynamo_record_order(order_info_obj):    
    pm_data = order_info_obj['pm_data']
    order_item ={
        'order_id': order_info_obj['order_id'],
        'transaction_ids': order_info_obj['transaction_ids'],
        'trade_close_outcome': order_info_obj['trade_result'],
        'closing_order_id': order_info_obj['closing_order_id'],
        'close_creation_date': order_info_obj['order_created_datetime'],
        'close_transaction_date': order_info_obj['order_transaction_datetime'],
        'avg_fill_price_close': order_info_obj['avg_fill_price_close'],
        'last_fill_price_close': order_info_obj['last_fill_price_close'],
        'qty_executed_close': order_info_obj['exec_quantity'],
        'order_status': order_info_obj['order_status']
    }
    print(order_item)
    response = orders_table.put_item(
            Item=order_item
        )   
    return response, order_item

def create_new_dynamo_record_transaction(order_info_obj, trading_mode):    

    pm_data = order_info_obj['pm_data']
    ## FILL IN
    transaction_item ={
        'transaction_id': order_info_obj['transaction_id'],
        'trading_mode': trading_mode,
        'execution_strategy': execution_strategy,
        'order_id': order_info_obj['order_id'],
        'position_id': order_info_obj['position_id'],
        'trading_strategy': pm_data['strategy'],
        'option_name': order_info_obj['option_name'],
        'trade_open_outcome': order_info_obj['trade_result'],
        'avg_fill_price_open': order_info_obj['avg_fill_price_open'],
        'last_fill_price_open': order_info_obj['last_fill_price_open'],
        'qty_placed_open': order_info_obj['qty_placed_open'],
        'qty_executed_open': order_info_obj['qty_executed_open'],
        'order_creation_date': order_info_obj['order_created_datetime'],
        'order_transaction_date': order_info_obj['order_transaction_datetime'],
        'order_status': order_info_obj['order_status']
    }


    response = transactions_table.put_item(
            Item=transaction_item
        )
    return response, transaction_item

def close_dynamo_record_transaction(order_info_obj):    

    pm_data = order_info_obj['pm_data']
    ## FILL IN
    transaction_item ={
        'transaction_id': order_info_obj['transaction_id'],
        'close_creation_date': order_info_obj['order_created_datetime'],
        'close_transaction_date': order_info_obj['order_transaction_datetime'],
        'avg_fill_price_close': order_info_obj['avg_fill_price_close'],
        'last_fill_price_close': order_info_obj['last_fill_price_close'],
        'qty_executed_close': order_info_obj['exec_quantity'],
        'order_status': order_info_obj['order_status']
    }


    response = transactions_table.put_item(
            Item=transaction_item
        )
    return response, transaction_item