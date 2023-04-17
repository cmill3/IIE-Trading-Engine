import boto3
import pandas as pd
import os
import helpers.tradier as trade
import ast

ddb = boto3.resource('dynamodb','us-east-1')
transactions_table = ddb.Table('icarus-transaction-table')
orders_table = ddb.Table('icarus-orders-table')
positions_table = ddb.Table('icarus-positions-table')
client = boto3.client('dynamodb')

execution_strategy = os.getenv("EXECUTION_STRATEGY")

# Define a function to extract values from dict objects
def extract_values_from_dict(d):
    return list(d.values())[0] if isinstance(d, dict) else d
    


def get_open_trades_by_orderid(order_id_list):
    partitions = break_array_into_partitions(order_id_list)
    df_list = []
    print(partitions)
    for partition in partitions:
        response = client.batch_get_item(
            RequestItems={
                'icarus-orders-table': {
                    'Keys': [{'order_id': {'S': str(id)}} for id in partition]
                }
            }
        )
        
        items = response['Responses']['icarus-orders-table']
        result_df = pd.DataFrame(items)
        result_df = result_df.applymap(extract_values_from_dict)
        df_list.append(result_df)
    full_df = pd.concat(df_list)
    print(full_df)
    return full_df

def create_new_dynamo_record_position(position_id, position, order_ids, transaction_ids, trading_mode):
    position_item = {
        'position_id': position_id,
        'trading_mode': trading_mode,
        'order_ids': order_ids,
        'transaction_ids': transaction_ids,
        'trading_strategy': position['strategy'],
        'two_week_contract_expiry': position['expiry_2wk'],
        'position_order_status': "open"
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


def create_new_dynamo_record_order(order_info_obj, position, position_id, order_id, transactions, trading_mode):    
    order_item ={
        'order_id': order_id,
        'trading_mode': trading_mode,
        'execution_strategy': execution_strategy,
        # 'datetimestamp': datetime_stamp,
        'transaction_ids': transactions,
        'underlying_symbol': position['symbol'],
        'position_id': position_id,
        'trading_strategy': position['strategy'],
        'option_name': order_info_obj['option_name'],
        'option_side': position['Call/Put'],
        'strike_price': position['strike'],
        'two_week_contract_expiry': position['expiry_2wk'],
        'trade_open_outcome': order_info_obj['trade_result'],
        'avg_fill_price_open': order_info_obj['avg_fill_price_open'],
        'last_fill_price_open': order_info_obj['last_fill_price_open'],
        'qty_executed_open': order_info_obj['exec_quantity'],
        'order_creation_date': order_info_obj['order_created_datetime'],
        'order_transaction_date': order_info_obj['order_transaction_datetime'],
        'closed_creation_date': order_info_obj['order_created_datetime'],
        'order_transaction_date': order_info_obj['order_transaction_datetime'],
        'order_status': 'open'
    }

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
        'order_status': 'closed'
    }

    response = orders_table.put_item(
            Item=order_item
        )   
    return response, order_item

def create_new_dynamo_record_transaction(transaction_id, position_id, order_id, order_info_obj, position, trading_mode):    
    transaction_item ={
        'transaction_id': transaction_id,
        'trading_mode': trading_mode,
        'execution_strategy': execution_strategy,
        'order_id': order_id,
        'position_id': position_id,
        'trading_strategy': position['strategy'],
        'option_name': order_info_obj['option_name'],
        'trade_open_outcome': order_info_obj['trade_result'],
        'avg_fill_price_open': order_info_obj['avg_fill_price_open'],
        'last_fill_price_open': order_info_obj['last_fill_price_open'],
        'qty_placed_open': order_info_obj['qty_placed_open'],
        'qty_executed_open': order_info_obj['qty_executed_open'],
        'order_creation_date': order_info_obj['order_created_datetime'],
        'order_transaction_date': order_info_obj['order_transaction_datetime'],
        'order_status': 'open'
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


def process_opened_orders(data, position_id, base_url, access_token, account_id, trading_mode):
    position_transactions_list = []
    order_id_list = []
    unfulfilled_orders = []
    fulfilled_orders = []
    data['orders'] = ast.literal_eval(data['orders'])

    for order in data['orders']:
        for order_id in order:
            try:
                order_info_obj = trade.get_order_info(base_url, account_id, access_token, order_id)
                order_id_list.append(order_id)
                # order_info_obj['account_balance'] = data['account_balance']
                order_info_obj['order_status'] = "open"
                order_info_obj['trade_result'] = "success"
                transactions = order[order_id]
                create_new_dynamo_record_order(order_info_obj, data, transactions, trading_mode)
                for transaction in transactions:
                    create_new_dynamo_record_transaction(transaction, position_id, order_id, order_info_obj, data, trading_mode)
                    position_transactions_list.append(transaction)
                fulfilled_orders.append(order_id)
            except Exception as e:
                print(e)
                unfulfilled_orders.append(order_id)
    if len(unfulfilled_orders) == 0:
        create_new_dynamo_record_position(position_id, data, order_id_list, position_transactions_list, trading_mode)
    return fulfilled_orders, unfulfilled_orders

def process_closed_orders(full_transactions_data, base_url, access_token, account_id, position_ids, trading_mode):
    for transaction in full_transactions_data:
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, transaction['closing_order_id'])
        close_dynamo_record_order(order_info_obj)
        close_dynamo_record_transaction(order_info_obj)
        final_positions_dict = create_positions_list(transaction['transactions'])
    for position_id, transaction_list in final_positions_dict.items():
        close_dynamo_record_position(position_id, transaction_list)

def create_positions_list(total_transactions):
    positions_dict = {}
    for position in total_transactions:
        for key, value in position.items():
            if key in positions_dict:
                positions_dict[key].extend(value)
            else:
                positions_dict[key] = value
    return positions_dict


def break_array_into_partitions(arr):
    """
    Breaks an array into equal partitions of less than 100 items each.

    Args:
        arr (list): The input array.

    Returns:
        list: A list of partitions, where each partition is a list of items.
    """
    if len(arr) <= 100:
        # If the array has 100 or fewer items, return it as-is
        return [arr]
    else:
        # Calculate the number of partitions needed
        num_partitions = (len(arr) + 99) // 100  # Round up to the nearest integer

        # Calculate the size of each partition
        partition_size = len(arr) // num_partitions

        # Initialize the starting index and ending index for each partition
        start = 0
        end = partition_size

        # Create the partitions
        partitions = []
        for i in range(num_partitions):
            # If it's the last partition, include any remaining items
            if i == num_partitions - 1:
                partitions.append(arr[start:])
            else:
                partitions.append(arr[start:end])

            # Update the starting and ending index for the next partition
            start = end
            end += partition_size

        return partitions