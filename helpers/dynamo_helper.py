import boto3
import pandas as pd
import os
import helpers.tradier as trade
import ast

client = boto3.client('dynamodb')
ddb = boto3.resource('dynamodb','us-east-1')
transactions_table = ddb.Table('icarus-transactions-table')
orders_table = ddb.Table('icarus-orders-table')
closed_orders_table = ddb.Table('icarus-closed-orders-table')
positions_table = ddb.Table('icarus-posititons-table')


execution_strategy = os.getenv("EXECUTION_STRATEGY")

# Define a function to extract values from dict objects
def extract_values_from_dict(d):
    return list(d.values())[0] if isinstance(d, dict) else d
    

def get_all_orders_from_dynamo():
    response = orders_table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = orders_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    df = pd.DataFrame(data)
    return df

def get_open_trades_by_orderid(order_id_list):
    print(order_id_list)
    partitions = break_array_into_partitions(order_id_list)
    df_list = []
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
    # pm_data = order_info_obj['pm_data']    
    position_item = {
        'position_id': order_info_obj['position_id'],
        'transaction_ids': transaction_ids,
        'position_order_status': "closed"
    }

    response = positions_table.put_item(
            Item=position_item
        )   
    return response, position_item


def create_new_dynamo_record_order(order_info_obj, position, position_id, transactions, underlying_purchase_price, trading_mode):    
    # details = ast.literal_eval(position['trade_details'])[0]
    print(order_info_obj)
    order_item ={
        'order_id': str(order_info_obj['id']),
        'trading_mode': trading_mode,
        'execution_strategy': execution_strategy,
        'underlying_purchase_price': underlying_purchase_price,
        # 'datetimestamp': datetime_stamp,
        'transaction_ids': transactions,
        'underlying_symbol': position['symbol'],
        'position_id': position_id,
        'trading_strategy': position['strategy'],
        'option_symbol': order_info_obj['option_symbol'],
        'option_side': position['Call/Put'],
        # 'strike_price': details['strike'],
        'two_week_contract_expiry': position['expiry_2wk'],
        'avg_fill_price_open': str(order_info_obj['average_fill_price']),
        'last_fill_price_open': str(order_info_obj['last_fill_price']),
        'qty_executed_open': str(order_info_obj['exec_quantity']),
        'order_creation_date': str(order_info_obj['created_date']),
        'order_transaction_date': str(order_info_obj['transaction_date']),
        'order_status': order_info_obj['status'],
        'sellby_date': position['sellby_date']
    }

    response = orders_table.put_item(
            Item=order_item
        )   
    return response, order_item

def create_new_dynamo_record_closed_order(order_info_obj, transaction, trading_mode):    
    # details = ast.literal_eval(position['trade_details'])[0]

    order_item ={
        'order_id': str(transaction['order_id']),
        'closing_order_id': str(order_info_obj['id']),
        'trading_mode': trading_mode,
        'execution_strategy': execution_strategy,
        # 'datetimestamp': datetime_stamp,
        'underlying_symbol': transaction['symbol'],
        'position_id': transaction['position_id'],
        'trading_strategy': transaction['strategy'],
        'option_symbol': order_info_obj['option_symbol'],
        'option_side': transaction['Call/Put'],
        # 'strike_price': details['strike'],
        'two_week_contract_expiry': transaction['expiry_2wk'],
        'avg_fill_price_open': str(transaction['avg_fill_price_open']),
        'last_fill_price_open': str(transaction['last_fill_price_open']),
        'qty_executed_open': str(transaction['qty_executed_open']),
        'order_creation_date': str(transaction['order_creation_date']),
        'order_transaction_date': str(transaction['order_transaction_date']),
        'closing_order_id': order_info_obj['closing_order_id'],
        'close_creation_date': order_info_obj['created_date'],
        'close_transaction_date': order_info_obj['transaction_date'],
        'avg_fill_price_close': order_info_obj['average_fill_price'],
        'last_fill_price_close': order_info_obj['last_fill_price'],
        'qty_executed_close': order_info_obj['exec_quantity'],
    }

    response = orders_table.put_item(
            Item=order_item
        )   
    return response, order_item

def close_dynamo_record_order(order_info_obj):    
    pm_data = order_info_obj['pm_data']
    order_item ={
        'order_id': order_info_obj['order_id'],
        # 'trade_close_outcome': order_info_obj['trade_result'],
        'closing_order_id': order_info_obj['closing_order_id'],
        'close_creation_date': order_info_obj['created_date'],
        'close_transaction_date': order_info_obj['transaction_date'],
        'avg_fill_price_close': order_info_obj['average_fill_price'],
        'last_fill_price_close': order_info_obj['last_fill_price'],
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
        'option_symbol': order_info_obj['option_symbol'],
        'trade_open_outcome': order_info_obj['trade_result'],
        'avg_fill_price_open': str(order_info_obj['average_fill_price']),
        'last_fill_price_open': str(order_info_obj['last_fill_price']),
        'qty_executed_open': str(order_info_obj['exec_quantity']),
        'order_creation_date': str(order_info_obj['created_date']),
        'order_transaction_date': str(order_info_obj['transaction_date']),
        'order_status': 'open'
    }


    response = transactions_table.put_item(
            Item=transaction_item
        )
    return response, transaction_item


def close_dynamo_record_transaction(order_info_obj):    

    transaction_item ={
        'transaction_id': order_info_obj['transaction_id'],
        'close_creation_date': order_info_obj['created_date'],
        'close_transaction_date': order_info_obj['transaction_date'],
        'avg_fill_price_close': order_info_obj['average_fill_price'],
        'last_fill_price_close': order_info_obj['last_fill_price'],
        'qty_executed_close': order_info_obj['exec_quantity'],
        'order_status': order_info_obj['order_status']
    }


    response = transactions_table.put_item(
            Item=transaction_item
        )
    return response, transaction_item

def delete_order_record(order_id):
    response = orders_table.delete_item(
        Key={
            'order_id': order_id
        }
    )
    return response


def process_opened_orders(data, position_id, base_url, account_id, access_token, trading_mode):
    position_transactions_list = []
    order_id_list = []
    unfulfilled_orders = []
    fulfilled_orders = []
    data['orders'] = ast.literal_eval(data['orders'])
    underlying_purchase_price = data['purchase_price']
    # positions_list = trade.get_account_positions(base_url, account_id, access_token)
    # positions_df = pd.DataFrame(positions_list)

    for order_id in data['orders']:
        print(order_id)
        try:
            order_info_obj = trade.get_order_info(base_url, account_id, access_token, order_id)
            if order_info_obj == "Order not filled":
                unfulfilled_orders.append(order_id)
                continue
            order_info_obj['order_status'] = "open"
            order_info_obj['trade_result'] = "success"
            create_new_dynamo_record_order(order_info_obj, data, position_id, order_id, underlying_purchase_price, trading_mode)
            # create_new_dynamo_record_transaction(order_id, position_id, temp['id'], order_info_obj, data, trading_mode)
            position_transactions_list.append(order_id)
            fulfilled_orders.append(order_info_obj)
        except Exception as e:
            print("Fail",e)
            unfulfilled_orders.append(order_id)
    if len(unfulfilled_orders) == 0:
        create_new_dynamo_record_position(position_id, data, order_id_list, position_transactions_list, trading_mode)
    return fulfilled_orders, unfulfilled_orders


def process_closed_orders(full_transactions_data, base_url, account_id, access_token,position_ids, trading_mode):
    for transaction in full_transactions_data:
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, transaction['closing_order_id'])
        del_response = delete_order_record(transaction['order_id'])
        create_response = create_new_dynamo_record_closed_order(order_info_obj, transaction, trading_mode)
        # close_dynamo_record_transaction(order_info_obj)
    final_positions_dict = create_positions_list(full_transactions_data)
    for position_id, transaction_list in final_positions_dict.items():
        close_dynamo_record_position(position_id, transaction_list)


def create_positions_list(total_transactions):
    positions_dict = {}
    for position in total_transactions:
        if position['position_id'] in positions_dict:
            positions_dict[position['position_id']].append(position['closing_order_id'])
        else:
            positions_dict[position['position_id']] = [position['closing_order_id']]
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