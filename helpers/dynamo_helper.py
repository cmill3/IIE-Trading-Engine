import boto3
import pandas as pd
import os
# import helpers.tradier as trade
import ast
import logging

client = boto3.client('dynamodb')
ddb = boto3.resource('dynamodb','us-east-1')
transactions_table = ddb.Table('icarus-transactions-table')
# orders_table = ddb.Table('icarus-orders-table')
# closed_orders_table = ddb.Table('icarus-closed-orders-table')
# orders_table_inv = ddb.Table('icarus-orders-table')
# closed_orders_table_inv = ddb.Table('icarus-closed-orders-table')
positions_table = ddb.Table('icarus-posititons-table')
logger = logging.getLogger()
user = os.getenv("USER")

execution_strategy = os.getenv("EXECUTION_STRATEGY")

# Define a function to extract values from dict objects
def extract_values_from_dict(d):
    return list(d.values())[0] if isinstance(d, dict) else d
    

def get_all_orders_from_dynamo(table):
    table = ddb.Table(table)
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    df = pd.DataFrame(data)
    return df

def get_open_trades_by_orderid(order_id_list):
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


# def close_dynamo_record_position(position_id,transaction_ids):
#     # pm_data = order_info_obj['pm_data']    
#     position_item = {
#         'position_id': position_id,
#         'transaction_ids': transaction_ids,
#         'position_order_status': "closed"
#     }

#     response = positions_table.put_item(
#             Item=position_item
#         )   
#     return response, position_item


def create_new_dynamo_record_order(order_info_obj, position, position_id, transactions, underlying_purchase_price, trading_mode, table):
    table = ddb.Table(table)    
    # details = ast.literal_eval(position['trade_details'])[0]
    order_item ={
        'order_id': str(order_info_obj['id']),
        'trading_mode': trading_mode,
        'execution_strategy': execution_strategy,
        'underlying_purchase_price': str(underlying_purchase_price),
        'transaction_ids': transactions,
        'underlying_symbol': position['symbol'],
        'position_id': position_id,
        'trading_strategy': position['strategy'],
        'option_symbol': order_info_obj['option_symbol'],
        'option_side': position['Call/Put'],
        # 'two_week_contract_expiry': position['expiry_2wk'],
        'avg_fill_price_open': str(order_info_obj['average_fill_price']),
        'last_fill_price_open': str(order_info_obj['last_fill_price']),
        'qty_executed_open': str(order_info_obj['exec_quantity']),
        'order_creation_date': str(order_info_obj['created_date']),
        'order_transaction_date': str(order_info_obj['transaction_date']),
        'order_status': order_info_obj['status'],
        'sellby_date': position['sellby_date'],
        'user': user,
        'return_vol_10D': str(position['return_vol_10D']),
    }

    response = table.put_item(
            Item=order_item
        )   
    return response, order_item

def create_new_dynamo_record_order_logmessage(order_info_obj,log_message, trading_mode, table):
    table = ddb.Table(table)    
    # details = ast.literal_eval(position['trade_details'])[0]
    order_item ={
        'order_id': str(order_info_obj['id']),
        'trading_mode': trading_mode,
        'execution_strategy': execution_strategy,
        'underlying_purchase_price': log_message['underlying_purchase_price'],
        # 'transaction_ids': transactions,
        'underlying_symbol': log_message['underlying_symbol'],
        'position_id': log_message['position_id'],
        'trading_strategy': log_message['trading_strategy'],
        'option_symbol': order_info_obj['option_symbol'],
        'option_side': log_message['option_side'],
        # 'two_week_contract_expiry': position['expiry_2wk'],
        'avg_fill_price_open': str(order_info_obj['average_fill_price']),
        'last_fill_price_open': str(order_info_obj['last_fill_price']),
        'qty_executed_open': str(order_info_obj['exec_quantity']),
        'order_creation_date': str(order_info_obj['created_date']),
        'order_transaction_date': str(order_info_obj['transaction_date']),
        'order_status': order_info_obj['status'],
        'user': user,
        'return_vol_10D': str(log_message['return_vol_10D']),
        'target_value': str(log_message['target_value']),
    }

    response = table.put_item(
            Item=order_item
        )   
    return response, order_item

def create_new_dynamo_record_order_reconciliation(order_info_obj,position_id,underlying_symbol,order_id,underlying_purchase_price,trading_mode,table):    
    table = ddb.Table(table)    
    order_item ={
        'order_id': str(order_id),
        'trading_mode': trading_mode,
        'execution_strategy': execution_strategy,
        'underlying_purchase_price': str(underlying_purchase_price),
        # 'transaction_ids': transactions,
        'underlying_symbol': underlying_symbol,
        'position_id': position_id,
        # 'trading_strategy': position['strategy'],
        'option_symbol': order_info_obj['option_symbol'],
        # 'option_side': position['Call/Put'],
        # 'two_week_contract_expiry': position['expiry_2wk'],
        'avg_fill_price_open': str(order_info_obj['average_fill_price']),
        'last_fill_price_open': str(order_info_obj['last_fill_price']),
        'qty_executed_open': str(order_info_obj['exec_quantity']),
        'order_creation_date': str(order_info_obj['created_date']),
        'order_transaction_date': str(order_info_obj['transaction_date']),
        'order_status': order_info_obj['status'],
        # 'sellby_date': position['sellby_date'],
        'reconciliation': 'True'
    }

    response = table.put_item(
            Item=order_item 
        )   
    return response, order_item

def create_new_dynamo_record_closed_order(order_info_obj, transaction, trading_mode, table):    
    table = ddb.Table(table) 
    order_item ={
        'order_id': str(transaction['order_id']),
        'closing_order_id': str(order_info_obj['id']),
        'trading_mode': trading_mode,
        'execution_strategy': str(execution_strategy),
        'underlying_symbol': transaction['underlying_symbol'],
        'position_id': transaction['position_id'],
        'trading_strategy': transaction['trading_strategy'],
        'option_symbol': order_info_obj['option_symbol'],
        'open_option_symbol': transaction['option_symbol'],
        'option_side': transaction['option_side'],
        # 'two_week_contract_expiry': transaction['two_week_contract_expiry'],
        'avg_fill_price_open': str(transaction['avg_fill_price_open']),
        'last_fill_price_open': str(transaction['last_fill_price_open']),
        'qty_executed_open': str(transaction['qty_executed_open']),
        'order_creation_date': str(transaction['order_creation_date']),
        'order_transaction_date': str(transaction['order_transaction_date']),
        'close_creation_date': str(order_info_obj['created_date']),
        'close_transaction_date': str(order_info_obj['transaction_date']),
        'avg_fill_price_close': str(order_info_obj['average_fill_price']),
        'last_fill_price_close': str(order_info_obj['last_fill_price']),
        'qty_executed_close': str(order_info_obj['exec_quantity']),
        'user': user,
        "close_reason": transaction['close_reason']
    }

    response = table.put_item(
            Item=order_item
        )   
    return response

def create_new_dynamo_record_closed_order_logmessage(order_info_obj, original_order, log_message, trading_mode, table):    
    table = ddb.Table(table) 
    order_item ={
        'order_id': str(log_message['order_id']),
        'closing_order_id': str(order_info_obj['id']),
        'trading_mode': trading_mode,
        'execution_strategy': str(execution_strategy),
        'underlying_symbol': original_order['underlying_symbol'],
        'position_id': original_order['position_id'],
        'trading_strategy': original_order['trading_strategy'],
        'option_symbol': order_info_obj['option_symbol'],
        'open_option_symbol': original_order['option_symbol'],
        'option_side': original_order['option_side'],
        # 'two_week_contract_expiry': transaction['two_week_contract_expiry'],
        'avg_fill_price_open': str(original_order['avg_fill_price_open']),
        'last_fill_price_open': str(original_order['last_fill_price_open']),
        'qty_executed_open': str(original_order['qty_executed_open']),
        'order_creation_date': str(original_order['order_creation_date']),
        'order_transaction_date': str(log_message['order_transaction_date']),
        'close_creation_date': str(original_order['created_date']),
        'close_transaction_date': str(order_info_obj['transaction_date']),
        'avg_fill_price_close': str(order_info_obj['average_fill_price']),
        'last_fill_price_close': str(order_info_obj['last_fill_price']),
        'qty_executed_close': str(order_info_obj['exec_quantity']),
        'user': user,
        "close_reason": log_message['close_reason']
    }

    response = table.put_item(
            Item=order_item
        )   
    return response


def create_new_dynamo_record_closed_order_reconciliation(order_info_obj, trading_mode):    

    order_item ={
        'order_id': str(order_info_obj['id']),
        'closing_order_id': str(order_info_obj['id']),
        'trading_mode': trading_mode,
        'execution_strategy': str(execution_strategy),
        # 'underlying_symbol': transaction['underlying_symbol'],
        # 'position_id': transaction['position_id'],
        # 'trading_strategy': transaction['trading_strategy'],
        'option_symbol': order_info_obj['option_symbol'],
        # 'open_option_symbol': transaction['option_symbol'],
        # 'option_side': transaction['option_side'],
        # 'two_week_contract_expiry': transaction['two_week_contract_expiry'],
        # 'avg_fill_price_open': str(transaction['avg_fill_price_open']),
        # 'last_fill_price_open': str(transaction['last_fill_price_open']),
        # 'qty_executed_open': str(transaction['qty_executed_open']),
        # 'order_creation_date': str(transaction['order_creation_date']),
        # 'order_transaction_date': str(transaction['order_transaction_date']),
        'close_creation_date': str(order_info_obj['created_date']),
        'close_transaction_date': str(order_info_obj['transaction_date']),
        'avg_fill_price_close': str(order_info_obj['average_fill_price']),
        'last_fill_price_close': str(order_info_obj['last_fill_price']),
        'qty_executed_close': str(order_info_obj['exec_quantity']),
        'reconciliation': "True"
    }

    response = closed_orders_table.put_item(
            Item=order_item
        )   
    return response, order_item

def close_dynamo_record_order(order_info_obj, table):    
    table = ddb.Table(table) 
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
        'order_status': 'closed',
        'user': user
    }

    response = table.put_item(
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

def delete_order_record(order_id, table):
    table = ddb.Table(table) 
    order_info = table.get_item(
        Key={
            'order_id': str(order_id)
        }
    )

    response = table.delete_item(
        Key={
            'order_id': str(order_id)
        }
    )
    return order_info['Item']


def process_opened_orders(data, position_id, base_url, account_id, access_token, trading_mode, table):
    position_transactions_list = []
    order_id_list = []
    unfulfilled_orders = []
    fulfilled_orders = []
    data['orders'] = ast.literal_eval(data['orders'])
    underlying_purchase_price = data['purchase_price']
    # positions_list = trade.get_account_positions(base_url, account_id, access_token)
    # positions_df = pd.DataFrame(positions_list)

    for order_id in data['orders']:
        try:
            order_info_obj = trade.get_order_info(base_url, account_id, access_token, order_id)
            if order_info_obj == "Order not filled":
                unfulfilled_orders.append(order_id)
                continue
            # order_info_obj['order_status'] = "open"
            order_info_obj['trade_result'] = "success"
            create_new_dynamo_record_order(order_info_obj, data, position_id, order_id, underlying_purchase_price, trading_mode, table)
            position_transactions_list.append(order_id)
            fulfilled_orders.append(order_info_obj)
        except Exception as e:
            logger.info(f"Error getting order info {order_id}: {e}")
            unfulfilled_orders.append(order_id)
    if len(unfulfilled_orders) == 0:
        create_new_dynamo_record_position(position_id, data, order_id_list, position_transactions_list, trading_mode)
    return fulfilled_orders, unfulfilled_orders

def process_opened_ordersv2(orders_data, base_url, account_id, access_token, trading_mode, table):
    process_orders = []
    for index, row  in orders_data.iterrows():
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, row['order_id'])
        create_new_dynamo_record_order(order_info_obj, row, row['position_id'], row['order_id'], row['underlying_purchase_price'], trading_mode, table)
        row['avg_fill_price_open'] = order_info_obj['average_fill_price']
        row['last_fill_price_open'] = order_info_obj['last_fill_price']
        row['qty_executed_open'] = order_info_obj['exec_quantity']
        row['order_creation_date'] = order_info_obj['created_date']
        row['order_transaction_date'] = order_info_obj['transaction_date']
        process_orders.append(row.to_dict())
    processed_df = pd.DataFrame.from_dict(process_orders)
    return processed_df

def process_closed_orders(full_transactions_data, base_url, account_id, access_token, position_ids, trading_mode, table, close_table):
    closed_orders = []
    for index, row  in full_transactions_data.iterrows():
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, row['closing_order_id'])
        del_response = delete_order_record(row['order_id'], table)
        create_response, full_order_record = create_new_dynamo_record_closed_order(order_info_obj, row, trading_mode, close_table)
        closed_orders.append(full_order_record)
    final_positions_dict = create_positions_list(full_transactions_data)
    # for position_id, transaction_list in final_positions_dict.items():
    #     close_dynamo_record_position(position_id, transaction_list)
    return closed_orders
    

def create_positions_list(total_transactions):
    positions_dict = {}
    for index, row in total_transactions.iterrows():
        if row['position_id'] in positions_dict:
            positions_dict[row['position_id']].append(row['closing_order_id'])
            positions_dict[row['position_id']].append(row['order_id'])
        else:
            positions_dict[row['position_id']] = [row['closing_order_id'],row['order_id']]
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
    

# if __name__ == "__main__":
#     table = ddb.Table("icarus-orders-table-inv") 
#     order_info = table.get_item(
#         Key={
#             'order_id': str(10215932)
#         }
#     )

#     print(order_info['Item'])