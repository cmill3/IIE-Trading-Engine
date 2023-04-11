import boto3

ddb = boto3.resource('dynamodb','us-east-1')
transactions_table = ddb.Table('icarus-transaction-table')
orders_table = ddb.Table('icarus-orders-table')
positions_table = ddb.Table('icarus-positions-table')


def create_new_dynamo_record_transaction(order_info_obj, trading_mode):    

    pm_data = order_info_obj['pm_data']
    ## FILL IN
    transaction_item ={
        'transaction_id': order_info_obj['transaction_id'],
        'trading_mode': trading_mode,
        'order_id': order_info_obj['order_id'],
        # 'datetimestamp': datetime_stamp,
        'position_id': order_info_obj['position_id'],
        'trading_strategy': pm_data['trading_strategy'],
        'option_name': order_info_obj['option_name'],
        'trade_open_outcome': order_info_obj['trade_result'],
        'avg_fill_price_open': order_info_obj['avg_fill_price_open'],
        'last_fill_price_open': order_info_obj['last_fill_price_open'],
        'qty_placed_open': order_info_obj['qty_placed_open'],
        'qty_executed_open': order_info_obj['qty_executed_open'],
        'order_creation_date': order_info_obj['order_created_datetime'],
        'order_transaction_date': order_info_obj['order_transaction_datetime'],
        'closing_order_id': None,
        'avg_fill_price_open': None,
        'last_fill_price_open': None,
        'qty_placed_open': None,
        'qty_executed_open': None,
        'order_status': order_info_obj['order_status']
    }


    response = transactions_table.put_item(
            Item=transaction_item
        )
    return response, transaction_item

def create_new_dynamo_record_order(order_info_obj,transaction_ids, trading_mode):    
    pm_data = order_info_obj['pm_data']
    order_item ={
        'order_id': order_info_obj['order_id'],
        'trading_mode': trading_mode,
        # 'datetimestamp': datetime_stamp,
        'transaction_ids': transaction_ids,
        'position_id': order_info_obj['position_id'],
        'trading_strategy': pm_data['trading_strategy'],
        'option_name': order_info_obj['option_name'],
        'option_side': pm_data['Call/Put'],
        'strike_price': pm_data['strike'],
        'two_week_contract_expiry': pm_data['expiry_2wk'],
        'trade_open_outcome': order_info_obj['trade_result'],
        'avg_fill_price_open': order_info_obj['avg_fill_price_open'],
        'last_fill_price_open': order_info_obj['last_fill_price_open'],
        'qty_placed_open': order_info_obj['qty_placed_open'],
        'qty_executed_open': order_info_obj['qty_executed_open'],
        'order_creation_date': order_info_obj['order_created_datetime'],
        'order_transaction_date': order_info_obj['order_transaction_datetime'],
        'trade_close_outcome': None,
        'closing_order_id': None,
        'avg_fill_price_open': None,
        'last_fill_price_open': None,
        'qty_placed_open': None,
        'qty_executed_open': None,
        'PITM_Balance': order_info_obj['account_balance'],
        'order_status': order_info_obj['order_status']
    }
    print(order_item)
    response = orders_table.put_item(
            Item=order_item
        )   
    return response, order_item

def create_new_dynamo_record_position(order_info_obj,transaction_ids, order_ids, trading_mode):
    pm_data = order_info_obj['pm_data']    
    position_item = {
        'position_id': order_info_obj['position_id'],
        'trading_mode': trading_mode,
        'order_ids': order_ids,
        # 'datetimestamp': datetime_stamp,
        'transaction_ids': transaction_ids,
        'trading_strategy': pm_data['trading_strategy'],
        # 'option_names': option_name,
        'two_week_contract_expiry': pm_data['expiry_2wk'],
        'closing_order_ids': None,
        'position_order_status': order_info_obj['order_status']
    }

    response = positions_table.put_item(
            Item=position_item
        )   
    return response, position_item