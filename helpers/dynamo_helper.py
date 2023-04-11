import boto3


def create_dynamo_record(Trade_Type, order_id, datetime_stamp, transaction_id, position_id, trading_strategy, option_name, 
                         option_side, strike_price, contract_expiry, open_outcome, avg_fill_price_open, last_fill_price_open,
                         qty_placed_open, qty_executed_open, order_created_datetime, order_transaction_datetime, bp_balance, order_status):    
    ddb = boto3.resource('dynamodb','us-east-1')
    transactions_table = ddb.Table('icarus-transaction-table')
    orders_table = ddb.Table('icarus-orders-table')
    positions_table = ddb.Table('icarus-positions-table')

    ## FILL IN
    transaction_item ={
        'transaction_id': transaction_id,
        'trade_type': Trade_Type,
        'order_id': order_id,
        'datetimestamp': datetime_stamp,
        'position_id': position_id,
        'trading_strategy': trading_strategy,
        'option_name': option_name,
        'trade_open_outcome': open_outcome,
        'avg_fill_price_open': avg_fill_price_open,
        'last_fill_price_open': last_fill_price_open,
        'qty_placed_open': qty_placed_open,
        'qty_executed_open': qty_executed_open,
        'order_creation_dt': order_created_datetime,
        'order_transaction_dt': order_transaction_datetime,
        'closing_order_id': None,
        'avg_fill_price_open': None,
        'last_fill_price_open': None,
        'qty_placed_open': None,
        'qty_executed_open': None,
        'order_status': order_status
    }
    order_item ={
        'order_id': order_id,
        'trade_type': Trade_Type,
        'datetimestamp': datetime_stamp,
        'transaction_id': transaction_id,
        'position_id': position_id,
        'trading_strategy': trading_strategy,
        'option_name': option_name,
        'option_side': option_side,
        'strike_price': strike_price,
        'two_week_contract_expiry': contract_expiry,
        'trade_open_outcome': open_outcome,
        'avg_fill_price_open': avg_fill_price_open,
        'last_fill_price_open': last_fill_price_open,
        'qty_placed_open': qty_placed_open,
        'qty_executed_open': qty_executed_open,
        'order_creation_dt': order_created_datetime,
        'order_transaction_dt': order_transaction_datetime,
        'trade_close_outcome': None,
        'closing_order_id': None,
        'avg_fill_price_open': None,
        'last_fill_price_open': None,
        'qty_placed_open': None,
        'qty_executed_open': None,
        'PITM_Balance': bp_balance,
        'order_status': order_status
    }
    position_item ={
        'position_id': position_id,
        'trade_type': Trade_Type,
        'order_ids': order_id,
        'datetimestamp': datetime_stamp,
        'transaction_ids': transaction_id,
        'trading_strategy': trading_strategy,
        'option_names': option_name,
        'two_week_contract_expiry': contract_expiry,
        'closing_order_ids': None,
        'position_order_status': order_status
    }

    print(position_item)
    response = transactions_table.put_item(
            Item=transaction_item
        )
    print(item_2)
    response = orders_table.put_item(
            Item=order_item
        )   
    print(item_3)
    response = positions_table.put_item(
            Item=position_item
        )

    return response