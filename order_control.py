import boto3
import pandas as pd
import os
from helpers import helper as helper
from helpers import tradier as te 
from helpers import dynamo_helper as db
import logging
from datetime import datetime

bucket = os.getenv("TRADING_BUCKET")
env = os.getenv("ENV")
orders_table = os.getenv("ORDERS_TABLE")

ddb = boto3.resource('dynamodb','us-east-1')
s3 = boto3.client("s3")

logger = logging.getLogger()


def run_order_control(event, context):
    date_prefix = helper.calculate_date_prefix()
    base_url, account_id, access_token = te.get_tradier_credentials(env=env)
    dynamo_orders_df = db.get_all_orders_from_dynamo(orders_table)
    tradier_orders = te.get_account_positions(base_url, account_id, access_token)

    dynamo_orders_df['qty_executed_open'] = dynamo_orders_df['qty_executed_open'].astype(float)
    ddb_symbol_count = dynamo_orders_df.groupby('option_symbol')['qty_executed_open'].sum().reset_index()    

    tradier_df = pd.DataFrame.from_dict(tradier_orders)
    tradier_df = tradier_df[['symbol','quantity']]
    tradier_df.rename(columns={'symbol':'option_symbol'}, inplace=True)
    tradier_df['quantity'] = tradier_df['quantity'].astype(int)
    
    mismatched_symbols = compare_dataframes(tradier_df, ddb_symbol_count)
    if len(mismatched_symbols) > 0:
        logger.error(f"Mismatched symbols: {mismatched_symbols}")
        if (env == "PROD_VAL") or (env == "PROD"):
            raise ValueError(f"Mismatched symbols: {mismatched_symbols}")
        
    exposure_totalling()
    

def compare_dataframes(tradier_df, ddb_symbol_count):
    mismatched_symbols = {}
    
    for index, row in tradier_df.iterrows():
        symbol = row['option_symbol']
        quantity_tradier = row['quantity']
        
        if symbol in ddb_symbol_count['option_symbol'].values:
            quantity_ddb = ddb_symbol_count.loc[ddb_symbol_count['option_symbol'] == symbol, 'qty_executed_open'].values[0]
            
            if quantity_tradier != quantity_ddb:
                mismatched_symbols[symbol] = quantity_tradier - quantity_ddb
        else:
            mismatched_symbols[symbol] = quantity_tradier
    for symbol in mismatched_symbols:
        logger.info(f"Symbol: {symbol}, Quantity Difference: {mismatched_symbols[symbol]}")
        logger.info(f"Tradier Quantity: {tradier_df.loc[tradier_df['option_symbol'] == symbol, 'quantity'].values[0]}")
        try:
            logger.info(f"DynamoDB Quantity: {ddb_symbol_count.loc[ddb_symbol_count['option_symbol'] == symbol, 'qty_executed_open'].values[0]}")
        except:
            logger.info(f"DynamoDB Quantity: 0")
            continue
    return mismatched_symbols

def exposure_totalling():
    base_url, account_id, access_token = te.get_tradier_credentials(env)
    position_list = te.get_account_positions(base_url, account_id, access_token)

    # total open trades & values in df
    df = pd.DataFrame.from_dict(position_list)
    df['underlying_symbol'] = df['symbol'].apply(lambda symbol: symbol[:-15])
    agg_functions = {'cost_basis': ['sum', 'mean'], 'quantity': 'sum'}
    df_new = df.groupby(df['underlying_symbol']).aggregate(agg_functions)

    # export df as csv --> AWS S3
    date = datetime.now()
    s3.put_object(Bucket='inv-alerts-trading-data', Key=f'positions_exposure/{env}/{date.year}/{date.month}/{date.day}/{date.hour}/{date.minute}.csv', Body=df_new.to_csv(index=False))
    return "Exposure Analysis Complete"

if __name__ == "__main__":
    run_order_control(None, None)