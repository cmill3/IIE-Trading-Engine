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

    ddb_symbol_count = dynamo_orders_df.groupby('option_symbol').size().reset_index(name='count')
    tradier_df = pd.DataFrame.from_dict(tradier_orders)
    print(tradier_df)
    print(ddb_symbol_count)
    tradier_df = tradier_df[['symbol','quantity']]
    tradier_df.rename(columns={'symbol':'option_symbol'}, inplace=True)
    tradier_df['quantity'] = tradier_df['quantity'].astype(int)
    ddb_symbol_count['count'] = ddb_symbol_count['count'].astype(int)
    
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
            quantity_ddb = ddb_symbol_count.loc[ddb_symbol_count['option_symbol'] == symbol, 'count'].values[0]
            
            if quantity_tradier != quantity_ddb:
                mismatched_symbols[symbol] = quantity_tradier - quantity_ddb
        else:
            mismatched_symbols[symbol] = quantity_tradier
    
    return mismatched_symbols

def exposure_totalling():
    base_url, account_id, access_token = db.get_tradier_credentials(env)
    position_list = db.get_account_positions(base_url, account_id, access_token)

    # total open trades & values in df
    df = pd.DataFrame.from_dict(position_list)
    df['underlying_symbol'] = df['symbol'].apply(lambda symbol: helper.pull_symbol(symbol))
    agg_functions = {'cost_basis': ['sum', 'mean'], 'quantity': 'sum'}
    df_new = df.groupby(df['underlying_symbol']).aggregate(agg_functions)

    # export df as csv --> AWS S3
    year, month, day, hour = helper.date_and_time()
    df_csv = df_new.to_csv()
    s3_resource = boto3.resource('s3')
    s3_resource.Object("inv-alerts-trading-data", f'positions_exposure/{env}/{year}/{month}/{day}/{hour}.csv').put(Body=df_csv.getvalue())
    return "Exposure Analysis Complete"

if __name__ == "__main__":
    run_order_control(None, None)