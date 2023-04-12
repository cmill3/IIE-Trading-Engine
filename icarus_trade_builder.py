# Purpose: This script is used to build trades for the Icarus Trading Bot
import pandas as pd
from datetime import datetime, timedelta
from yahooquery import Ticker
from helpers import strategy_helper
import boto3
import os

s3 = boto3.client('s3')
d = datetime.now().date() # Monday

trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
model_results_bucket = os.getenv('MODEL_RESULTS_BUCKET')

def build_trade(event, context):
    df, key = pull_data()
    results_df = process_data(df)
    print(results_df)
    # csv_buffer = results_df.to_csv("/Users/charlesmiller/Code/PycharmProjects/FFACAP/Icarus/icarus_production/icarus-trading-engine/test.csv")
    csv = results_df.to_csv()
    key = key.replace("yqalerts_full_results", "yqalerts_potential_trades")
    s3.put_object(Body=csv, Bucket=trading_data_bucket, Key=key)
    return {
        'statusCode': 200
    }


#Non-Explanatory Variables Explained Below:
#CP = Call/Put (used to represent the Call/Put Trend Value)
#Sym = Symbol (used to repesent the symbol of the value that we are analyzing)

def pull_data():
    keys = s3.list_objects(Bucket=model_results_bucket,Prefix="yqalerts_full_results/")["Contents"]
    key = keys[-1]['Key']
    print(key)
    dataset = s3.get_object(Bucket=model_results_bucket, Key=key)
    df = pd.read_csv(dataset.get("Body"))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    return df, key

def process_data(df):
    df['Call/Put'] = df['strategy'].apply(lambda strategy: infer_CP(strategy))
    df['expiry_1wk'] = Date_1wk()
    df['expiry_2wk'] = Date_2wk()
    df['trade_details'] = df.apply(lambda row: build_trade_structure(row), axis=1)

    return df


def infer_CP(strategy):
    if strategy == "day_gainers" or strategy == "most_actives":
        return "calls"
    elif strategy == "day_losers" or strategy == "maP":
        return "puts"
    

def build_trade_structure(row):
    try:
        Tick = Ticker(str(row['symbol']))
        df_ocraw = Tick.option_chain #pulling the data into a data frame (optionchainraw = ocraw)
        df_optionchain_2wk = df_ocraw.loc[row['symbol'], row['expiry_2wk'], row['Call/Put']]
        if row['strategy'] == 'day_losers':
            if len(df_optionchain_2wk) < 12:
                contracts = []
                return contracts
        elif row['strategy'] == 'day_gainers' or row['strategy'] == 'most_actives' or row['strategy'] == 'maP':
            if len(df_optionchain_2wk) < 20:
                contracts = []
                return contracts
        contracts = strategy_helper.build_spread(df_optionchain_2wk, spread_length=3)
    except Exception as e:
        contracts = None
        print(e)
    return contracts

def Date_1wk():
    t_1wk = timedelta((11 - d.weekday()) % 7)
    Expiry_Date = (d + t_1wk).strftime('%Y-%m-%d')
    return Expiry_Date 

def Date_2wk():
    t_2wk = timedelta((11 - d.weekday()) % 14)
    Expiry_Date = (d + t_2wk).strftime('%Y-%m-%d')
    return Expiry_Date 

if __name__ == "__main__":
    build_trade(None, None)