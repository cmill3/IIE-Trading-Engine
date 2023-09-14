import pandas as pd
import numpy
import requests
import boto3
from datetime import datetime

def build_spread(chain_df, spread_length, cp, current_price):
    contract_list = []
    if cp == "call":
        chain_df = chain_df.loc[chain_df['strike_price'] > current_price]
        chain_df.sort_values('strike_price',ascending=True,inplace=True)
        chain_df = chain_df.head(3)
    if cp == "put":
        chain_df = chain_df.loc[chain_df['strike_price'] < current_price]
        chain_df.sort_values('strike_price',ascending=False,inplace=True)
        chain_df = chain_df.head(3)
    for index, row in chain_df.iterrows():
        temp_object = {
            "contract_ticker": row['ticker'],
            "strike": row['strike_price'],
            "volume": row['volume'],
            "last_price": row['last_price'],
            "quantity": 1
        }
        contract_list.append(temp_object)
    return contract_list

def sector_data():
    s3 = boto3.client('s3')
    bucket_name = 'icarus-trading-data'
    object_key = 'sector_data/sector_data.csv'
    dataset = s3.get_object(Bucket = bucket_name, Key = object_key)
    sector_list = pd.read_csv(dataset.get("Body"))
    sector_list.dropna(inplace = True)
    index_list = ["SPY","IVV","VOO","VTI","QQQ","VEA","IEFA","VTV","BND","AGG","VUG","VWO","IEMG","IWF","VIG","IJH","IJR","GLD",
              "VGT","VXUS","VO","IWM","BNDX","EFA","IWD","VYM","SCHD","XLK","ITOT","VB","VCIT","XLV","TLT","BSV","VCSH",
              "LQD","XLE","VEU","RSP", "TQQQ","SQQQ","SPXS","SPXL","SOXL","SOXS"]
    return sector_list, index_list

def match_sector(symbol):
    sector_list,index_list = sector_data()
    try:
        match = sector_list[sector_list['Symbol'] == symbol]
        office = match['Sector'].values
        sector = office[0]
        pair = {
        'Symbol':symbol,
        'Sector': sector
            }
    except:
        if symbol in index_list == True:
            sector = 'Index Fund'
        else:
            sector = 'Failed'
            print(symbol)
        pair = {
        'Symbol':symbol,
        'Sector': sector
        }

    return sector
