import pandas as pd
import datetime 

def build_spread(chain_df, spread_length, cp, current_price):
    contract_list = []
    print(current_price)
    # chain_df = chain_df.loc[chain_df['inTheMoney'] == False].reset_index(drop=True)
    # chain_df = chain_df.loc[chain_df['strike_price'] % 1 == 0.0]
    # chain_df = chain_df.loc[chain_df['strike_price'].str.contains('.50') == False]
    if cp == "call":
        chain_df = chain_df.loc[chain_df['strike_price'] > current_price]
        chain_df.sort_values('strike_price',ascending=True,inplace=True)
        chain_df = chain_df.head(3)
    if cp == "put":
        chain_df = chain_df.loc[chain_df['strike_price'] < current_price]
        chain_df.sort_values('strike_price',ascending=False,inplace=True)
        chain_df = chain_df.head(3)
    # if len(chain_df) < spread_length:
    #     return contract_list
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