import pandas as pd
import datetime 

def build_spread(chain_df, spread_length):
    contract_list = []
    chain_df = chain_df.loc[chain_df['inTheMoney'] == False].reset_index(drop=True)
    chain_df = chain_df.iloc[:spread_length]
    for index, row in chain_df.iterrows():
        temp_object = {
            "contractSymbol": row['contractSymbol'],
            "strike": row['strike'],
            "lastPrice": row['lastPrice'],
            "bid": row['bid'],
            "ask": row['ask'],
            "quantity": 1
        }
        contract_list.append(temp_object)
    
    return contract_list