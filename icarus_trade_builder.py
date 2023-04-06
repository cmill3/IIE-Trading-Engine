from ctypes.wintypes import PUSHORT
from operator import index
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from yahooquery import Ticker

#Non-Explanatory Variables Explained Below:
#CP = Call/Put (used to represent the Call/Put Trend Value)
#Sym = Symbol (used to repesent the symbol of the value that we are analyzing)

df = pd.read_csv('/Users/ogdiz/Projects/APE-Executor/Trade_Builder/Scout_Data_v0.csv', usecols=['Sym','Trend','Class','Target Price'])
df.dropna(inplace = True)
df = df.reset_index()

Symbol_Value = df['Sym']
Trend_Value = df['Trend']
Expiry_Date = []

#This is for a list to df to append to the starting dataframe (CSV that we pulled in)
sym_list = []
cp_list = []
date_list_1wk = []
date_list_2wk = []
option_name_list = []
strike_price_list = []


d = datetime.now().date() # Monday
t_1wk = timedelta((11 - d.weekday()) % 7)
t_2wk = timedelta((11 - d.weekday()) % 14)

def Symbol(x):
    sym_list.append(Symbol_Value[i])

def Trend(x):
    if x == -1:
        cp_list.append("PUT")
    elif x == 1:
        cp_list.append("CALL")
    else:
        cp_list.append("Trend Processing Error: Review TB Data and Code")
        print("Trend Processing Error: Review TB Data and Code")

def options_chain_data_processing(x):
    Tick = Ticker(str(x))
    df_ocraw = Tick.option_chain #pulling the data into a data frame (optionchainraw = ocraw)
    df_ocraw.head(100)
    print(df_ocraw)
    Expiry_Date_OC = ((d + t_2wk).strftime('%Y-%m-%d'))
    if Trend_Value[i] == -1:
        contract_type = "puts"
    elif Trend_Value[i] == 1:
        contract_type = "calls"
    else:
        print("OOTM STRIKE PRICE PROCESSING ERROR")
    df_optionchain_2wk = df_ocraw.loc[x, Expiry_Date_OC, contract_type]
    if contract_type == "calls":
        true_list_length = len(df_optionchain_2wk[(df_optionchain_2wk['inTheMoney'] == True)])
        option_name = df_optionchain_2wk.contractSymbol[true_list_length + 1]
        strike_price = df_optionchain_2wk.strike[true_list_length + 1]
        option_name_list.append(option_name)
        strike_price_list.append(strike_price)
    elif contract_type == "puts":
        df_optionchain_2wk_put = df_optionchain_2wk[df_optionchain_2wk['inTheMoney'] == False] #had to use different variable here (than the if for calls above) to be sure that I am indexing to the right value for PUTS - needed to index to the last variable that is OOTM b/c the listing goes PUTS OOTM -> ITM(puts and then calls) --> Calls OOTM
        option_name = df_optionchain_2wk_put.contractSymbol[-1]
        strike_price = df_optionchain_2wk_put.strike[-1]
        option_name_list.append(option_name)
        strike_price_list.append(strike_price)
    else:
        print("OOTM OPTION NAME & STRIKE PRICE PROCESSING ERROR")

def Date_1wk():
    Expiry_Date = (d + t_1wk).strftime('%Y-%m-%d')
    date_list_1wk.append(Expiry_Date)

def Date_2wk():
    Expiry_Date = (d + t_2wk).strftime('%Y-%m-%d')
    date_list_2wk.append(Expiry_Date)

for i, row in df.iterrows():
    Symbol(Symbol_Value.iloc[i])
    Trend(Trend_Value.iloc[i])
    Date_1wk()
    Date_2wk()
    options_chain_data_processing(Symbol_Value.iloc[i])

df_cp = pd.DataFrame(cp_list, columns = ['Call/Put'])
df_date_1wk = pd.DataFrame(date_list_1wk, columns = ['Contract Expiry @ 1 Week'])
df_date_2wk = pd.DataFrame(date_list_2wk, columns = ['Contract Expiry @ 2 Weeks'])
df_option_name = pd.DataFrame(option_name_list, columns = ['Option Name'])
df_strike_price = pd.DataFrame(strike_price_list, columns = ['Strike Price'])

df_final = pd.concat([df, df_cp, df_strike_price, df_option_name, df_date_1wk, df_date_2wk], axis=1)

print(df_final)