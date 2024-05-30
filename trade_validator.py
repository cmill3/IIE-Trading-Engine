import pandas as pd
from helpers.validator_helpers import *
import os
import boto3
import ast
import time
from datetime import datetime
import pytz
import logging

s3 = boto3.client('s3')
ddb = boto3.client('dynamodb')

env = os.environ.get('ENV')
bucket = os.environ.get('TRADING_BUCKET')
strategies = os.environ.get('ACTIVE_STRATEGIES').split(',')
ddb_table = os.environ.get('CLOSED_ORDERS_TABLE')

prefix = f'closed_orders/{env}/'
logger = logging.getLogger()

def run_process(event, context):
    date = datetime.now() #To be uncommented when live
    logger.info(f"Running trade validation process for {date}")
    # # date_str = '2024-05-21 14:15:00'
    # date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    date_hour_prefix = str(date.strftime('%Y/%m/%d/%H'))
    dfs = []
    for strategy in strategies:
        path = f'{prefix}{strategy}/{date_hour_prefix}' 
        df = pull_closed_trades_s3(path, bucket)
        dfs.append(df)
    full_df = pd.concat(dfs)
    real_dfs = []
    simulated_dfs = []
    for i, row in full_df.iterrows():
        # print(row)
        df, strategy, symbol, hour, date, index, sim_df = pull_potential_trades(row['position_id'])
        item, data = create_real_dataset(row, strategy, symbol)
        positions_list = simulate_trades_invalerts(sim_df, config)
        positions_df = pd.DataFrame(positions_list)
        data_df = pd.DataFrame(data, index = [0])
        real_dfs.append(data_df)
        simulated_dfs.append(positions_df)
        # if i == 5:
        #     break

    truevalues_df = pd.concat(real_dfs)
    simulated_df = pd.concat(simulated_dfs)

    sim_df = explode_sim_data(simulated_df)
    # print(sim_df, sim_df.columns)
    print(truevalues_df, truevalues_df.columns)
    real_values_df = clean_real_data(truevalues_df)
    raw_df, viz_df = prod_sim_match(sim_df, real_values_df)
    s3.put_object(Bucket=bucket, Key=f'trade_validations/{date_hour_prefix}/raw.csv', Body=raw_df.to_csv(index=False)) 
    s3.put_object(Bucket=bucket, Key=f'trade_validations/{date_hour_prefix}/viz.csv', Body=viz_df.to_csv(index=False))
    logger.info(f"Trade validation process complete for {date}")
    return "Process Complete"
    


def pull_closed_trades_s3(prefix, bucket):
    dfs = []
    keys = s3.list_objects(Bucket=bucket, Prefix=prefix)['Contents']
    for object in keys:
        key = object['Key']
        dataset = s3.get_object(Bucket=bucket,Key=key)
        df = pd.read_csv(dataset.get("Body"))
        if len(df) > 0:
            dfs.append(df)
    full_df = pd.concat(dfs)
    full_df.reset_index(drop=True)
    return full_df

def pull_potential_trades(position_id):

    year = position_id.split('-')[2]
    month = position_id.split('-')[3]
    dayhour = position_id.split('-')[4]
    day = dayhour.split('T')[0]
    hour_str = dayhour.split('T')[1]
    minute_str = position_id.split('-')[5]
    open_dt = year + '-' + month + '-' + day + 'T' + hour_str + '-' + minute_str
    trading_date = year + '-' + month + '-' + day
    # query_date = month + '/' + day + '/' + year[-2:]
    open_datetime = datetime.strptime(open_dt, '%Y-%m-%dT%H-%M')
    open_dt_est = open_datetime.astimezone(pytz.timezone('US/Eastern'))
    tz = pytz.timezone('US/Eastern')
    open_dt_est =pytz.utc.localize(open_datetime, is_dst=None).astimezone(tz)
    hour = open_dt_est.hour
    strategy = position_id.split('-')[1]
    symbol = position_id.split('-')[0]
    if len(strategy) > 5:
        new_strategy = strategy[:5]+ '_' + strategy[-2:]
    else:
        new_strategy = strategy
    key = f'invalerts_potential_trades/PROD_VAL/{new_strategy}/{year}/{month}/{day}/{hour}.csv'

    try:
        data = s3.get_object(Bucket=bucket, Key= f"{key}")
    except Exception as e:
        print(f"Error reading key {key} from bucket {bucket}: {e}")
    
    df = pd.read_csv(data.get("Body"))
    index = df.loc[df['symbol'] == symbol].index
    row = df.loc[index].to_dict('records')[0]
    contracts_to_trade = ast.literal_eval(row['trade_details1wk'])
    if row['Call/Put'] == 'call':
        side = 'C'
    elif row['Call/Put'] == 'put':
        side = 'P'
    sim_list = []
    
    for i in contracts_to_trade:

        sim_dict = {
            'date': trading_date,
            'symbol': symbol,
            'strategy': new_strategy,
            'hour': hour_str[:2],
            'contract_ticker': i['contract_ticker'],
            'quantity': i['quantity'],
            'o': i['last_price'],
            'volume': i['volume'],
            'spread_position': i['spread_position'],
            'side': side,
            'target_pct': row['target_pct'],
            'classifier_prediction': row['classifier_prediction']
            }
        
        sim_list.append(sim_dict)

    sim_df = pd.DataFrame(sim_list)

    return df, new_strategy, symbol, hour_str, trading_date, index, sim_df


def create_real_dataset(row, strategy, symbol):
    print(row['open_order_id'])
    order_dict = {'order_id': {'S': str(row['open_order_id'])}}
    dynamo_query = ddb.get_item(TableName = ddb_table, Key = order_dict)
    item = dynamo_query.get('Item', None)
    open_dt = datetime.strptime(item['order_transaction_date']['S'],"%Y-%m-%dT%H:%M:%S.%fZ")
    trading_date = datetime.strptime(item['order_transaction_date']['S'][:10],"%Y-%m-%d")
    close_dt = datetime.strptime(item['close_transaction_date']['S'],"%Y-%m-%dT%H:%M:%S.%fZ")
    avg_open_price = item['average_fill_price_open']['S']
    avg_close_price = item['average_fill_price_close']['S']
    qty = item['qty_executed_open']['S']
    prod_net = ((float(avg_close_price) - float(avg_open_price)) * float(qty)) * 100
    data = {
        'symbol': symbol,
        'option_symbol': row['option_symbol'],
        'open_order_id': row['open_order_id'],
        'open_dt': open_dt,
        'close_dt': close_dt,
        'avg_open_price': avg_open_price,
        'avg_close_price': avg_close_price,
        'quantity': qty,
        'prod_net': prod_net,
        'closing_order_id': row['closing_order_id'],
        'position_id': row['position_id'],
        'strategy': strategy,
        'trading_date': trading_date
    }

    return item, data

def explode_sim_data(simulated_df):
    sim_df = pd.concat([simulated_df.drop(['transactions'], axis=1), simulated_df['transactions'].apply(pd.Series)], axis=1)
    sim_df.columns = ['position_id', 'open_datetime', 'transactions']
    simulation_df = pd.concat([sim_df.drop(['transactions'], axis=1), sim_df['transactions'].apply(pd.Series)], axis=1)
    sim_df = pd.concat([simulation_df.drop(['buy_info'], axis=1), simulation_df['buy_info'].apply(pd.Series)], axis=1)
    sim_df['option_symbol'] = sim_df['option_symbol'].apply(lambda x: x[2:])
    sim_df = sim_df.loc[:,~sim_df.columns.duplicated()]
    sim_df = sim_df.add_prefix('sim_')
    sim_df = sim_df.rename({'sim_position_id': 'position_id', 'sim_option_symbol' : 'option_symbol'}, axis=1)
    return sim_df

def clean_real_data(real_df):
    real_df['position_id'] = real_df['position_id'].apply(lambda x: x[:-6] + '-' + x[-5:-3])
    print(real_df)
    return real_df

def prod_sim_match(simulated_df, real_df):
    raw_df = pd.merge(simulated_df, real_df, on=['option_symbol'], how='inner')
    raw_df['profit_diff_pct'] = (raw_df['prod_net'] - raw_df['sim_total_gain'])/raw_df['prod_net']
    viz_df = raw_df[['option_symbol', 'prod_net', 'sim_total_gain', 'profit_diff_pct','sim_open_trade_dt','open_dt','sim_close_trade_dt','close_dt','position_id_x']]
    return raw_df, viz_df




if __name__ == "__main__":
    # row = {
    #     'open_order_id': '11371401',
    #     'closing_order_id': '11379317',
    #     'option_symbol': 'NKE240503C00094000',
    #     'position_id': 'NKE-CDBFC1D-2024-04-30T19-02'
    # }

    config = {
    "put_pct": 1, 
    "spread_search": "1:4",
    "aa": 0,
    "risk_unit": .05,
    "model": "CDVOLVARVC",
    "vc_level":"100+300+500+500",
    "capital_distributions": ".33,.33,33",
    "portfolio_cash": 20000,
    "scaling": "dynamicscale",
    "volatility_threshold": 0.4,
    "model_type": "cls",
    "user": "cm3",
    "threeD_vol": "return_vol_5D",
    "oneD_vol": "return_vol_5D",
    "dataset": "CDVOLBF3-6PE",
    "spread_length": 3,
    "reserve_cash": 5000
        }
    

    run_process(None, None)



    # path = 'closed_orders/PROD_VAL/CDBFC_1D'
    # bucket = 'inv-alerts-trading-data'
    # date_hour_prefix = '2024/05/01/13'
    # full_df = pull_opened_data_s3(path, bucket, date_hour_prefix)
    # print(full_df)
    

    # df, strategy, symbol, hour, date, index, sim_df = pull_potential_trades(row['position_id'])

    # item, data = create_real_dataset(row, strategy, symbol)


    # positions_list = simulate_trades_invalerts(sim_df, config)

    # for i in positions_list:
    #     print(i)
    # print(data)





