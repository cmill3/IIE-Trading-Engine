# import helpers.dynamo_helper as db
# import helpers.tradier as trade
# import helpers.helper as helper
# import boto3
# import pandas as pd
# import numpy as np
# import os
# from datetime import datetime
# import re
# import logging

# s3 = boto3.client('s3')
# date = datetime.now().strftime("%Y-%m-%d")
# logger = logging.getLogger()

# trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
# env = os.getenv("ENV")

# def run_reconciliation(event, context):
#     base_url, account_id, access_token = trade.get_tradier_credentials(env)
#     tradier_trades = trade.get_account_orders(base_url, account_id, access_token)
#     tradier_df = pd.DataFrame.from_dict(tradier_trades)
#     dynamo_trades = db.get_all_orders_from_dynamo(table="icarus-orders-table-inv")
#     still_pending  = compare_order_dfs(tradier_df, dynamo_trades,base_url, account_id, access_token)
#     # exposure_totalling()
#     return "No pending orders"


# def compare_order_dfs(tradier_trades, dynamo_trades,base_url, account_id, access_token):
#     tradier_trades = tradier_trades[tradier_trades['status'] == 'filled']
#     tradier_trades['underlying_symbol'] = tradier_trades['symbol'].apply(lambda symbol: symbol[:-15])
#     opened_trades = tradier_trades[tradier_trades['side'] == 'buy_to_open']
#     closed_trades = tradier_trades[tradier_trades['side'] == 'sell_to_close']
#     merged = opened_trades.merge(closed_trades, on='tag', how='outer', indicator=True)
#     still_open = merged[merged['_merge'] == 'left_only']
#     dynamo_trades['date_str'] = dynamo_trades['order_creation_date'].apply(lambda date: date.split('T')[0])
#     todays_dynamo = dynamo_trades[dynamo_trades['date_str'] == date]

#     tradier_set = set(still_open['id_x'].values.astype(int))
#     dynamo_set = set(todays_dynamo['order_id'].values.astype(int))
#     not_in_tradier = dynamo_set - tradier_set
#     not_in_dynamo = tradier_set - dynamo_set
#     print(f"Trades in Dynamo but not in Tradier: {not_in_tradier}")
#     print(f"Trades in Tradier but not in Dynamo: {not_in_dynamo}")
#     print(dynamo_trades['order_id'].values)
    
#     for order_id in not_in_dynamo:
#         order_info_obj = trade.get_order_info(base_url, account_id, access_token, order_id)
#         print(order_info_obj)
#         if order_info_obj['status'] == "filled":
#             dt = datetime.strptime(order_info_obj['created_date'],"%Y-%m-%dT%H:%M:%S.%fZ")
#             underlying_symbol = order_info_obj['position_id'].split("-")[0]
#             underlying_purchase_price = trade.call_polygon_price_reconciliation(underlying_symbol,dt)
#             db.create_new_dynamo_record_order_reconciliation(order_info_obj, order_info_obj['position_id'], underlying_symbol,order_id, underlying_purchase_price, env, table="icarus-orders-table-inv")
        


# def exposure_totalling():
#     # base_url = "https://sandbox.tradier.com/v1/"
#     # account_id = "VA72174659"
#     # access_token = "ld0Mx4KbsOBYwmJApdowZdFcIxO7"
#     #pull all open contract symbols & contract values
#     base_url, account_id, access_token = trade.get_tradier_credentials(env)
#     position_list = trade.get_account_positions(base_url, account_id, access_token)
#     # total open trades & values in df
#     df = pd.DataFrame.from_dict(position_list)
#     df['underlying_symbol'] = df['symbol'].apply(lambda symbol: helper.pull_symbol(symbol))
#     agg_functions = {'cost_basis': ['sum', np.mean], 'quantity': 'sum'}
#     df_new = df.groupby(df['underlying_symbol']).aggregate(agg_functions)
#     # export df as csv --> AWS S3
#     year, month, day, hour = helper.date_and_time()
#     bucket = trading_data_bucket
#     df_csv = df_new.to_csv()
#     s3_resource = boto3.resource('s3')
#     s3_resource.Object(bucket, f'positions_exposure/{year}/{month}/{day}/{hour}.csv').put(Body=df_csv.getvalue())
#     return "Exposure Analysis Complete"

# # def run_reconciliation(event, context):
# #     base_url, account_id, access_token = trade.get_tradier_credentials(env)
# #     trades_df = pull_pending_trades()
# #     formatted_df = format_pending_df(trades_df)
# #     still_pending  = process_dynamo_orders(formatted_df, base_url, account_id, access_token, env)
# #     # if len(trades_df) > 0:
# #     #     still_pending  = process_dynamo_orders(trades_df, base_url, account_id, access_token)
# #     #     pending_df = pd.DataFrame.from_dict(still_pending)
# #     # if len(pending_df) > 0:
# #     #     pending_csv = pending_df.to_csv()
# #     #     response = s3.put_object(Bucket=trading_data_bucket, Key=f"pending_orders_enriched/{date}.csv", Body=pending_csv)
# #     #     return response
# #     return "No pending orders"



# # def pull_pending_trades():
# #     keys = s3.list_objects(Bucket=trading_data_bucket,Prefix='pending_orders/2023')["Contents"]
# #     # pending_keys = keys[-2:-1]['Key']
# #     key = keys[-1]['Key']
# #     dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
# #     pending_df = pd.read_csv(dataset.get("Body"))
    
# #     # dfs = []
# #     # for key in pending_keys:
# #     #     dataset = s3.get_object(Bucket=trading_data_bucket, Key=key)
# #     #     df = pd.read_csv(dataset.get("Body"))
# #     #     dfs.append(df)
# #     # pending_df = pd.concat(dfs)
# #     return pending_df

# # def process_dynamo_orders(trades_df, base_url, account_id, access_token, env):
# #     for index, row in trades_df.iterrows():
# #         unfilled_order = db.process_opened_orders(row, row['position_id'],base_url, account_id, access_token, env)
# #     # unfulfilled_orders = db.process_opened_ordersv2(trades_df, base_url, account_id, access_token, env)
# #     return unfilled_order

# # def format_pending_df(df):
# #     columns = df['Unnamed: 0'].values
# #     indexes = df.columns.values[1:]

# #     unpacked_data = []
# #     for index in indexes:
# #         data = df[index].values
# #         unpacked_data.append(data)
    
# #     formatted_df = pd.DataFrame(unpacked_data,indexes,columns)
# #     return formatted_df

# if __name__ == "__main__":
#     run_reconciliation(None, None)
