import pandas as pd
import boto3
import os

df = pd.read_csv(f'/Users/ogdiz/Downloads/results.csv')
table_name = 'icarus-orders-table-dev'

print(df.head())

AWS_ACCESS_KEY_ID = os.getenv("AWS_API_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

dynamo = boto3.client('dynamodb', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
ddb = boto3.resource('dynamodb', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def write_csv(table_name, row):
    table = ddb.Table(table_name)
    print(row)
    order_item = {
        'order_id': str(row[0]),
        'closing_order_id': str(row[5]),
        'trading_mode': str(row[17]),
        'execution_strategy': str(row[6]),
        'underlying_symbol': str(row[20]),
        'position_id': str(row[14]),
        'trading_strategy': str(row[18]),
        'option_symbol': str(row[11]),
        'open_option_symbol': str(row[9]),
        'option_side': str(row[10]),
        # 'two_week_contract_expiry': str(row[19]),
        'avg_fill_price_open': str(row[2]),
        'last_fill_price_open': str(row[8]),
        'qty_executed_open': str(row[16]),
        'order_creation_date': str(row[12]),
        'order_row_date': str(row[13]),
        'close_creation_date': str(row[3]),
        'close_df_date': str(row[4]),
        'avg_fill_price_close': str(row[1]),
        'last_fill_price_close': str(row[7]),
        'qty_executed_close': str(row[15]),
        'user': str(row[21])
    }
    response = table.put_item(
        Item=order_item
        )
    return response
    
    
    
for i, row in df.iterrows():
    print(i)
    response = write_csv(table_name, row)