import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
import os

prefix = 'enriched_closed_orders_data/inv/'
date_str = datetime.now().strftime("%Y/%m/%d")

def lambda_handler(event, context):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # List all CSV files in the S3 bucket with the specified prefix
    response = s3.list_objects_v2(Bucket="inv-alerts-trading-data", Prefix=f"{prefix}{date_str}")
    print(response)
    all_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]

    # Read each CSV file and concatenate into a single DataFrame
    list_of_dataframes = []
    for file_key in all_files:
        csv_obj = s3.get_object(Bucket="inv-alerts-trading-data", Key=file_key)
        csv_string = csv_obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        list_of_dataframes.append(df)
    
    combined_df = pd.concat(list_of_dataframes, ignore_index=True)
    stats_df = calculate_statistics(combined_df)

    # Convert the aggregated DataFrame to a dictionary for the Lambda response
    # stats_dict = stats_df.to_dict()

    return {
        'statusCode': 200,
        'body': stats_dict
    }


def calculate_statistics(df):
    stats_df = df[['avg_fill_price_open','avg_fill_price_close','option_symbol','open_option_symbol','order_transaction_date','close_transaction_date','option_side','trading_strategy']]
    stats_df['absolute_return'] = stats_df['avg_fill_price_close'] - stats_df['avg_fill_price_open']
    stats_df['relative_return'] = stats_df['absolute_return'] / stats_df['avg_fill_price_open']
    stats_df['close_transaction_date'] = pd.to_datetime(stats_df['close_transaction_date'])
    stats_df['order_transaction_date'] = pd.to_datetime(stats_df['order_transaction_date'])
    stats_df['days_to_close'] = (stats_df['close_transaction_date'] - stats_df['order_transaction_date']).dt.days
    # stats_df = combined_df.groupby('strategy').agg({
    #     'YOUR_COLUMN_1': ['mean', 'sum'],
    #     'YOUR_COLUMN_2': ['max', 'min']
    #     # Add more columns and aggregation functions as needed
    # })
    print(stats_df.head(5))
    print(stats_df.columns)

if __name__ == '__main__':
    lambda_handler(None, None)