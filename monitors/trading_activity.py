import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
import os

bucket_name = os.getenv('TRADING_DATA_BUCKET')
prefix = 'invalerts_potential_trades/'
date_str = datetime.now().strftime("%Y/%m/%d")

def lambda_handler(event, context):
    # Initialize the S3 client
    s3 = boto3.client('s3')


    # List all CSV files in the S3 bucket with the specified prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}/{date_str}")
    all_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]

    # Read each CSV file and concatenate into a single DataFrame
    list_of_dataframes = []
    for file_key in all_files:
        csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_string = csv_obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        list_of_dataframes.append(df)
    
    combined_df = pd.concat(list_of_dataframes, ignore_index=True)
    stats_df = calculate_statistics(combined_df)

    # Convert the aggregated DataFrame to a dictionary for the Lambda response
    stats_dict = stats_df.to_dict()

    return {
        'statusCode': 200,
        'body': stats_dict
    }


def calculate_statistics():
    # Generate statistics based on groupby aggregations of the 'strategy' column
    # Adjust the aggregation functions as needed
    stats_df = combined_df.groupby('strategy').agg({
        'YOUR_COLUMN_1': ['mean', 'sum'],
        'YOUR_COLUMN_2': ['max', 'min']
        # Add more columns and aggregation functions as needed
    })