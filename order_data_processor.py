from datetime import datetime, timedelta
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import boto3
import os
import logging
import helpers.trade_executor as te
import helpers.tradier as trade
import helpers.dynamo_helper as db
import pytz
import json
import pandas as pd
import time

trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
orders_table = os.getenv('TABLE')
close_table = os.getenv("CLOSE_TABLE")
env = os.getenv("ENV")
urllib3.disable_warnings(category=InsecureRequestWarning)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
est = pytz.timezone('US/Eastern')


s3 = boto3.client('s3')
logs = boto3.client('logs')
ddb = boto3.client('dynamodb')

def run_closed_trades_data_process(event,context):
    time.sleep(60)
    preprocess_order_count = ddb.describe_table(TableName=orders_table)['Table']['ItemCount']
    logger.info(f"preprocess_order_count: {preprocess_order_count}")
    lambda_signifier = event.get('lambda_signifier', 'default_value')
    if lambda_signifier == "default_value":
        lambda_signifier = retrieve_signifier()
    logger.info(f"LAMBDA SIGNIFIER: {lambda_signifier}")

    succesful_logs, unsuccesful_logs = pull_log_data("closed",lambda_signifier)
    create_dynamo_order_close(succesful_logs)
    postprocess_order_count = ddb.describe_table(TableName=orders_table)['Table']['ItemCount']
    logger.info(f"postprocess_order_count: {postprocess_order_count}")

    success_df = pd.DataFrame.from_dict(succesful_logs)
    failed_df = pd.DataFrame.from_dict(unsuccesful_logs)
    s3.put_object(Bucket=trading_data_bucket, Key=f"failed_close_orders_data/{datetime.now(est).strftime('%Y/%m/%d')}.csv", Body=failed_df.to_csv(index=False))
    s3.put_object(Bucket=trading_data_bucket, Key=f"successful_close_orders_data/{datetime.now(est).strftime('%Y/%m/%d')}.csv", Body=success_df.to_csv(index=False))

    # dynamo_record_diff = preprocess_order_count - postprocess_order_count
    # if dynamo_record_diff != len(succesful_logs):
    #     raise ValueError(f"Error: Dynamo record count: {dynamo_record_diff} does not match succesful logs count: {len(succesful_logs)}")
    
    return "Created new dynamo records for closed orders"

def retrieve_signifier():
    signifier = s3.get_object(Bucket=trading_data_bucket, Key="lambda_signifiers/recent_signifier_open_trades.txt")
    return signifier['Body'].read().decode('utf-8')

def run_opened_trades_data_process(event,context):
    time.sleep(60)
    lambda_signifier = datetime.now().strftime("%Y%m%d+%H")
    preprocess_order_count = ddb.describe_table(TableName=orders_table)['Table']['ItemCount']
    logger.info(f"preprocess_order_count: {preprocess_order_count}")
    succesful_logs, unsuccesful_logs = pull_log_data("opened",lambda_signifier)
    logger.info(f"LAMBDA SIGNIFIER: {lambda_signifier}")


    create_dynamo_order_open(succesful_logs)
    postprocess_order_count = ddb.describe_table(TableName=orders_table)['Table']['ItemCount']
    logger.info(f"postprocess_order_count: {postprocess_order_count}")


    success_df = pd.DataFrame.from_dict(succesful_logs)
    failed_df = pd.DataFrame.from_dict(unsuccesful_logs)
    s3.put_object(Bucket=trading_data_bucket, Key=f"failed_new_orders_data/{datetime.now(est).strftime('%Y/%m/%d')}.csv", Body=failed_df.to_csv(index=False))
    s3.put_object(Bucket=trading_data_bucket, Key=f"successful_new_orders_data/{datetime.now(est).strftime('%Y/%m/%d')}.csv", Body=success_df.to_csv(index=False))

    # dynamo_record_diff = postprocess_order_count - preprocess_order_count
    # if dynamo_record_diff != len(succesful_logs):
    #     raise ValueError(f"Error: Dynamo record count open: {dynamo_record_diff} does not match succesful logs count: {len(succesful_logs)}")

    return "Created new dynamo records for open orders"


def create_dynamo_order_open(log_messages):
    capital_spent = 0
    base_url, account_id, access_token = trade.get_tradier_credentials(env)
    for log in log_messages:
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, log['order_id'])
        capital_spent += float(order_info_obj['exec_quantity']) * float(order_info_obj['average_fill_price'])
        db.create_new_dynamo_record_order_logmessage(order_info_obj,log, env, orders_table)
        logger.info(f"Created new dynamo record for order_id: {log['order_id']} in {orders_table}")

    return "Created new dynamo records for open orders"

def create_dynamo_order_close(log_messages):
    base_url, account_id, access_token = trade.get_tradier_credentials(env)
    for log in log_messages:
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, log['order_id'])
        original_order = db.delete_order_record(log['order_id'], orders_table)
        create_response = db.create_new_dynamo_record_closed_order_logmessage(order_info_obj, original_order, log, env, close_table)
        logger.info(f"Created new dynamo record for order_id: {log['order_id']} {log['closing_order_id']} in {close_table}")

    return "Created new dynamo records for close orders"


def pull_log_data(process_type,lambda_signifier):
    log_messages = {}
    if env == "DEV":
        if process_type == "closed":
            log_group_name = '/aws/lambda/open-trades-portfolio-manager-dev'
        elif process_type == "opened":
            log_group_name = '/aws/lambda/new-trades-portfolio-manager-dev'
    elif env == "PROD_VAL":
        if process_type == "closed":
            log_group_name = '/aws/lambda/open-trades-portfolio-manager-prod-val'
        elif process_type == "opened":
            log_group_name = '/aws/lambda/new-trades-portfolio-manager-prod-val'

    minute_ms = 60000
    minutes_back = 20

    # Calculate the time range for the last 24 hours
    end_time = int((datetime.utcnow()).timestamp() * 1000)
    start_time = int((datetime.utcnow()).timestamp() * 1000) - (minutes_back * minute_ms)
    print(f"start_time: {start_time} end_time: {end_time}")

    # Create a paginator to fetch logs
    paginator = logs.get_paginator('filter_log_events')

    # Pagination options
    pagination_options = {
        'logGroupName': log_group_name,
        'startTime': start_time,
        'endTime': end_time,
        'limit': 500,  # Adjust based on how many logs you want to fetch per page
    }
    succesful_logs = []
    unsuccessful_logs = []
    # Fetch and process log events
    for page in paginator.paginate(**pagination_options):
        for event in page['events']:
            message = event['message']
            try:
                parts = message.split('\t')
                json_part = parts[-1]
                log_dict = json.loads(json_part)

                if log_dict['lambda_signifier'] != lambda_signifier:
                    continue
                if process_type == "closed":
                    if log_dict['log_type'] == "close_success":
                        if log_dict['closing_order_id'] is not None:
                            succesful_logs.append(log_dict)
                    elif log_dict['log_type'] == "close_error":
                        unsuccessful_logs.append(log_dict)
                elif process_type == "opened":
                    if log_dict['log_type'] == "open_success":
                        succesful_logs.append(log_dict)
                    elif log_dict['log_type'] == "open_error":
                        unsuccessful_logs.append(log_dict)
            except Exception as e:
                print(e)
                continue
    return succesful_logs, unsuccessful_logs


def update_balance_opened_trades(capital_spent):
    df = pull_balance_df()

    previous_balance = df['balance'].iloc[-1]
    new_balance = previous_balance - capital_spent
    logger.info(f"previous_balance: {previous_balance} new_balance: {new_balance} capital_spent: {capital_spent}")
    df = df.append({'date': datetime.now().strftime("%Y-%m-%d"), 'balance': new_balance}, ignore_index=True)
    
    # Save the updated DataFrame back to S3
    s3.put_object(Bucket=trading_data_bucket, Key=f"trading_balance/{datetime.now().strftime("%Y/%m/%d/%H/%M")}", Body=df.to_csv(index=False))
    
    return df

def update_balance_closed_trades(capital_received):
    df = pull_balance_df()

    previous_balance = df['balance'].iloc[-1]
    new_balance = previous_balance + capital_received
    logger.info(f"previous_balance: {previous_balance} new_balance: {new_balance} capital_received: {capital_received}")
    df = df.append({'date': datetime.now().strftime("%Y-%m-%d"), 'balance': new_balance}, ignore_index=True)
    
    # Save the updated DataFrame back to S3
    s3.put_object(Bucket=trading_data_bucket, Key=f"trading_balance/{datetime.now().strftime("%Y/%m/%d/%H/%M")}", Body=df.to_csv(index=False))
    
    return df

def pull_balance_df():
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=trading_data_bucket,Prefix='trading_balance/')['Contents']
    
    # Sort the objects by last modified date and get the most recent one
    latest_file = sorted(objects, key=lambda x: x['LastModified'], reverse=True)[0]
    
    # Download the most recent file and load it into a pandas DataFrame
    csv_obj = s3.get_object(Bucket=trading_data_bucket, Key=latest_file['Key'])
    df = pd.read_csv(csv_obj.get("Body"))

    return df

if __name__ == "__main__":
    # lambda_signifier = retrieve_signifier()
    # print(lambda_signifier)
    run_opened_trades_data_process(None,None)

    