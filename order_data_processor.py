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

trading_mode = os.getenv('TRADING_MODE')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
table = os.getenv('TABLE')
close_table = os.getenv("CLOSE_TABLE")
user = os.getenv("USER")
urllib3.disable_warnings(category=InsecureRequestWarning)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
est = pytz.timezone('US/Eastern')

s3 = boto3.client('s3')
logs = boto3.client('logs')

def run_closed_trades_data_process(event,context):
    lambda_signifier = event.get('lambda_signifier', 'default_value')
    succesful_logs, unsuccesful_logs = pull_log_data("closed",lambda_signifier)
    create_dynamo_order_close(succesful_logs)
    success_df = pd.DataFrame.from_dict(succesful_logs)
    failed_df = pd.DataFrame.from_dict(unsuccesful_logs)
    s3.put_object(Bucket=trading_data_bucket, Key=f"failed_close_orders_data/{datetime.now(est).strftime('%Y/%m/%d')}.csv", Body=failed_df.to_csv(index=False))
    s3.put_object(Bucket=trading_data_bucket, Key=f"successful_close_orders_data/{datetime.now(est).strftime('%Y/%m/%d')}.csv", Body=success_df.to_csv(index=False))
    return "Created new dynamo records for closed orders"


def run_opened_trades_data_process(event,context):
    lambda_signifier = event.get('lambda_signifier', 'default_value')
    # lambda_signifier = "20240212+1642"
    succesful_logs, unsuccesful_logs = pull_log_data("opened",lambda_signifier)
    create_dynamo_order_open(succesful_logs)
    success_df = pd.DataFrame.from_dict(succesful_logs)
    failed_df = pd.DataFrame.from_dict(unsuccesful_logs)
    s3.put_object(Bucket=trading_data_bucket, Key=f"failed_new_orders_data/{datetime.now(est).strftime('%Y/%m/%d')}.csv", Body=failed_df.to_csv(index=False))
    s3.put_object(Bucket=trading_data_bucket, Key=f"successful_new_orders_data/{datetime.now(est).strftime('%Y/%m/%d')}.csv", Body=success_df.to_csv(index=False))
    return "Created new dynamo records for open orders"


def create_dynamo_order_open(log_messages):
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode,user)
    for log in log_messages:
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, log['order_id'])
        db.create_new_dynamo_record_order_logmessage(order_info_obj,log, trading_mode, table)
        logger.info(f"Created new dynamo record for order_id: {log['order_id']} in {table}")

    return "Created new dynamo records for open orders"

def create_dynamo_order_close(log_messages):
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode,user)
    for log in log_messages:
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, log['order_id'])
        original_order = db.delete_order_record(log['order_id'], table)
        create_response = db.create_new_dynamo_record_closed_order_logmessage(order_info_obj, original_order, log, trading_mode, close_table)
        logger.info(f"Created new dynamo record for order_id: {log['order_id']} {log['closing_order_id']} in {close_table}")

    return "Created new dynamo records for close orders"


def pull_log_data(process_type,lambda_signifier):
    log_messages = {}
    if process_type == "closed":
        log_group_name = '/aws/lambda/open-trades-portfolio-manager-inv-prod-val'
    elif process_type == "opened":
        log_group_name = '/aws/lambda/new-trades-portfolio-manager-inv-prod-val'
    minute_ms = 60000
    minutes_back = 15

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
            print(message)
            try:
                parts = message.split('\t')
                json_part = parts[-1]
                log_dict = json.loads(json_part)

                if log_dict['lambda_signifier'] != lambda_signifier:
                    continue

                if process_type == "closed":
                    if log_dict['log_type'] == "close_success":
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


if __name__ == "__main__":
    run_opened_trades_data_process(None,None)
    # run_new_trades_data_process(None,None)

    