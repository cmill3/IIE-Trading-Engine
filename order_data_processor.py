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

trading_mode = os.getenv('TRADING_MODE')
trading_data_bucket = os.getenv('TRADING_DATA_BUCKET')
table = os.getenv('TABLE')
close_table = os.getenv("CLOSE_TABLE")
urllib3.disable_warnings(category=InsecureRequestWarning)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
logs = boto3.client('logs')

def run_close_order_data_process(event,context):
   log_data = pull_log_data("open")
   create_dynamo_order_open(log_data)


def run_open_order_data_process(event,context):
    log_data = pull_log_data("close")
    create_dynamo_order_close(log_data)


def create_dynamo_order_open(log_messages):
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode)
    for order_id, message in log_messages.items():
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, order_id)
        db.create_new_dynamo_record_order_logmessage(order_info_obj,message, trading_mode, table)
        logger.info(f"Created new dynamo record for order_id: {order_id} in {table}")

    return "Created new dynamo records for open orders"

def create_dynamo_order_close(log_messages):
    base_url, account_id, access_token = trade.get_tradier_credentials(trading_mode)
    for order_id, message in log_messages.items():
        order_info_obj = trade.get_order_info(base_url, account_id, access_token, order_id)
        original_order = db.delete_order_record(log_messages['order_id'], table)
        create_response = db.create_new_dynamo_record_closed_order_logmessage(order_info_obj, original_order, message,trading_mode, close_table)
        logger.info(f"Created new dynamo record for order_id: {order_id} {message['closing_order_id']} in {close_table}")

    return "Created new dynamo records for close orders"


def pull_log_data(process_type):
    log_messages = {}
    if process_type == "open":
        log_group_name = '/aws/lambda/open-trades-portfolio-manager-inv-prod-val'
    else:
        log_group_name = '/aws/lambda/new-trades-portfolio-manager-inv-prod-val'

    # Calculate the time range for the last 24 hours
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = int((datetime.now()).timestamp() * 1000) - 60000
    print(f"start_time: {start_time} end_time: {end_time}")

    # Create a paginator to fetch logs
    paginator = logs.get_paginator('filter_log_events')

    # Pagination options
    pagination_options = {
        'logGroupName': log_group_name,
        'startTime': start_time,
        'endTime': end_time,
        'limit': 100,  # Adjust based on how many logs you want to fetch per page
    }

    # Fetch and process log events
    for page in paginator.paginate(**pagination_options):
        for event in page['events']:
            print(event['message'])
            log_messages[event['message']['order_id']] = event['message']
    return log_messages


if __name__ == "__main__":
    run_close_order_data_process(None,None)
    run_open_order_data_process(None,None)

    