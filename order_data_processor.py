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

def run_open_order_data_process(event,context):
    log_data = pull_log_data("close")


def pull_log_data(process_type):
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
            print(event['message'])  # Process each log event as needed

    return None


if __name__ == "__main__":
    run_close_order_data_process(None,None)
    run_open_order_data_process(None,None)

    