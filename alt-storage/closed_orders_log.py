import pandas as pd
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import os
import ast

ddb_region = os.environ['AWS_DEFAULT_REGION']
table_list = os.environ['TABLE_LIST']
table_list = ast.literal_eval(table_list)
current_date_and_time = datetime.now()
ddb = boto3.client('dynamodb', region_name=ddb_region)
ddbr = boto3.resource('dynamodb', region_name=ddb_region)
s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        for item in table_list:
            log_name = 'scheduled_monthly_log_' + item + str(current_date_and_time.year) + "_" + str(current_date_and_time.month)
            print('Backup started for: ', log_name)
            table = ddbr.Table(item)
            response = table.scan()
            data = response['Items']

            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                data.extend(response['Items'])

            log_df = pd.DataFrame(data)
            log_df.set_index('order_id', inplace=True)
            log_csv = log_df.to_csv()
            s3.put_object(Bucket= 'closed-orders-log', Key=f"monthly/{item}/{log_name}.csv", Body=log_csv)
        
    except ClientError as e:
        print(e)
    
    except ValueError as ve:
        print('error:',ve)
        
    except Exception as ex:
        print(ex)


# def month_year_extraction(date):
#     if date is str:
#         date_year = date[:4]
#         date_month = date[5:7]
#         month_year = date_month + date_year
#     else:
#         try:
#             month_year = date.strftime('%m/%y')
#             date_year = date.strftime('%y')
#             date_month = date.strftime('%m')
#         except Exception as e:
#             print(e)
#     return date_month, date_year, month_year