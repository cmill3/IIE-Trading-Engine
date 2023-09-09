from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import os
import ast

ddbRegion = os.environ['AWS_DEFAULT_REGION']
table_list = os.environ['TABLE_LIST']
table_list = ast.literal_eval(table_list)
current_date_and_time = datetime.now()
backup_name = 'scheduled_backup_' + str(current_date_and_time.year) + "_" + str(current_date_and_time.month) + "_" + str(current_date_and_time.day) + "_" + str(current_date_and_time.hour) + "_" + str(current_date_and_time.minute)
print('Backup started for: ', backup_name)
ddb = boto3.client('dynamodb', region_name=ddbRegion)
 
def lambda_handler(event, context):
    try:
        for item in table_list:
            # upper_date = datetime.utcnow() - timedelta(minutes=15)
            lower_date = datetime.utcnow() - timedelta(days=4)

            backups_to_delete = ddb.list_backups(TableName=item, TimeRangeLowerBound=datetime(lower_date.year, lower_date.month, lower_date.day))#, TimeRangeUpperBound=datetime(upper_date.year, upper_date.month, upper_date.day))
            deletion_backup_count=len(backups_to_delete['BackupSummaries'])

            delete_upper_date = datetime.utcnow() - timedelta(minutes=15)
            delete_lower_date = datetime.utcnow() - timedelta(days=4)

            response = ddb.list_backups(TableName=item, TimeRangeUpperBound=delete_upper_date)

            if deletion_backup_count > 0:
                for record in response['BackupSummaries']:
                    backup_arn = record['BackupArn']
                    ddb.delete_backup(BackupArn=backup_arn)
                    print(backup_name, 'has deleted this backup:', backup_arn)

                response = ddb.list_backups(TableName=item, TimeRangeLowerBound=datetime(delete_lower_date.year, delete_lower_date.month, delete_lower_date.day)) #, TimeRangeUpperBound=datetime(delete_upper_date.year, delete_upper_date.month, delete_upper_date.day)

            else:
                print('Backups that meet deletion criteria have been deleted')

    except  ClientError as e:
        print(e)
    
    except ValueError as ve:
        print('error:',ve)
        
    except Exception as ex:
        print(ex)