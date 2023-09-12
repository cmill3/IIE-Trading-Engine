from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import os
import ast

ddb_region = os.environ['AWS_DEFAULT_REGION']
table_list = os.environ['TABLE_LIST']
frequency = os.environ['FREQUENCY']
table_list = ast.literal_eval(table_list)
current_date_and_time = datetime.now()
backup_name = 'scheduled_backup_' + str(current_date_and_time.year) + "_" + str(current_date_and_time.month) + "_" + str(current_date_and_time.day) + "_" + str(current_date_and_time.hour) + "_" + str(current_date_and_time.minute)
print('Backup started for: ', backup_name)
ddb = boto3.client('dynamodb', region_name=ddb_region)
 
def lambda_handler(event, context):
    try:
        for item in table_list:
            #Create backup
            ddb.create_backup(TableName=item, BackupName = backup_name)
            print('Backup has been taken successfully for table:', item)

            #Change variables depending on frequency from YAML env variable
            if frequency == "15MIN":
                upper_date = datetime.utcnow() - timedelta(minutes=15)
                lower_date = datetime.utcnow() - timedelta(days=4)
                delete_upper_date = datetime.utcnow() - timedelta(minutes=30)
                delete_lower_date = datetime.utcnow() - timedelta(days=4)
            elif frequency == "DAILY":
                upper_date = datetime.utcnow() - timedelta(hours=24)
                lower_date = datetime.utcnow() - timedelta(days=4)
                delete_upper_date = datetime.utcnow() - timedelta(hours=24)
                delete_lower_date = datetime.utcnow() - timedelta(days=4)
            else:
                print("frequency env variable failure")

            #Pulls all of the backups that are listed after the minimum time since the last backup (daily = 24 hours, 15 min = 15 min)
            response = ddb.list_backups(TableName=item, TimeRangeUpperBound=delete_upper_date)
            deletion_backup_count=len(response['BackupSummaries'])

            #itterates through to delete each backup if the list of backups to be deleted is is greater than zero
            if deletion_backup_count > 0:
                for record in response['BackupSummaries']:
                    backup_arn = record['BackupArn']
                    ddb.delete_backup(BackupArn=backup_arn)
                    print(backup_name, 'has deleted this backup:', backup_arn)
            else:
                print('Backups that meet deletion criteria have been deleted')

    except ClientError as e:
        print(e)
    
    except ValueError as ve:
        print('error:',ve)
        
    except Exception as ex:
        print(ex)