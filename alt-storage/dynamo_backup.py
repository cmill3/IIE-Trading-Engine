from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import os

ddbRegion = os.environ['AWS_DEFAULT_REGION']
current_date_and_time = datetime.now()
backupName = 'Scheduled_Backup' + "_" + str(datetime.now(current_date_and_time.year) + "_" + str(datetime.now(current_date_and_time.month)) + "_" + str(datetime.now(current_date_and_time.day)) + "_" + str(datetime.now(current_date_and_time.hour)) + "_" + str(datetime.now(current_date_and_time.minute)))
print('Backup started for: ', backupName)
ddb = boto3.client('dynamodb', region_name=ddbRegion)
ddb_table_list = ['icarus-closed-orders-table', 'icarus-closed-orders-table-inv', 'icarus-orders-table', 'icarus-orders-table-inv']

# for deleting old backup. It will search for old backup and will escape deleting last backup days you mentioned in the backup retention
# daysToLookBackup=2
# daysToLookBackup= int(os.environ['BackupRetention'])
# daysToLookBackupL = daysToLookBackup - 4
 
def lambda_handler(event, context):
    try:
        for item in ddb_table_list:
        #create backup
            ddb.create_backup(TableName=item, BackupName = backupName)
            print('Backup has been taken successfully for table:', item)
            
            #check number/information of backup in the set time range (backups older than 3 days)
            lower_date = datetime.now() - timedelta(days=4)
            upper_date = datetime.now() - timedelta(days=3)
            
            backups_to_delete = ddb.list_backups(TableName=item, TimeRangeLowerBound=datetime(lower_date.year, lower_date.month, lower_date.day)) #, TimeRangeUpperBound=datetime(upper_date.year, upper_date.month, upper_date.day))
            deletion_backup_count=len(backups_to_delete['BackupSummaries'])
            print('Total backup count in recent days:',deletion_backup_count)

            delete_upper_date = datetime.now() - timedelta(days=4)
            delete_lower_date = datetime.now() - timedelta(days=3)
            # print(delete_upper_date)

            # TimeRangeLowerBound is the release of Amazon DynamoDB Backup and Restore - Nov 29, 2017
            response = ddb.list_backups(TableName=item, TimeRangeLowerBound=datetime(delete_lower_date.year, delete_lower_date.month, delete_lower_date.day)) #, TimeRangeUpperBound=datetime(delete_upper_date.year, delete_upper_date.month, delete_upper_date.day))
            
            #check whether latest backup count is more than two before removing the old backup
            if deletion_backup_count>0:
                if 'LastEvaluatedBackupArn' in response:
                    last_eval_bucket_arn = response['LastEvaluatedBackupArn']
                else:
                    last_eval_bucket_arn = ''
                
                while (last_eval_bucket_arn != ''):
                    for record in response['BackupSummaries']:
                        backup_arn = record['BackupArn']
                        ddb.delete_backup(BackupArn=backup_arn)
                        print(backupName, 'has deleted this backup:', backup_arn)

                    response = ddb.list_backups(TableName=item, TimeRangeLowerBound=datetime(delete_lower_date.year, delete_lower_date.month, delete_lower_date.day), ExclusiveStartBackupArn=last_eval_bucket_arn) #, TimeRangeUpperBound=datetime(delete_upper_date.year, delete_upper_date.month, delete_upper_date.day)
                    if 'LastEvaluatedBackupArn' in response:
                        last_eval_bucket_arn = response['LastEvaluatedBackupArn']
                    else:
                        last_eval_bucket_arn = ''
                        print ('the end')
            else:
                print ('Backups that meet deletion criteria have been deleted')

    except  ClientError as e:
        print(e)
    
    except ValueError as ve:
        print('error:',ve)
        
    except Exception as ex:
        print(ex)