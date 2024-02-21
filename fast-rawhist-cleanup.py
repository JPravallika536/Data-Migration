import sys
import time
import boto3
from io import StringIO
import configparser
from awsglue.utils import getResolvedOptions
import logging_service

CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
DATE_FUNC = "date_func"
SOURCE_SYSTEM = "src_sys"
FEED_NAME = "feed_name"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
PROCESS_KEY = "landing_to_raw"
JOB_KEY = "fast_rawhist_cleanup"

args = getResolvedOptions(sys.argv,
                          [CONFIG_BUCKET_NAME_KEY, SYS_CONFIG_KEY, FEED_CONFIG_FILE_KEY, REGION_KEY, SOURCE_SYSTEM,
                           FEED_NAME, DATE_FUNC, GUID_KEY])


def main():
    global BOTO3_AWS_REGION

    guid = args[GUID_KEY]
    src_sys = args[SOURCE_SYSTEM]
    feed_name = args[FEED_NAME]
    date_func = args[DATE_FUNC]
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    pre_raw_bucket_name = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')
    work_group_name = sys_config.get(args[REGION_KEY], 'work_group')
    raw_bucket_name = sys_config.get(args[REGION_KEY], 'raw_bucket')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)
    client_s3 = boto3.client('s3')

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-fast-rawhist-cleanup')
    log_manager.log(message="Starting the fast rawhist cleanup job", args={"environment": args[REGION_KEY], "job": JOB_KEY})

    distinct_event_date = "select distinct event_creation_time from fast_" + feed_name + "_hist where event_creation_time >= '" + date_func + "'"
    exe_id = run_query(boto3_aws_region, distinct_event_date, athena_raw_database,
                               "s3://" + pre_raw_bucket_name + "/tmp/athena-query-results/drop_partition",work_group_name)
    print(exe_id)
    log_manager.log(message='date list created', args={"job": JOB_KEY})
    copy_key = "temp-athena-results/" + exe_id + ".csv"
    filename = client_s3.get_object(Bucket=pre_raw_bucket_name, Key=copy_key)
    linesf = filename['Body'].read().decode('utf-8').splitlines(True)
    lines = list(map(str.strip, linesf))
    print(lines)

    length = len(lines)
    print(length)
    if length > 0:
        i = 1
        while i < length:
            date_value_int = lines[i]
            date_value = date_value_int.strip('"')
            file_prefix = 'fast_raw_hist' + '/' + src_sys.upper() + '_' + feed_name.upper() + '_RAW_ES' + '/event_creation_time=' + date_value
            print(file_prefix)
            raw_hist_cleanup = s3cleanup(raw_bucket_name, file_prefix)
            print("raw_bucket clean up completed for date " + date_value)
            drop_partition_query = "alter table fast_" + feed_name + "_hist drop partition (event_creation_time = '" + date_value + "')"
            drop_query_ext = run_query(boto3_aws_region, drop_partition_query, athena_raw_database,
                               "s3://" + pre_raw_bucket_name + "/tmp/athena-query-results/drop_partition",work_group_name)
            print("drop partition completed for date " + date_value)
            i += 1

    else:
        log_manager.log(message='no data available for these dates', args={"job": JOB_KEY})
        print("no data available for these dates")

def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    i = 0
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.readfp(buf)
        return config


def s3cleanup(bucket, file_prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        s3.Object(bucket.name, obj.key).delete()


def run_query(region, query, database, s3_output, work_group_name):
    client = boto3.client('athena', region_name=region)
    # response = client.start_query_execution(
    execution_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        },
        WorkGroup = work_group_name
    )
    # print('Execution ID: ' + execution_id['QueryExecutionId'])
    print(execution_id['QueryExecutionId'])
    # sys.exit(0)
    if not execution_id:
        return

    while True:
        stats = client.get_query_execution(QueryExecutionId=execution_id['QueryExecutionId'])
        status = stats['QueryExecution']['Status']['State']
        exe_id = execution_id['QueryExecutionId']
        print (status)
        if status == 'RUNNING':
            time.sleep(5)
        elif status == 'QUEUED':
            time.sleep(5)
        else:
            print ("job completed with status of " + status)
            return exe_id
            break


if __name__ == '__main__':
    main()
