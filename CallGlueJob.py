import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')

glue = boto3.client(service_name='glue')
glueJobName = 'ProcessData'

def lambda_handler(event, context):
    try:
        # glue.start_crawler(Name='ProcessData')
        glue.start_job_run(JobName = glueJobName)
    except Exception as e:
        print(e)
        print('Error starting crawler')
        raise e