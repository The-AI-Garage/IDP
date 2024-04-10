from lambdaFunctions.comprehend_classifier.custom_classifier import ComprehendClassifier
import json
import boto3
import os
import time
import logging
import sys
import tarfile
import pandas as pd
import numpy as np
import datetime

logger = logging.getLogger()
logger.setLevel("INFO")


def lambda_handler(event, context):
    # TODO implement
    
    # helper
    millis = str(int(round(time.time() * 1000)))
    
    # load variables
    comprehend_arn = 'arn:aws:comprehend:us-east-1:<accound_id>:document-classifier/custom-classifier/version/420-docs'
    jobname = 'comprehend-job' + millis
    input_bucket = 'my-bucket-for-classifier-ml'
    input_key = 'simple_text_documents'
    input_format = 'ONE_DOC_PER_FILE'
    output_bucket = 'my-bucket-for-classifier-ml'
    output_key = 'output_from_model'
    role_arn = 'arn:aws:iam::<accound_id>:role/classifier-comprehend'
    
    # init classifier
    comprehend_custom_class = ComprehendClassifier(comprehend_arn)
    
    # start inference
    response = comprehend_custom_class.start_job(jobname, 
                                    input_bucket, 
                                    input_key, 
                                    input_format, 
                                    output_bucket, 
                                    output_key, 
                                    role_arn)
    
    # buscamos job
    job = comprehend_custom_class.describe_job(response['JobId'])
    job_name = job["JobName"]
    print(f'Got classification job {job_name}')
    # comprobamos que el job termino
    start_time = time.time()
    while job['JobStatus'] != 'COMPLETED':
        print('.', end='', flush=True)
        time.sleep(1)
        job = comprehend_custom_class.describe_job(response['JobId'])
    print("\njob " + job['JobStatus'])
    print("execution finished in {} seconds".format(time.time() - start_time))
    
    # post processing
    # download results from s3
    # loop through docs in bucket, get names of all docs
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(output_bucket)
    # get account id
    client = boto3.client("sts")
    account = client.get_caller_identity()["Account"]
    # to hold all docs in bucket
    docs_list = []
    job_id = response['JobId']
    for bucket_object in bucket.objects.filter(Prefix=f'{output_key}/{account}-CLN-{job_id}/'):
        if bucket_object.key.endswith('.gz'):
            docs_list.append(bucket_object.key)
    print(docs_list)
    
    object_key = docs_list[0]

    # Define the local file path where you want to save the downloaded object
    local_file_path = '/tmp/output.tar.gz'

    # Download the object
    s3_client = boto3.client('s3')
    s3_client.download_file(output_bucket, object_key, local_file_path)

    print(f"Downloaded '{object_key}' from S3 bucket '{output_bucket}' to '{local_file_path}'")

    # untar result
    tar = tarfile.open("/tmp/output.tar.gz")
    tar.extractall(path='/tmp')
    tar.close()
    
    load_results = []

    # read predictions
    with open('/tmp/predictions.jsonl', 'r') as file:
        for line in file:
            load_result = json.loads(line)
            load_results.append(load_result)
    
    #transform to dataframe
    df = pd.read_json('/tmp/predictions.jsonl', lines=True)
    #drop unnecessary column
    df.drop(columns='Line', inplace=True)
    #for multi-class
    for row in range(df.shape[0]):
        # get class with max score
        row_value = df.iloc[row]['Classes']
        max_score = max([ clss['Score']  for clss in row_value])
        cls_name = [clss['Name'] for clss in row_value if clss['Score'] == max_score]
        df.at[row,'Classes'] = cls_name[0]
        df.at[row,'Score'] = max_score
    
    # check confidence
    #df['Confidence'] = df['Score'].apply(lambda x: "OK" if x >= 0.6 else "CHECK")
    for row in range(df.shape[0]):
        row_value = df.iloc[row]['Classes']
        score_m = df.iloc[row]['Score']
        if row_value == 'ACTIVIDAD' and score_m >= 0.8:
            df.at[row,'Confidence'] = "OK"
        elif row_value == 'LECTURA' and score_m >= 0.5:
            df.at[row,'Confidence'] = "OK"
        else:
            df.at[row,'Confidence'] = "CHECK"
    
    date = datetime.datetime.now()        
    # to csv 
    df.to_csv('/tmp/result.csv', index=False)
    df.to_excel('/tmp/result.xlsx', index=False)
    # upload result to s3
    bucket_name = 'my-bucket-for-classifier-ml'
    object_key_csv = f'predictions/result-{date}.csv'
    object_key_excel = f'predictions/result-{date}.xlsx'
    file_location_csv = f'/tmp/result.csv'
    file_location_xlsx = f'/tmp/result.xlsx'
    s3_client.upload_file(file_location_csv, bucket_name, object_key_csv)
    time.sleep(5)
    s3_client.upload_file(file_location_xlsx, bucket_name, object_key_excel)
    
    
    return {
        'statusCode': 200,
        'response': str(date)
    }

