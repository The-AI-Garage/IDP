import boto3
import json

def lambda_handler(event, context):
    date = event['Payload']['response']
    s3_client = boto3.client('s3')
    bucket_name = 'my-bucket-for-classifier-ml'
    
    # loop through docs in bucket, get names of all docs
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)
    for bucket_object in bucket.objects.filter(Prefix='simple_text_documents/', Delimiter='/'):
        if bucket_object.key.endswith('.txt'):
            # Delete the object
            s3_client.delete_object(Bucket=bucket_name, Key=bucket_object.key)

    

    return {
        'statusCode': 200,
        'response': str(date)
    }
