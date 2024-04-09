import json
import boto3
import pandas as pd
import os
import shutil
import time

def make_tmp_directory(fuero, city, file_name, class_name):
    tmp_path = f"/tmp/{fuero}/{city}/{class_name}/"
    if os.path.exists(tmp_path):
        print(os.path.exists(tmp_path))
        return tmp_path
    else:
        os.makedirs(tmp_path)
        return tmp_path

def lambda_handler(event, context):
    # Init variables
    date = event['Payload']['response']
    s3 = boto3.client("s3")
    bucket_name = 'my-bucket-for-classifier-ml'
    csv_path = f'predictions/'
    documents_path = 'pdf_documents'
    new_folder = 'documents_splitted_by_class'
    
    # Read S3 objects in a specific path
    csv_object = s3.list_objects(Bucket=bucket_name, Prefix=csv_path)
    print(f"objects: {csv_object}")
    
    # get content
    for obj in csv_object.get('Contents', []):
        if obj['Key'].endswith(f'{date}.csv'):
            csv_content = s3.get_object(Bucket=bucket_name, Key=obj['Key'])['Body']
        else:
            continue
    print(f"csv_content: {csv_content}")
    df = pd.read_csv(csv_content)
    print(f"dataframe: {df}")
    
    # temporary directories to save files
    fuero_found_array = []    
    for row in range(df.shape[0]):
        class_name = df.iloc[row]['Classes']
        confidence = df.iloc[row]['Confidence']
        fuero = df.iloc[row]['Fuero']
        city = df.iloc[row]['Ciudad']
        fuero_found_array.append(fuero)
        print(f"class_name: {class_name}")
        if class_name == 'ACTIVIDAD' and confidence == 'OK':
            file_name = df.iloc[row]['File']
            file_name = file_name.replace('.txt','.pdf')
            print(f"file_name: {file_name}")
            # look for this document in pdf_documents/
            object_path = f"{documents_path}/{file_name}"
            print(f"object_path: {object_path}")
            tmp_path = make_tmp_directory(fuero, city, file_name, class_name)
            tmp_dir = tmp_path + file_name 
            #tmp_dir = f'/tmp/actividad/{file_name}'
            s3.download_file(bucket_name, object_path, tmp_dir)
        elif class_name == 'LECTURA' and confidence == 'OK':
            file_name = df.iloc[row]['File']
            file_name = file_name.replace('.txt','.pdf')
            print(f"file_name: {file_name}")
            # look for this document in pdf_documents/
            object_path = f"{documents_path}/{file_name}"
            print(f"object_path: {object_path}")
            tmp_path = make_tmp_directory(fuero, city, file_name, class_name)
            tmp_dir = tmp_path + file_name
            #tmp_dir = f'/tmp/lectura/{file_name}'
            s3.download_file(bucket_name, object_path, tmp_dir)
        else:
            file_name = df.iloc[row]['File']
            file_name = file_name.replace('.txt','.pdf')
            print(f"file_name: {file_name}")
            # look for this document in pdf_documents/
            object_path = f"{documents_path}/{file_name}"
            print(f"object_path: {object_path}")
            class_name = 'revisar'
            tmp_path = make_tmp_directory(fuero, city, file_name, class_name)
            tmp_dir = tmp_path + file_name
            #tmp_dir = f'/tmp/revisar/{file_name}'
            s3.download_file(bucket_name, object_path, tmp_dir)
    # zip directories
    for fuero_found in fuero_found_array:
        base_name_var = f"/tmp/{fuero_found}"
        root_dir = f"/tmp/{fuero_found}"
        archived_Folder = shutil.make_archive(base_name_var, 'zip', root_dir)
        #upload to s3
        s3.upload_file(archived_Folder, bucket_name, f"{new_folder}/{fuero_found}-{date}.zip")
        time.sleep(5)
    
    # delete pdf documents
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)
    for bucket_object in bucket.objects.filter(Prefix='pdf_documents/', Delimiter='/'):
        if bucket_object.key.endswith('.pdf'):
            # Delete the object
            s3.delete_object(Bucket=bucket_name, Key=bucket_object.key)
        
    return {
        'statusCode': 200,
        'response': str(date)
    }
