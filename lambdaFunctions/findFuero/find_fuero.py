import json
import boto3
import time
import string
import random
import numpy as np
import pandas as pd

def find_text_between_breaklines(text):
    words_to_find = ["tribunal", "juzgado", "camara", "unidad funcional", "secretaria", "suprema"]
    lines = text.split("\n")  # Split text into lines
    found_line = None
    for i,line in enumerate(lines):
        for word in words_to_find:
            if word in line:
                found_line = line
                print(found_line)
                break
        if found_line:
            break

    if found_line:
        fueros_list = ["penal", "civil y comercial", "contencioso administrativo", "trabajo", "familia", "laboral"]
        city_list = ["moron", "la matanza", "san martin", "san miguel", "moreno", "mercedes",
                    "junin", "pergamino", "la plata", "quilmes", "san isidro", "pilar", "dolores",
                    "general rodriguez", "san nicolas"]
        # find coincidences in list
        for term in fueros_list:
            if term in found_line:
                fuero = term
                print(f"fuero: {fuero}")
                break
            else:
                print(f"fuero not found in {found_line}")
        for term in city_list:
            if term in found_line:
                city = term
                print(f"ciudad: {city}")
                break
            else:
                print(f"city not found in {found_line}")
        return [fuero, city]

    else:
        return "Word not found in text"


def lambda_handler(event, context):
    
    # Init variables
    date = event['Payload']['response']
    s3 = boto3.client("s3")
    csv_path = f'predictions/'
    bucket_name = 'my-bucket-for-classifier-ml'
    folder_path = 'simple_text_documents'
    
    # Read csv file
    csv_object = s3.list_objects(Bucket=bucket_name, Prefix=csv_path)
    print(f"objects: {csv_object}")
    
    # get current csv file
    for obj in csv_object.get('Contents', []):
        if obj['Key'].endswith(f'{date}.csv'):
            csv_content = s3.get_object(Bucket=bucket_name, Key=obj['Key'])['Body']
        else:
            continue
    # csv to pandas dataframe
    print(f"csv_content: {csv_content}")
    df = pd.read_csv(csv_content)
    print(f"dataframe: {df}")

    # find txt files listed in dataframe
    for row in range(df.shape[0]):
        # Descarga el contenido del archivo TXT desde S3
        file_name = df.iloc[row]['File']
        obj_key = folder_path + "/" + str(file_name)
        print(file_name)
        file_content = s3.get_object(Bucket=bucket_name, Key=obj_key)['Body'].read().decode('utf-8')
        fuero_and_city = find_text_between_breaklines(file_content.lower())
        df.at[row,'Fuero'] = fuero_and_city[0]
        df.at[row,'Ciudad'] = fuero_and_city[1]
    # to csv 
    df.to_csv('/tmp/result.csv', index=False)
    df.to_excel('/tmp/result.xlsx', index=False)
    # upload result to s3
    object_key_csv = f'predictions/result-{date}.csv'
    object_key_excel = f'predictions/result-{date}.xlsx'
    file_location_csv = f'/tmp/result.csv'
    file_location_xlsx = f'/tmp/result.xlsx'
    s3.upload_file(file_location_csv, bucket_name, object_key_csv)
    time.sleep(5)
    s3.upload_file(file_location_xlsx, bucket_name, object_key_excel)
    

    return {
        'statusCode': 200,
        'response': str(date)
    }
