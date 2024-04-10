import json
import logging
import boto3
from botocore.exceptions import ClientError


def create_presigned_url(bucket_name, object_name, expiration=43200):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response

def send_email(subject, body, sender, recipient):
    """send mail through Amazon SES

    :param subject: string
    :param body: string
    :param sender: string
    :param recipient: string
    :return: response message id
    """
    CHARSET = "UTF-8"
    client = boto3.client('ses', region_name='us-east-1')
    response = client.send_email(
        Source=sender,
        Destination={
            'ToAddresses': [recipient]
        },
        Message={
            'Subject': {
                'Data': subject
            },
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': body,
                },
            }
        }
    )
    return response['MessageId']


def lambda_handler(event, context):
    # Init variables
    date = event['Payload']['response']
    bucket_name = 'my-bucket-for-classifier-ml'
    s3 = boto3.client('s3')
    object_name = [f'predictions/result-{date}.xlsx']
    # Read S3 objects in a specific path
    results_path = 'documents_splitted_by_class'
    documents_s3 = s3.list_objects(Bucket=bucket_name, Prefix=results_path)
    
    for obj in documents_s3.get('Contents', []):
        if obj['Key'].endswith(f'{date}.zip'):
            object_name.append(obj['Key'])
            print(f"document found in s3: {obj['Key']}")
    
    # create a presigned url for each object
    url_array = []
    for obj in object_name:
        url = create_presigned_url(bucket_name, obj)
        url_array.append(url)
    
    # mapping between url and document name
    mapping = dict(zip(object_name, url_array))
    # body for email in html
    body_html = f""" <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Email Template</title>
    </head>
    <body style="font-family: Arial, sans-serif;">

        <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ccc;">
            <h1 style="text-align: center;">Documentos Clasificados</h1>
            <p>Hola,</p>
            <p>Este es un mensaje del clasificador de cedulas.</p>
            <p>En los links encontrarás los resultados de la clasificación, los archivos divididos por clases y los archivos a revisar.</p>
            """
    for nombre_archivo, url in mapping.items():
        nombre_archivo = nombre_archivo.replace('documents_splitted_by_class/', '')
        nombre_archivo = nombre_archivo.replace('predictions/', '')
        body_html += f'<p><a href="{url}">{nombre_archivo}</a></p>\n'

    # Cerrar el cuerpo del HTML
    body_html += """</div>
    </body>
    </html>"""
            #<p><a href={url_array[0]} > resultados en excel</a></p>
            #<p><a href={url_array[0]} > cedulas actividad </a></p>
            #<p><a href={url_array[1]} > cedulas lectura</a></p>
            #<p><a href={url_array[3]} > cedulas a revisar</a></p>
            #<p>Muchas gracias!</p>
        #</div>

    #</body>
    #</html>"""
    # send presigned url by email
    subject = "Resultados de la clasificación"
    body = body_html
    sender = "hectorm242@gmail.com" # your_verified_email@example.com
    recipient = "hectorm242@gmail.com" # recipient_email@example.com

    send_email(subject, body, sender, recipient)
    
    return {
        'statusCode': 200,
        'body': json.dumps('All done!')
    }
