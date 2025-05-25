import boto3, uuid
import pandas as pd
from io import BytesIO

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb').Table('UploadedFileData')

def lambda_handler(event, context):
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    
    file_obj = s3.get_object(Bucket=bucket, Key=key)
    file_content = file_obj['Body'].read()

    # Parse CSV
    if key.endswith('.csv'):
        df = pd.read_csv(BytesIO(file_content))

    # Parse Excel
    elif key.endswith('.xlsx'):
        df = pd.read_excel(BytesIO(file_content))

    else:
        print("Unsupported file type")
        return {'statusCode': 400, 'body': 'Unsupported file'}

    # Save to DynamoDB
    for _, row in df.iterrows():
        item = row.dropna().to_dict()
        item['record_id'] = str(uuid.uuid4())
        dynamodb.put_item(Item=item)

    return {'statusCode': 200, 'body': f'{key} processed'}
