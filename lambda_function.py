import csv
import json
import boto3

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    dynamo_db = boto3.resource('dynamodb')
    dynamoTable = dynamo_db.Table('Test_CSV_DATA')
    if event:
        print("Event: ", event)
        file_obj = event["Records"][0]
        filename = str(file_obj['s3']['object']['key'])
        print("Filename: ", filename)
        fileObj = s3.get_object(Bucket='csv-files-storage', Key=filename)
        
        file_content = fileObj['Body'].read().decode('utf-8-sig').splitlines()
        file_content = csv.DictReader(file_content)
        for dct in map(dict, file_content):
            dynamoTable.put_item(Item = dct)
            
        fileObj = s3.delete_object(Bucket='csv-files-storage', Key=filename)

    return "Success"

