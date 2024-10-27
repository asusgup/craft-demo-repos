import boto3
from botocore.exceptions import ClientError
from jproperties import Properties
import logging

configs = Properties()
with open('dynamoDB-config.properties', 'rb') as config_file:
		 configs.load(config_file)


def insertInto_DynamoDB(decoded_json, table_name):
    # Initialize a session using your AWS credentials
    session = boto3.Session(
        aws_access_key_id=configs.get("YOUR_ACCESS_KEY").data,       
        aws_secret_access_key=configs.get("YOUR_SECRET_KEY").data,   
        region_name=configs.get("YOUR_REGION").data
    )

    # Initialize DynamoDB resource
    dynamodb = session.resource('dynamodb')

    # Specify the table
    table_name = configs.get("DYNAMO_TABLE_NAME").data
    table = dynamodb.Table(table_name)

    # Insert data into the table
    logging.info("Insert data into the dynamodb table")	
    try:
        response = table.put_item(Item=item)
        logging.info("Insert succeeded:", response)
    except ClientError as e:
        logging.error("Error inserting item:", e.response['Error']['Message'])
