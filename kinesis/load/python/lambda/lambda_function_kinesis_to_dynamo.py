import base64
import json
from decimal import Decimal
import boto3
import datetime


def lambda_handler(event, context):
    """
    Receive a batch of events from Kinesis and insert into our DynamoDB table
    """
    print('Received request')
    item = None
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('test_address')
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]

    with table.batch_writer() as batch_writer:
        for item in deserialized_data:
            item['processed'] = datetime.datetime.utcnow().isoformat()
            ddb_data = json.loads(json.dumps(item), parse_float=Decimal)
            batch_writer.put_item(Item=ddb_data)

    print('Number of records: {}'.format(str(len(deserialized_data))))