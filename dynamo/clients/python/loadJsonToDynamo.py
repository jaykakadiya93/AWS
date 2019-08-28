import boto3
import json
import time


# Modify this section to reflect your AWS configuration.
awsRegion = "us-east-2"         # The AWS region where your Kinesis Analytics application is configured.
accessKeyId = ""       # Your AWS Access Key ID
secretAccessKey = ""   # Your AWS Secret Access Key
inputStream = "load_address_stream"       # The name of the stream being used as input into the Kinesis Analytics hotspots application

session = boto3.session.Session(profile_name='lokesh')
resource = session.resource('dynamodb','us-east-2')
table = resource.Table('test_address')

with open('athenaAddressSummaryStr10.json') as lines:
	for address in lines:
		table.put_item(Item=json.loads(address))