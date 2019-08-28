 
import boto3
import json
import time
from decimal import Decimal


# Modify this section to reflect your AWS configuration.
awsRegion = "us-east-2"         # The AWS region where your Kinesis Analytics application is configured.
accessKeyId = ""       # Your AWS Access Key ID
secretAccessKey = ""   # Your AWS Secret Access Key
inputStream = "load_address_stream"       # The name of the stream being used as input into the Kinesis Analytics hotspots application


def main():
    session = boto3.session.Session(profile_name='lokesh')
    kinesis = session.client('kinesis')

    while True:
        with open('athenaAddressSummaryStr10.json') as lines:
            for address in lines:
                kinesis.put_records(StreamName="load_address_stream", Records=[
                {
                    'Data':json.loads(json.dumps(address), parse_float=Decimal),
                    'PartitionKey':"partitionkey"
                }
                ])
            
if __name__ == "__main__":
    main()
 
