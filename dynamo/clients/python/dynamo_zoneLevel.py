import pandas as pd
import boto3
import numpy as np

session = boto3.session.Session(profile_name='saml')
resource = session.resource('dynamodb','us-east-2')
df=pd.read_excel('ZipData.xlsx', dtype=str)

df.columns=["zip5","zip4","state","division_name","region_name","zone","zoneeffectivedate","createdate"]
df["division_name"]=df["division_name"].str.strip()
df["region_name"]=df["region_name"].str.strip()
df['zoneeffectivedate'] = pd.to_datetime(df['zoneeffectivedate'])
df['createdate'] = pd.to_datetime(df['createdate'])
# Clean-up the data, change column types to strings to be on safer side :)
df = df.replace('', np.nan, regex=True)
print(df)
for i in df.columns:
    df[i] = df[i].astype(str)

# Convert dataframe to list of dictionaries (JSON) that can be consumed by any no-sql database
myl=df.T.to_dict().values()

# Connect to the DynamoDB table
table = resource.Table('ZipZoneMaster')

# Load the JSON object created in the step 3 using put_item method
for address in myl:
    table.put_item(Item=address)
