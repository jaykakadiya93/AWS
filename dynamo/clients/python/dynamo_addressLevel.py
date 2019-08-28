import pandas as pd
import boto3
import numpy as np

session = boto3.session.Session(profile_name='saml')
resource = session.resource('dynamodb','us-east-2')
df=pd.read_excel('AddressData.xlsx', dtype=str)

df.columns=["ID", "Address1", "City", "StateAbbr", "Zip", "Zip4", "Latitude", "Longitude", "NaxId", "Zoneoverride", "ZoneoverrideStartDate","ZoneoverrideEndDate","CreateDate"]

#removing the trailing spaces on address
df["Address1"]=df["Address1"].str.strip()
df["City"]=df["City"].str.strip()
df["StateAbbr"]=df["StateAbbr"].str.strip()
df['ZoneoverrideStartDate'] = pd.to_datetime(df['ZoneoverrideStartDate'])
df['ZoneoverrideEndDate'] = pd.to_datetime(df['ZoneoverrideEndDate'])
df['CreateDate'] = pd.to_datetime(df['CreateDate'])

print(df["NaxId"])
#Replacing all empty values with nan
df = df.replace('', np.nan, regex=True)

for i in df.columns:
    df[i] = df[i].astype(str)

# Convert dataframe to list of dictionaries (JSON) that can be consumed by any no-sql database

myl=df.T.to_dict().values()

# Connect to the DynamoDB table

table = resource.Table('AddressZoneMaster')

# Load the JSON object created in the step 3 using put_item method

for address in myl:
    table.put_item(Item=address)