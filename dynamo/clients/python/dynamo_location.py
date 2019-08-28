import pandas as pd
import boto3
import numpy as np

session = boto3.session.Session(profile_name='saml')
resource = session.resource('dynamodb','us-east-2')
df=pd.read_excel('LocationData.xlsx', dtype=str)
df.columns=["CMCST_FOOTPRINT","CMCST_DIVISION","CMCST_REGION","CMCST_CORPSYSPRIN","ATHENA_BUILDING_ID","ATHENA_ADDRESS_ID","NAX_ADDRESS_ID","ATHENA_ADDRESS_TYPE","ATHENA_ADDRESS","ATHENA_ADDRESS_2","ATHENA_CITY","ATHENA_STATE","ATHENA_ZIP_5","ATHENA_ZIP_4","ATHENA_ADDRESS_IS_CASS","ATHENA_ADDRESS_IS_USPS_RESI","ATHENA_ADDRESS_IS_DO_NOT_MAIL",	"ATHENA_ADDRESS_DO_NOT_MAIL_DT","INSITE_IS_SERVICEABLE_DATA","INSITE_IS_SERVICEABLE_VIDEO","INSITE_IS_SERVICEABLE_VOICE","INSITE_IS_SERVICEABLE_METRO_E",	"INSITE_NEXT_SERVICEABLE_DIST","MDT_SELLABILITY_COLOR_COAX","MDT_SELLABILITY_COLOR_FIBER","MDT_TOTAL_DISTANCE_COAX","MDT_TOTAL_DISTANCE_FIBER","ATHENA_MTKG_FRANCHISE_CD",	"ATHENA_AP_DPBC","ATHENA_AP_CHK_DIGIT","ATHENA_CARRIER_RT","ATHENA_AP_LOT","ATHENA_AP_LOT_ORDER","ATHENA_ADDRESS_DLV_MAIL_SCORE","ATHENA_ADDRESS_HAS_CUST_CB",	"ATHENA_ADDRESS_HAS_CUST_RESI"]


df["ATHENA_ADDRESS"] = df["ATHENA_ADDRESS"].str.strip()
df["ATHENA_ADDRESS_2"]=df["ATHENA_ADDRESS_2"].str.strip()
df["ATHENA_CITY"]=df["ATHENA_CITY"].str.strip()
df["ATHENA_STATE"]=df["ATHENA_STATE"].str.strip()
df["CMCST_FOOTPRINT"]=df["CMCST_FOOTPRINT"].str.strip()
df["CMCST_DIVISION"]=df["CMCST_DIVISION"].str.strip()
df["CMCST_REGION"]=df["CMCST_REGION"].str.strip()
df["MDT_SELLABILITY_COLOR_FIBER"]=df["MDT_SELLABILITY_COLOR_FIBER"].str.strip()
df["MDT_SELLABILITY_COLOR_COAX"]=df["MDT_SELLABILITY_COLOR_COAX"].str.strip()
df["CMCST_CORPSYSPRIN"]=df["CMCST_CORPSYSPRIN"].str.strip()
df['ATHENA_ADDRESS_DO_NOT_MAIL_DT'] = pd.to_datetime(df['ATHENA_ADDRESS_DO_NOT_MAIL_DT'])


# Clean-up the data, change column types to strings to be on safer side :)
df = df.replace('', np.nan, regex=True)

for i in df.columns:
    df[i] = df[i].astype(str)
    
# Convert dataframe to list of dictionaries (JSON) that can be consumed by any no-sql database
myl=df.T.to_dict().values()

# Connect to the DynamoDB table

table = resource.Table('LocationMaster')

# Load the JSON object created in the step 3 using put_item method

for address in myl:
    table.put_item(Item=address)
