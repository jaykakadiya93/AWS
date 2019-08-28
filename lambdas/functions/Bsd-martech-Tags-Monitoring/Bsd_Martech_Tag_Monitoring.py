import boto3   
import collections     
import datetime     
import time     
import sys 
access_key = ''
secret_key = ''
AccountID = boto3.client('sts').get_caller_identity()['Account']
#Region = boto3.session.Session().region_name
client_sns = boto3.client('sns', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
targetArn = 'arn:aws:sns:'+boto3.session.Session().region_name+':'+AccountID+':Bsd_Martech_Tag_Monitoring'

message = []

subject = """ AWS Resource Tagging Report """
key = ['PoC','Application','Environment','Name','Org','Domain','ComcastDataClassification','GitCloneURL']

#def lambda_handler(event, context):



def EC2_Tagging(Region):
    ec2 = boto3.client('ec2',region_name=Region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    ec2_instance_name = []
    try:
        list_ec2_instances = ec2.describe_instances()['Reservations']
        for i in list_ec2_instances:
            for j in i['Instances']:
                for k in j['Tags']:
                    if k['Key'] == 'Name' and "martech" in k['Value']:
                        ec2_instance_name.append(j['Tags'])
                        for l in key:
                            if(l not in str(ec2_instance_name)):
                                if k['Key'] == 'Name':
                                    name = k['Value']
                                message.append(name+": EC2 instance has no "+ l+" TAG------> Region = "+ Region)
    except Exception as e:
        message.append("EC2 instance has no TAG------> Region = "+ Region)
                                
    #                            print(name, ": EC2 instance has no", l,"TAG")


def Lambda_Tagging(Region):
    Lambda = boto3.client('lambda',region_name=Region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    Lambda_function_name_list = []
    list_lambda_function = Lambda.list_functions()['Functions']
    for i in list_lambda_function:
        if "Martech" in i['FunctionName']:
            Lambda_function_name_list.append(i['FunctionName'])
    for name in range(len(Lambda_function_name_list)):
        response = Lambda.list_tags(Resource="arn:aws:lambda:"+Region+":"+AccountID+":function:"+Lambda_function_name_list[name])
        for j in key:
            if(j not in str(response['Tags'])):
                message.append(Lambda_function_name_list[name]+"Lambda Function has no "+ j+ " TAG-------> Region = "+Region)
#                print(Lambda_function_name_list[name], "Lambda Function has no", j, "TAG")
    

    
def DynamoDB_Tagging(Region):
    dynamodb = boto3.client('dynamodb',region_name=Region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    try:
        list_Dynamodb_Table = dynamodb.list_tables()['TableNames']
        Tables = [s for s in list_Dynamodb_Table if "Martech" in s]
        for Table in range(len(Tables)):
            response = dynamodb.list_tags_of_resource(ResourceArn="arn:aws:dynamodb:"+Region+":"+AccountID+":"+"table/"+Tables[Table])
            for i in key:
                if(i not in str(response['Tags'])):
                    message.append(Tables[Table]+ ": DynamoDb Table has no "+ i+" TAG------> Region = "+Region)
#                    print(Tables[Table], ": DynamoDb Table has no", i,"TAG")
            
    except Exception as e:
        message.append(Tables[Table]+ ": DynamoDb Table has no TAG------> Region = "+Region)
#        print (Tables[Table],": DynamoDB has no Tags")
    
def S3_Tagging(Region):
    s3 = boto3.client('s3',region_name=Region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    list_bucket_name = s3.list_buckets()
    try:
        bucket = [bucket['Name'] for bucket in list_bucket_name['Buckets']]
        buckets= [s for s in bucket if "martech" in s]
        for bucketname in range(len(buckets)):
            response = s3.get_bucket_tagging(Bucket=buckets[bucketname])
            for i in key:
                if (i not in str(response['TagSet'])):
                    message.append(buckets[bucketname]+ ": Bucket has no "+ i+ " TAG-------> Region = " + Region)
#                    print(buckets[bucketname], ": Bucket has no", i, "TAG")


    except Exception as e:
        message.append(buckets[bucketname]+ ": Bucket has no TAG-------> Region = "+Region)
#        print (buckets[bucketname],": Bucket has no Tags")

EC2_Tagging('us-east-1')
EC2_Tagging('us-east-2')
Lambda_Tagging('us-east-1')
Lambda_Tagging('us-east-2')
S3_Tagging('us-east-1')
S3_Tagging('us-east-2')
DynamoDB_Tagging('us-east-1')
DynamoDB_Tagging('us-east-2')
if message:
	client_sns.publish(TargetArn=targetArn, Message=str(message).replace(',', '\n'), Subject=subject)
	print(str(message).replace(',', '\n'))
else:
	client_sns.publish(TargetArn=targetArn, Message="All AWS Resources Taged well", Subject=subject)
	print(str(message).replace(',', '\n'))