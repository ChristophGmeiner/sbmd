import boto3
import configparser
import os

client = boto3.client("rds", region_name="eu-central-1")

config = configparser.ConfigParser()
config.read("/home/ec2-user/sbmd/dwh.cfg")

rdsid = config['RDS']['ID1']

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

response = client.start_db_instance(DBInstanceIdentifier=rdsid)