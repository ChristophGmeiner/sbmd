import boto3
import configparser
import os

config = configparser.ConfigParser()
config.read("/home/ec2-user/sbmd/dwh.cfg")

ec2id = config['EC2']['ID']

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

client = boto3.client("ec2", region_name="eu-central-1")

response = client.start_instances(InstanceIds=[ec2id])
