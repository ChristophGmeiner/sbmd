import boto3
import configparser
import os

config = configparser.ConfigParser()
config.read("dwh.cfg")

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

client = boto3.client("ec2", region_name="us-west-2")

response = client.stop_instances(InstanceIds=["i-00d6d547d2e51ca98"])