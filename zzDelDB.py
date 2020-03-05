import boto3
import configparser
import os
import sys
from datetime import datetime

class Printer():
    """Print things to stdout on one line dynamically"""
    def __init__(self,data):
        sys.stdout.write("\r\x1b[K"+data.__str__())
        sys.stdout.flush()

config = configparser.ConfigParser()
config.read("/home/ubuntu/dwh.cfg")

rdsid = config['RDS']['ID1']

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

client = boto3.client("rds", region_name="eu-central-1")

snn_base = "sbmd-final-snapshot"
snn = snn_base + "-" + datetime.now().strftime("%y-%m-%d-%H-%M")

response = client.delete_db_instance(
        DBInstanceIdentifier=rdsid,
        SkipFinalSnapshot=False,
        FinalDBSnapshotIdentifier=snn,
        DeleteAutomatedBackups=True
        )
        
dbdesc = client.describe_db_instances(DBInstanceIdentifier=rdsid)
dbstate = dbdesc["DBInstances"][0]["DBInstanceStatus"]

while dbstate:
    dbdesc = client.describe_db_instances(DBInstanceIdentifier=rdsid)
    dbstate = dbdesc["DBInstances"][0]["DBInstanceStatus"]
    Printer(dbstate)