import boto3
import configparser
import os
import sys
from datetime import datetime, timedelta
import pytz

utc=pytz.UTC

class Printer():
    """Print things to stdout on one line dynamically"""
    def __init__(self,data):
        sys.stdout.write("\r\x1b[K"+data.__str__())
        sys.stdout.flush()

config = configparser.ConfigParser()
config.read("/home/ec2-user/sbmd/dwh.cfg")

rdsid = config['RDS']['ID1']
rdspw = config["RDS"]["PW"]

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

client = boto3.client("rds", region_name="eu-central-1")

snn_base = "sbmd-final-snapshot"
snn = snn_base + "-" + datetime.now().strftime("%y-%m-%d-%H-%M")

existing_snapshots = client.describe_db_snapshots(
        DBInstanceIdentifier=rdsid)
esn_list = existing_snapshots["DBSnapshots"]

final_esn_list = [utc.localize(datetime.utcnow() - timedelta(weeks=1000))]
for s in esn_list:
    sn_name = s["DBSnapshotIdentifier"]
    sn_time = s["SnapshotCreateTime"]
    
    if final_esn_list:
        if sn_name.find(snn_base) > -1 and sn_time > final_esn_list[0]:
            final_esn_list.pop()
            final_esn_list.append(sn_name)

response = client.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=rdsid,
        DBSnapshotIdentifier=final_esn_list[0],
        DBInstanceClass="db.m5.4xlarge",
        Port=5432,
        AvailabilityZone="eu-central-1b",
        PubliclyAccessible=True,
        Engine="postgres",
        VpcSecurityGroupIds=["sg-0010016f8c9f15a2d"],
        DeletionProtection=False)

dbdesc = client.describe_db_instances(DBInstanceIdentifier=rdsid)
dbstate = dbdesc["DBInstances"][0]["DBInstanceStatus"]

while dbstate != "available":
    dbdesc = client.describe_db_instances(DBInstanceIdentifier=rdsid)
    dbstate = dbdesc["DBInstances"][0]["DBInstanceStatus"]
    Printer(dbstate)
