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
config.read("dwh.cfg")
    
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

client = boto3.client("redshift", region_name="eu-central-1")

cid = config["Red"]["DWH_CLUSTER_IDENTIFIER"]

snn_base = "sbmd-final-snapshot"
snn = snn_base + "-" + datetime.now().strftime("%y-%m-%d-%H-%M")

existing_snapshots = client.describe_cluster_snapshots(
        ClusterIdentifier=cid)
esn_list = existing_snapshots["Snapshots"]

final_esn_list = list()
final_esn_list.append("test")
final_esn_list.append(utc.localize(datetime.utcnow() - timedelta(weeks=1000)))
for s in esn_list:
    sn_name = s["SnapshotIdentifier"]
    sn_time = s["SnapshotCreateTime"]

    if sn_name.find(snn_base) > -1 and sn_time > final_esn_list[1]:
        final_esn_list.pop()
        final_esn_list.pop()
        final_esn_list.append(sn_name)
        final_esn_list.append(sn_time)

response = client.restore_from_cluster_snapshot(
        ClusterIdentifier=cid,
        SnapshotIdentifier=final_esn_list[0],
        Port=5439,
        AvailabilityZone="eu-central-1b",
        PubliclyAccessible=True,
        VpcSecurityGroupIds=["sg-00496c38f5351e4ea"])

dbdesc = client.describe_clusters(ClusterIdentifier=cid)
dbstate = dbdesc["Clusters"][0]["ClusterStatus"]

while dbstate != "available":
    dbdesc = client.describe_clusters(ClusterIdentifier=cid)
    dbstate = dbdesc["Clusters"][0]["ClusterStatus"]
    Printer(dbstate)
