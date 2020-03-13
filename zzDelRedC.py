import boto3
import configparser
import os
import sys
import pytz
from datetime import datetime

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

#steer whether do create a final snappshot on delete
if sys.argv[1] == "with":

    response = client.delete_cluster(
            ClusterIdentifier=cid,
            SkipFinalClusterSnapshot=False,
            FinalClusterSnapshotIdentifier=snn,
            FinalClusterSnapshotRetentionPeriod=7
            )

else:
    
    response = client.delete_cluster(
            ClusterIdentifier=cid,
            SkipFinalClusterSnapshot=True,
            FinalClusterSnapshotRetentionPeriod=1
            )
        
dbdesc = client.describe_clusters(ClusterIdentifier=cid)
dbstate = dbdesc["Clusters"][0]["ClusterStatus"]

while dbstate:
    dbdesc = client.describe_clusters(ClusterIdentifier=cid)
    dbstate = dbdesc["Clusters"][0]["ClusterStatus"]
    Printer(dbstate)
