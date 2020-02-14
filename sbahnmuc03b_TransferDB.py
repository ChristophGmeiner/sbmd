import boto3
import configparser
import pandas as pd
import os
import s3fs
import json
import pytictoc
import psycopg2
import io
from sqlalchemy import create_engine
import sys
import datetime

t = pytictoc.TicToc()
t.tic()

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
dbdesc = client.describe_db_instances(DBInstanceIdentifier=rdsid)
dbstate = dbdesc["DBInstances"][0]["DBInstanceStatus"]

if dbstate != 'available':
    response = client.start_db_instance(DBInstanceIdentifier=rdsid)

    while dbstate != "available":
        dbdesc = client.describe_db_instances(DBInstanceIdentifier=rdsid)
        dbstate = dbdesc["DBInstances"][0]["DBInstanceStatus"]
        Printer(dbstate)
    
s3r = boto3.resource("s3")
BUCKET = "sbmd1db2"
bucket = s3r.Bucket(BUCKET)
objsr_all = bucket.objects.all()
client = boto3.client("s3")

s3r_files = []
for o in objsr_all:
    s3r_files.append(o.key)

s3r_files = [x for x in s3r_files if x.find("/") == -1]
    
basefile = s3r_files[0]
result = client.get_object(Bucket=BUCKET, Key=basefile) 
text = json.loads(result["Body"].read().decode())
base_df = pd.io.json.json_normalize(text, sep="_")

FILE_TO_READ = s3r_files[0]
client = boto3.client('s3')
archivfoldername = str(datetime.date.today()) + "-ArchivDB/"
response = client.put_object(
        Bucket=BUCKET,
        Body="",
        Key=archivfoldername)
s3res = boto3.resource("s3")
copy_source = {"Bucket": BUCKET, "Key": s3r_files[0]}
s3res.Object(BUCKET, archivfoldername).copy_from(copy_source)
s3res.Object(BUCKET, s3r_files[0]).delete()
df_list = []

for file in s3r_files[1:]:
    result = client.get_object(Bucket=BUCKET, Key=file) 
    text = json.loads(result["Body"].read().decode())
    df = pd.io.json.json_normalize(text, sep="_")
    base_df = pd.concat([base_df, df], axis=0, ignore_index=True)
    
    #archiving
    copy_source = {"Bucket": BUCKET, "Key": file}
    s3res.Object(BUCKET, archivfoldername).copy_from(copy_source)
    s3res.Object(BUCKET, file).delete()
        
    
conn = psycopg2.connect(
        host="sbmd.cfv4eklkdk8x.eu-central-1.rds.amazonaws.com", 
        dbname="sbmd1", port=5432, user="sbmdmaster", password=rdspw)

cur = conn.cursor()

output = io.StringIO()
base_df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()

constring = "postgresql+psycopg2://sbmdmaster:" +rdspw + \
            "@sbmd.cfv4eklkdk8x.eu-central-1.rds.amazonaws.com:5432/sbmd1"
engine = create_engine(constring)
base_df.head(0).to_sql('t_db01_stagings', engine, if_exists='replace', 
                        index=False)
cur.copy_from(output, 't_db01_stagings', null="") # null values become ''
conn.commit()
conn.close()

#stop in 05 transfer

#archiving






t.toc()
