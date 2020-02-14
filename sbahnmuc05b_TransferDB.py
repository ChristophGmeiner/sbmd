import boto3
import configparser
import pandas as pd
import os
import json
import pytictoc
from sqlalchemy import create_engine
import pickle
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
BUCKET = "sbmd3weather2"
bucket = s3r.Bucket(BUCKET)
objsr_all = bucket.objects.all()
client = boto3.client("s3")
archivfoldername = str(datetime.date.today()) + "-ArchivWeather/"
response = client.put_object(
        Bucket=BUCKET,
        Body="",
        Key=archivfoldername)
s3res = boto3.resource("s3")

i = 0
s3r_files = []
for o in objsr_all:
    i += 1
    s3r_files.append(o.key)
    
highest_ind = i
last_file = s3r_files[-1]
stamplist = [highest_ind, last_file]

with open("stamplistweather_file", "wb") as f:
    pickle.dump(stamplist, f)
    
basefile = s3r_files[0]
result = client.get_object(Bucket=BUCKET, Key=basefile) 
text = json.loads(result["Body"].read().decode())
base_df = pd.io.json.json_normalize(text, sep="_")

copy_source = {"Bucket": BUCKET, "Key": s3r_files[0]}
s3res.Object(BUCKET, archivfoldername).copy_from(copy_source)
s3res.Object(BUCKET, s3r_files[0]).delete()


FILE_TO_READ = s3r_files[0]
client = boto3.client('s3')
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

constring = "postgresql+psycopg2://sbmdmaster:" +rdspw + \
            "@sbmd.cfv4eklkdk8x.eu-central-1.rds.amazonaws.com:5432/sbmd1"
engine = create_engine(constring)
base_df.to_sql('t_w01_stagings', engine, if_exists='replace', 
                        index=False)

client = boto3.client("rds", region_name="eu-central-1")
response = client.stop_db_instance(DBInstanceIdentifier=rdsid)

t.toc()
