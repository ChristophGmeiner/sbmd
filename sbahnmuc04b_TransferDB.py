import boto3
import configparser
import pandas as pd
import os
import json
import pytictoc
from sqlalchemy import create_engine
import datetime
import logging

t = pytictoc.TicToc()
t.tic()

logpath = "/home/ubuntu/sbmd/logs/"
normlogfilename = "sb04blog_" \
      + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M") + ".log"
logging.basicConfig(filename=logpath+normlogfilename, level=logging.DEBUG)

config = configparser.ConfigParser()
config.read("/home/ubuntu/sbmd/dwh.cfg")

rdsid = config['RDS']['ID1']
rdspw = config["RDS"]["PW"]

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']
    
s3r = boto3.resource("s3")
BUCKET = "sbmd2gmap3"
bucket = s3r.Bucket(BUCKET)
objsr_all = bucket.objects.all()
client = boto3.client("s3")
archivfoldername = str(datetime.date.today()) + "-ArchivGmap/"
response = client.put_object(
        Bucket=BUCKET,
        Body="",
        Key=archivfoldername)
s3res = boto3.resource("s3")

df_list = []

logging.info("Starting gather S3 files...")

s3r_files = []
for o in objsr_all:
    s3r_files.append(o.key)

s3r_files = [x for x in s3r_files if x.find("/") == -1]
    
for f in s3r_files:    
    basefile = f
    result = client.get_object(Bucket=BUCKET, Key=basefile) 
    text = json.loads(result["Body"].read().decode())
    
    if "timestamp" in text.keys():
        base_df = pd.io.json.json_normalize(text, sep="_")
        break
    
logging.info("Finished gathering S3 files.")

copy_source = {"Bucket": BUCKET, "Key": s3r_files[0]}
dest = s3res.Object(BUCKET, archivfoldername + copy_source["Key"])
dest.copy(CopySource=copy_source)
response = s3res.Object(BUCKET, s3r_files[0]).delete()

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
    dest = s3res.Object(BUCKET, archivfoldername + copy_source["Key"])
    dest.copy(CopySource=copy_source)
    response = s3res.Object(BUCKET, file).delete()
    
logging.info("Finished DF")

coln = list(base_df.columns)
coln = [x.lower() for x in coln]
base_df.columns = coln

base_df_filename = str(datetime.date.today()) + "_Gmap_DF.csv"
base_df.to_csv("/home/ubuntu/sbmd/" + base_df_filename, index=False)

s3object = s3res.Object(BUCKET, "CSVs/" + base_df_filename)
s3object.upload_file("/home/ubuntu/sbmd/" + base_df_filename)
os.remove("/home/ubuntu/sbmd/" + base_df_filename)

if (len(s3r_files) != base_df.shape[0]):
    logging.error("Data Quality check failed! Files and DF length not \
                  identical!")

logging.info("Finished Copy")

t.toc()
