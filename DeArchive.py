import boto3
import os
import configparser

cfg = "/home/ec2-user/sbmd/dwh.cfg"

config = configparser.ConfigParser()
config.read(cfg)

rdsid = config['RDS']['ID1']
rdspw = config["RDS"]["PW"]

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

s3res = boto3.resource("s3")

###Enter relevant bucket below
s3r = boto3.resource("s3")
BUCKET = "sbmd1db2"
bucket = s3r.Bucket(BUCKET)
objsr_all = bucket.objects.all()
client = boto3.client("s3")

###Enter relevant archive folder below
archivname = "2020-03-03-ArchivDB/"

s3r_files = []
for o in objsr_all:
    if o.key.find(archivname) > -1:
        s3r_files.append(o.key)

s3r_files = s3r_files[1:]

###Enter specific index for only gathering the filename 
###as key below
for file in s3r_files[1:]:
    #archiving back
    copy_source = {"Bucket": BUCKET, "Key": file}
    dest = s3res.Object(BUCKET, file[20:])
    dest.copy(CopySource=copy_source)
    response = s3res.Object(BUCKET, file).delete()

