import boto3
import os
import json
import configparser
from datetime import datetime

s3logfolder = "errorlogs01_" + datetime.now().strftime("%Y-%m-%d_%H-%M")
path = "/home/ubuntu/sbmd/logs/"
logfiles = os.listdir(path)

config = configparser.ConfigParser()
config.read("/home/ubuntu/sbmd/dwh.cfg")
s3key = config["AWS"]["KEY"]
s3skey = config["AWS"]["SECRET"]
s3 = boto3.resource("s3", 
		    aws_access_key_id=s3key, 
		    aws_secret_access_key=s3skey)
print("Created creds...")

errordict = {}
for lf in logfiles:
    with open(path + lf, "r") as f:
        pre_list = f.readlines()
    preerrorlist = []
    for p in pre_list:
        if p.lower().find("error:") > -1:
            preerrorlist.append(p)
        if preerrorlist:
            errordict[lf] = preerrorlist
    s3object = s3.Object("sbmdother", s3logfolder + "/" + lf)
    s3object.upload_file(path + lf)
    os.remove(path + lf)

s3_filename = "/errorlog_01_" + datetime.now().strftime("%Y-%m-%d_%H-%M") \
              + ".json"

s3object = s3.Object("sbmdother", s3logfolder + s3_filename)

print("Evaluated and uploaded errorlog files!")

if errordict:
    s3object.put(Body=(bytes(json.dumps(errordict).encode('UTF-8'))))
    print("Errors found and files uploaded!")
    raise Exception(f"Errors logged to {s3_filename}")
