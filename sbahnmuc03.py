import schiene
import datetime
import json
import configparser
import os
import boto3
import pickle

s3 = "s3://sbahnmunich/"

config = configparser.ConfigParser()
config.read("dwh.cfg")

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

s3 = boto3.resource('s3')

with open("station", "rb") as f:
    fileobj = pickle.load(f)
    
s = schiene.Schiene()

statconns = fileobj[1]

for conns in statconns:
    
    c = s.connections(conns[0], conns[1])
    
    for conn in c:
        
        if "delay" in conn.keys():
    
            conn["date"] = str(datetime.date.today())
            conn["_id"] = str(conn["date"]) + "_" + conn["departure"]
            conn["timestamp"] = str(datetime.datetime.now())
            conn["total_delay"] = (conn["delay"]["delay_departure"] 
                + conn["delay"]["delay_arrival"])
            
            filename = conn["_id"] + "_" + conns[0] + "_" + conns[1] + ".json"
            filename = filename.replace(":", "_")
            filename = filename.replace(" ", "_")
            
            print(conn)
            
            s3object = s3.Object("sbahnmunich", filename)
            
            s3object.put(Body=(bytes(json.dumps(conn).encode('UTF-8'))))
