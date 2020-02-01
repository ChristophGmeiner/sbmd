import schiene
import datetime
import json
import configparser
import os
import boto3
import pickle
from pytictoc import TicToc
from sys import stdout

t = TicToc()

t.tic()

config = configparser.ConfigParser()
config.read("dwh.cfg")

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

s3 = boto3.resource('s3')

with open("station", "rb") as f:
    fileobj = pickle.load(f)
    
s = schiene.Schiene()

statit = fileobj[1]

i = 0
for st in statit:
    i +=1
    
allstat = i

i = 0

with open("station", "rb") as f:
    fileobj2 = pickle.load(f)

statconns = fileobj2[1]

faillist = []

for conns in statconns:
    
    try:
    
        c = s.connections(conns[0], conns[1])
        
        for conn in c:
            
            if "delay" in conn.keys():
        
                conn["date"] = str(datetime.date.today())
                conn["_id"] = (str(conn["date"]) + "_" + conn["departure"]
                               + "_" + conns[0] + "_" + conns[1])
                conn["timestamp"] = str(datetime.datetime.now())
                conn["total_delay"] = (conn["delay"]["delay_departure"] 
                    + conn["delay"]["delay_arrival"])
                conn["start"] = conns[0]
                conn["end"] = conns[1]
                
                filename = ("DB_" + conn["_id"] + "_" + conns[0] + "_" 
                            + conns[1] + ".json")
                filename = filename.replace(":", "_")
                filename = filename.replace(" ", "_")
                
                s3object = s3.Object("sbmd1db", filename)
                
                s3object.put(Body=(bytes(json.dumps(conn)\
                                         .encode('UTF-8'))))
                             
                stdout.write("\r%d" % i + " of " +  str(allstat) 
                                + " finshed!")
                stdout.flush()
                
                i += 1
                    

    except Exception as e:
        faillist.append(conns + "_" + e)
        

with open("faillist", "wb") as f:
    pickle.dump(faillist, f)        
          
    
t.toc()