import googlemaps
from datetime import datetime
import boto3
import configparser
import os
import pickle
import json

config = configparser.ConfigParser()
config.read("dwh.cfg")

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

s3 = boto3.resource('s3')

with open('gapi.txt') as f:
    api_key = f.readline()
    f.close
    
gmap = googlemaps.Client(key=api_key)

with open("station", "rb") as f:
    fileobj = pickle.load(f)

statconns = fileobj[1]

i = 0

for conns in statconns:
    
    try:

        now = datetime.now()
        nowstring = str(now).replace(":", "_").replace(" ", "_")\
                    .replace(".", "_").replace("-", "_")
        
        results = gmap.directions(conns[0], conns[1],
                                  mode="driving",
                                  departure_time=now)
        
        resdict = results[0]["legs"][0]
        del resdict["steps"]
        
        resdict["stat1"] = conns[0]
        resdict["stat2"] = conns[1]
            
        filename = ("Gmap_" + nowstring + "_" + conns[0] + "_" + conns[1] 
                            + ".json")
        filename = filename.replace(":", "_")
        filename = filename.replace(" ", "_")
        
        s3object = s3.Object("sbmd2gmap", filename)
    
        s3object.put(Body=(bytes(json.dumps(resdict).encode('UTF-8'))))
        
        i += 1
        
        
    except:
        print(str(i) + "failed at " + conns[0] + "-" + conns[1] + "!")
        i += 1
    
    