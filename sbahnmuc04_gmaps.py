import googlemaps
from datetime import datetime
import boto3
import configparser
import os
import json
from itertools import combinations
import pytictoc
import multiprocessing as mp

t = pytictoc.TicToc()

t.tic()

config = configparser.ConfigParser()
config.read("dwh.cfg")

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

s3 = boto3.resource('s3')

with open('gapi.txt') as f:
    api_key = f.readline()
    f.close
    
gmap = googlemaps.Client(key=api_key)

maps_stats = ["München-Ost", "München-Pasing", "Fürstenfeldbruck", "Landshut", 
              "Nürnberg", "Augsburg-Rathausplatz", "Rosenheim", 
              "München-Marienplatz"]

statconns = combinations(maps_stats, 2)

pool = mp.Pool(mp.cpu_count())

def gmap_query(start, end):
    '''
    loads
    '''
    now = datetime.now()
    nowstring = str(now).replace(":", "_").replace(" ", "_")\
                .replace(".", "_").replace("-", "_")
    
    results = gmap.directions(start, end,
                              mode="driving",
                              departure_time=now)
    
    resdict = results[0]["legs"][0]
    
    if resdict["steps"]:
        del resdict["steps"]
    
    
    resdict["stat1"] = start
    resdict["stat2"] = end
        
    filename = ("Gmap_" + nowstring + "_" + start + "_" + end 
                        + ".json")
    filename = filename.replace(":", "_")
    filename = filename.replace(" ", "_")
    
    s3object = s3.Object("sbmd2gmap", filename)

    s3object.put(Body=(bytes(json.dumps(resdict).encode('UTF-8'))))

def gmap_query_all(c):
    '''
    runs all gmap queries for provided stations
    c: iterable containing 2 elements for start and stop station
    '''
    
    try:
        
       gmap_query(c[0], c[1])
        
    except Exception as e:
        print("Error at first round for " + c)
        print(e)
        
    try:
        
       gmap_query(c[1], c[0])
        
    except Exception as e:
        print("Error at first round for " + c)
        print(e)
    
pool.map(gmap_query_all, [co for co in statconns])

pool.close()
    
t.toc()