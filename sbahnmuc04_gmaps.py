import googlemaps
from datetime import datetime
import boto3
import configparser
import os
import json
from itertools import combinations
import pytictoc
import multiprocessing as mp

def gmap_query(start, end, 
               keyfile="home/ec2-user/sbmd/gapi.txt", 
               s3=boto3.resource('s3'), 
               credfile="/home/ec2-user/sbmd/dwh.cfg"):
    '''
    loads direction details of a Google Maps direction object, mode is always
    set to driving, departure time is always current time and no other
    restrictions are set
    start: start of the direction, can be either a string or long/lat
    end: end of the direction, can be either a string or long/lat
    '''
    
    with open(keyfile) as f:
        api_key = f.readline()
        f.close
    
    config = configparser.ConfigParser()
    config.read(credfile)

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']
    
    now = datetime.now()
    nowstring = str(now).replace(":", "_").replace(" ", "_")\
                .replace(".", "_").replace("-", "_")
    
    gmap = googlemaps.Client(api_key)
    
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
    
    s3object = s3.Object("sbmd2gmap3", filename)

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
        print("Error at second round for " + c)
        print(e)
        
def main():

    t = pytictoc.TicToc()
    
    t.tic()

    maps_stats = ["München-Ost", "München-Pasing", "Fürstenfeldbruck", 
                  "Landshut", "Nürnberg", "Augsburg-Rathausplatz", "Rosenheim", 
                  "München-Marienplatz"]

    statconns = combinations(maps_stats, 2)

    pool = mp.Pool(mp.cpu_count())

    pool.map(gmap_query_all, [co for co in statconns])

    pool.close()
    
    t.toc()
    
if __name__ == "__main__":
    main()