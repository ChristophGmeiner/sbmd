import googlemaps
from datetime import datetime
import pytz
import boto3
import configparser
import json
from itertools import combinations
import pytictoc
import multiprocessing as mp
import logging
    
logpath = "/home/ubuntu/sbmd/logs/"
normlogfilename = "sb04log_" \
              + datetime.now().strftime("%Y-%m-%d_%H-%M") + ".log"
logging.basicConfig(filename=logpath+normlogfilename, level=logging.DEBUG)

def gmap_query(start, end, s3key, s3skey, api_key):
    '''
    loads direction details of a Google Maps direction object, mode is always
    set to driving, departure time is always current time and no other
    restrictions are set
    start: start of the direction, can be either a string or long/lat
    end: end of the direction, can be either a string or long/lat
    s3key: AWS Access Key
    s3skey: AWS SECRET Access Key
    api_key: Google Maps API Key
    '''
    
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
    resdict["timestamp"] = str(datetime.now(tz=pytz.timezone("Europe/Berlin")))
        
    filename = ("Gmap_" + nowstring + "_" + start + "_" + end 
                        + ".json")
    filename = filename.replace(":", "_")
    filename = filename.replace(" ", "_")
    
    s3 = boto3.resource('s3',
                         aws_access_key_id=s3key,
                         aws_secret_access_key= s3skey)
    
    s3object = s3.Object("sbmd2gmap3", filename)

    s3object.put(Body=(bytes(json.dumps(resdict).encode('UTF-8'))))

def gmap_query_all(c, s3key_p, s3skey_p, api_key_p):
    '''
    runs all gmap queries for provided stations
    c: iterable containing 2 elements for start and stop station
    s3key_p: AWS Access Key
    s3skey_p: AWS SECRET Access Key
    api_key_p: Google Maps API Key
    '''
    
    try:
        
       gmap_query(c[0], c[1], s3key_p, s3skey_p, api_key_p)
        
    except Exception as e:
        logging.error("Error at first round for " + c[0])
        logging.error(e)
        
    try:
        
       gmap_query(c[1], c[0], s3key_p, s3skey_p, api_key_p)
        
    except Exception as e:
        logging.error("Error at second round for " + c[1])
        logging.error(e)
        
def main():

    t = pytictoc.TicToc()
    
    t.tic()

    maps_stats = ["München-Ost", "München-Pasing", "Fürstenfeldbruck", 
                  "Landshut", "Nürnberg", "Augsburg-Rathausplatz", "Rosenheim", 
                  "München-Marienplatz"]

    statconns = combinations(maps_stats, 2)
    
    keyfile = "/home/ubuntu/sbmd/gapi.txt"
    with open(keyfile) as f:
        ak= f.readline()
        f.close
    
    credfile="/home/ubuntu/sbmd/dwh.cfg"
    config = configparser.ConfigParser()
    config.read(credfile)

    s3k = config['AWS']['KEY']
    s3ks = config['AWS']['SECRET']

    pool = mp.Pool(mp.cpu_count())

    [pool.apply(gmap_query_all, args=(co, s3k, s3ks, ak)) for co in statconns]

    pool.close()
    
    t.toc()
    
if __name__ == "__main__":
  
    main()
    logging.info("Gmap Data loaded succesfull!")
