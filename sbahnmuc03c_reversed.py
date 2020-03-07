import sys
import schiene
import datetime
import pytz
import json
import configparser
import boto3
import pickle
from pytictoc import TicToc
import multiprocessing as mp
import logging
import numpy as np

logpath = "/home/ubuntu/sbmd/logs/"
normlogfilename = "sb03clog_" + sys.argv[1] + "_" \
              + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M") + ".log"
logging.basicConfig(filename=logpath+normlogfilename, level=logging.DEBUG)

def load_train(start, end, s3key, s3skey, 
               s = schiene.Schiene()):
    '''
    loads connection details from a Schiene object
    start: start of the DB train connection, has to be a string and match a 
           name of Schiene stations
    end: end of the DB train connection, has to be a string and match a 
         name of Schiene stations
    s3key: AWS Access Key
    s3skey: AWS Secret Access Key,
    s: schiene instance
    '''
    c = s.connections(start, end)
    
    for conn in c:
    
        if "delay" in conn.keys():
    
            conn["date"] = str(datetime.date.today())
            conn["_id"] = (str(conn["date"]) + "_" + conn["departure"]
                           + "_" + start + "_" + end)
            conn["timestamp"] = str(datetime.datetime.now(
                                            tz=pytz.timezone("Europe/Berlin")))
            conn["total_delay"] = (conn["delay"]["delay_departure"] 
                + conn["delay"]["delay_arrival"])
            conn["start"] = start
            conn["end"] = end
            
            filename = "DB_" + conn["_id"] + ".json"
            filename = filename.replace(":", "_")
            filename = filename.replace(" ", "_")
            
            s3 = boto3.resource('s3',
                                aws_access_key_id=s3key,
                                aws_secret_access_key= s3skey)
            
            s3object = s3.Object("sbmd1db2", filename)
            
            s3object.put(Body=(bytes(json.dumps(conn)\
                                     .encode('utf-8'))))

def load_trains_all(conns, s3key_p, s3skey_p):
    '''
    runs all load_train queries for provided stations
    conns: iterable containing 2 elements for start and end station
    s3key_p: AWS Access key
    s3skey_p: AWS Secret Access Key
    '''

    try:
        load_train(conns[0], conns[1], s3key_p, s3skey_p)
    
    except Exception as e:
        logging.error("Error at first round for " + conns[0] + "_" + conns[1])
        logging.error(e)
        
        
    try:
        load_train(conns[1], conns[0], s3key_p, s3skey_p)
    
    except Exception as e:
        logging.error("Error at second round for " + conns[0] + "_" + conns[1])
        logging.error(e)
        
def main():

    t = TicToc()

    t.tic()

    with open("/home/ubuntu/sbmd/station", "rb") as f:
        fileobj = pickle.load(f) 
	
    statit = fileobj[2][int(sys.argv[1])]
    statit = np.flip(statit)

    credfile = "/home/ubuntu/sbmd/dwh.cfg"

    config = configparser.ConfigParser()
    config.read(credfile)

    s3k = config['AWS']['KEY']
    s3ks = config['AWS']['SECRET']
            
    pool = mp.Pool(mp.cpu_count())
    
    [pool.apply(load_trains_all, args=(co, s3k, s3ks)) for co in statit]
    
    ind = sys.argv[1]
    logging.info(f"Gathered conn data succesfully with index {ind}")
            
    t.toc()
    
if __name__ == "__main__":
    main()