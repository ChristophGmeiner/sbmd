import schiene
import datetime
import json
import configparser
import os
import boto3
import pickle
from pytictoc import TicToc
import multiprocessing as mp

def load_train(start, end, s = schiene.Schiene(), s3 = boto3.resource('s3'),
               credfile = "/home/ec2-user/sbmd/dwh.cfg"):
    '''
    loads connection details from a Schiene object
    start: start of the DB train connection, has to be a string and match a 
           name of Schiene stations
    end: end of the DB train connection, has to be a string and match a 
         name of Schiene stations
    '''
    
    c = s.connections(start, end)
    
    config = configparser.ConfigParser()
    config.read("/home/ec2-user/sbmd/dwh.cfg")
    
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']
    
    for conn in c:
    
        if "delay" in conn.keys():
    
            conn["date"] = str(datetime.date.today())
            conn["_id"] = (str(conn["date"]) + "_" + conn["departure"]
                           + "_" + start + "_" + end)
            conn["timestamp"] = str(datetime.datetime.now())
            conn["total_delay"] = (conn["delay"]["delay_departure"] 
                + conn["delay"]["delay_arrival"])
            conn["start"] = start
            conn["end"] = end
            
            filename = "DB_" + conn["_id"] + ".json"
            filename = filename.replace(":", "_")
            filename = filename.replace(" ", "_")
            
            s3object = s3.Object("sbmd1db", filename)
            
            s3object.put(Body=(bytes(json.dumps(conn)\
                                     .encode('UTF-8'))))

def load_trains_all(conns):
    '''
    runs all load_train queries for provided stations
    conns: iterable containing 2 elements for start and end station
    '''
    
    try:
        load_train(conns[0], conns[1])
    
    except Exception as e:
        print("Error at first round for " + conns[0] + "_" + conns[1])
        print(e)
        
    try:
        load_train(conns[1], conns[0])
    
    except Exception as e:
        print("Error at second round for " + conns[0] + "_" + conns[1])
        print(e)
        
def main():

    t = TicToc()
    
    t.tic()
    
    with open("station", "rb") as f:
        fileobj = pickle.load(f)
    
    statit = fileobj[1]
    
    
            
    pool = mp.Pool(mp.cpu_count())
    
    pool.map(load_trains_all, [co for co in statit])
            
    t.toc()
    
if __name__ == "__main__":
    main()