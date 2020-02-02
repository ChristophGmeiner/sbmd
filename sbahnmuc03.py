import schiene
import datetime
import json
import configparser
import boto3
import pickle
from pytictoc import TicToc
import multiprocessing as mp

def load_train(start, end, s3key, s3skey, 
               s = schiene.Schiene()):
    '''
    loads connection details from a Schiene object
    start: start of the DB train connection, has to be a string and match a 
           name of Schiene stations
    end: end of the DB train connection, has to be a string and match a 
         name of Schiene stations
    '''
    c = s.connections(start, end)
    
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
            
            s3 = boto3.resource('s3',
                                aws_access_key_id=s3k,
                                aws_secret_access_key= s3skey)
            
            s3object = s3.Object("sbmd1db2", filename)
            
            s3object.put(Body=(bytes(json.dumps(conn)\
                                     .encode('utf-8'))))

def load_trains_all(conns, s3key_p, s3skey_p):
    '''
    runs all load_train queries for provided stations
    conns: iterable containing 2 elements for start and end station
    '''

    try:
        load_train(conns[0], conns[1], s3key_p, s3skey_p)
    
    except Exception as e:
        print("Error at first round for " + conns[0] + "_" + conns[1])
        print(e)
        
        
    try:
        load_train(conns[1], conns[0], s3key_p, s3skey_p)
    
    except Exception as e:
        print("Error at second round for " + conns[0] + "_" + conns[1])
        print(e)
        
def main():

    t = TicToc()

    t.tic()

    with open("station", "rb") as f:
        fileobj = pickle.load(f)

    statit = fileobj[1]

    credfile = "/home/ec2-user/sbmd/dwh.cfg"

    config = configparser.ConfigParser()
    config.read(credfile)

    config = configparser.ConfigParser()
    config.read(credfile)

    s3k = config['AWS']['KEY']
    s3ks = config['AWS']['SECRET']
            
    pool = mp.Pool(mp.cpu_count())
    
    [pool.apply(load_trains_all, args=(co, s3k, s3ks)) for co in statit]
            
    t.toc()
    
if __name__ == "__main__":
    main()