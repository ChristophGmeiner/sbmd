from pymongo import MongoClient
import schiene
import datetime

client = MongoClient("localhost", 27017)
db = client["sbahnmuc"]
tocoll = "to"
db_tocoll = db[tocoll]

fromcoll = "from"
db_fromcoll = db[fromcoll]

#create index on day
#db_tocoll.create_index("date")
#db_fromcoll.create_index("date")

s = schiene.Schiene()
toconnlist = s.connections('Puchheim Bahnhof Nordseite', 'München Trudering')
fromconnlist = s.connections('München Trudering', 'Puchheim Bahnhof Nordseite')


for conn in toconnlist:
    if"delay" in conn.keys():
        
        del conn["details"]
        del conn["transfers"]
        del conn["price"]
        conn["date"] = str(datetime.date.today())
        conn["_id"] = str(conn["date"]) + "_" + conn["departure"]
        conn["timestamp"] = str(datetime.datetime.now())
        conn["total_delay"] = conn["delay"]["delay_departure"] + conn["delay"]["delay_arrival"]
                
        db_tocoll.remove({"_id": conn["_id"]})
        
        db_tocoll.insert_one(conn)



for conn in fromconnlist:
    if"delay" in conn.keys():
        
        del conn["details"]
        del conn["transfers"]
        del conn["price"]
        conn["date"] = str(datetime.date.today())
        conn["_id"] = str(conn["date"]) + "_" + conn["departure"]
        conn["timestamp"] = str(datetime.datetime.now())
        conn["total_delay"] = conn["delay"]["delay_departure"] + conn["delay"]["delay_arrival"]
               
        db_fromcoll.remove({"_id": conn["_id"]})
        
        db_fromcoll.insert_one(conn)





