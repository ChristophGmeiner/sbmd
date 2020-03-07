import schiene
import itertools
import configparser
import boto3
import pickle
import pytictoc
import multiprocessing as mp
from itertools import chain
import numpy as np
import logging
import datetime

logpath = "/home/ubuntu/sbmd/logs/"
normlogfilename = "sb02log_" \
              + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M") + ".log"
logging.basicConfig(filename=logpath+normlogfilename, level=logging.DEBUG)

def all_station(p, s_object=schiene.Schiene()):
    '''
    creates a list object containing all stations gathered from p
    p: String input on which the schiene instance checks for station names
       available
    '''
        
    slist = s_object.stations(p, limit=30)
    pre_list = []
    for sl in slist:
        if p in sl["value"] :
            pre_list.append(sl["value"])
    
    pre_list = list(set(pre_list))
    return(pre_list)   
    
def main():
    
    pool = mp.Pool(mp.cpu_count())
    
    t = pytictoc.TicToc()
    
    t.tic()
    
    potential_stations = ["München", "Puchheim", "Germering", 
                          "Fürstenfeldbruck", "Olching", "Gröbenzell", 
                          "Wolfratshausen", "Starnberg", "Gernlinden", 
                          "Maisach", "Mammendorf", "Schöngeising", 
                          "Geltendorf", "Buchenau", "Eichenau", "Murnau",
                          "Hackherbrücke", "Holzkirchen", "Ebersberg", 
                          "Grafing",  "Haar",  "Zorneding", "Freising", 
                          "Rosenheim", "Augsburg", "Miesbach", "Eichstätt",
                          "Ingolstadt", "Donauwörth", "Unterhaching", 
                          "Geretsried", "Taufkirchen", "Erding", "Dachau", 
                          "Steinebach", "Tutzing", "Feldafing", 
                          "Mühldorf am Inn", "Deggendorf", "Landsberg", 
                          "Landshut", "Nürnberg", "Grafrath", "Gräfelfing", 
                          "Markt Schwaben", "Icking", "Kempten", "Planegg", 
                          "Stockdorf", "Possenhofen", "Gauting", "Gilching", 
                          "Türkenfeld", "Petershausen", "Röhrmoos", 
                          "Hallbergmoos", "Ismaning", "Bayrischzell",
                          "Unterföhring", "Daglfing", "Unterschleißheim",
                          "Heimstetten", "Tegernsee", "Lenggries",
                          "Aying", "Vaterstetten", "Baldham", "Steinebach",
                          "Weßling", "Deisenhofen", "Sauerlach", "Otterfing", 
                          "Kreuzstraße", "Ottobrunn", "Hohenbrunn", 
                          "Mittenwald", "Oberschleißheim", "Eching", 
                          "Neufahrn", "Altomünster", "Schwabhausen", 
                          "Kolbermoor", "Bad Aibling", "Wasserburg am Inn", 
                          "Waldkraiburg", "Schrobenhausen",
                          "Garmisch-Partenkirchen", "Schliersee", "Gersthofen"]
         
       
    real_stations = pool.map(all_station, [p for p in potential_stations])
    real_stations = list(chain(*real_stations))
    
    pool.close()
    
    real_stations = [x for x in real_stations if x]            
    real_stations = [x for x in real_stations if x.find(",") == -1]
    real_stations = [x for x in real_stations if x.find(";") == -1]
    real_stations = [x for x in real_stations if x.find("Berlin") == -1]
    real_stations = [x for x in real_stations if x.find("Attnang") == -1]
    real_stations = [x for x in real_stations if x.find("Konstanz") == -1]
    real_stations = [x for x in real_stations if x.find("Kindsbach") == -1]
    
    real_stations.remove("Taufkirchen an der Pram")
    real_stations.remove("Steinebach an der Wied Ort")
    real_stations.remove('Mittenwalde b Templin Dorf')
    real_stations.remove("Haarhausen")
    
    add_stats = ["Puchheim Bahnhof Alpenstraße, Puchheim", 
                 "Bahnhofstraße, Eichenau", "Buchenau, Fürstenfeldbruck",
                 "Bahnhof, Olching", "Am Zillerhof, Gröbenzell"]
    
    for add in add_stats:
        real_stations.append(add)
                
    stations_iter = itertools.combinations(real_stations, 2)
    
    stations_iter_list = []
    
    for sta in stations_iter:
        stations_iter_list.append(sta)
        
    stations_iter_parts_list = np.array_split(stations_iter_list, 10)
    
    fileobj = [real_stations, stations_iter, stations_iter_parts_list]
    
    with open("/home/ubuntu/sbmd/station", "wb") as sf:
        pickle.dump(fileobj, sf)
        
    credfile = "/home/ubuntu/sbmd/dwh.cfg"

    config = configparser.ConfigParser()
    config.read(credfile)

    s3k = config['AWS']['KEY']
    s3ks = config['AWS']['SECRET']
    
    s3 = boto3.resource('s3',
                aws_access_key_id=s3k,
                aws_secret_access_key= s3ks)
    s3.meta.client.upload_file("/home/ubuntu/sbmd/station", "sbmdother", 
                               "station")
    
    logging.info("Stations gathered succesfully!")
    logging.info(t.toc())
    
if __name__ == "__main__":
    main()