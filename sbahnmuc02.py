import schiene
import itertools
import pickle
import pytictoc
import multiprocessing as mp
from itertools import chain

pool = mp.Pool(mp.cpu_count())

t = pytictoc.TicToc()

t.tic()

s = schiene.Schiene()

potential_stations = ["München", "Puchheim", "Germering", "Fürstenfeldbruck",
                      "Olching", "Gröbenzell", "Wolfratshausen", "Starnberg", 
                      "Gernlinden", "Maisach", "Mammendorf", "Schöngeising", 
                      "Geltendorf", "Buchenau", "Eichenau", "Murnau",
                      "Hackherbrücke", "Holzkirchen", "Ebersberg", "Grafing", 
                      "Haar",  "Zorneding", "Freising", 
                      "Rosenheim", "Augsburg", "Miesbach", "Eichstätt",
                      "Ingolstadt", "Donauwörth", "Unterhaching", "Geretsried",
                      "Taufkirchen", "Erding", "Dachau", "Steinebach", 
                      "Tutzing", "Feldafing", "Mühldorf am Inn", "Deggendorf", 
                      "Landsberg", "Landshut", "Nürnberg", "Grafrath", 
                      "Gräfelfing", "Markt Schwaben", "Icking", "Kempten",
                      "Planegg", "Stockdorf", "Possenhofen", "Gauting",
                      "Gilching", "Türkenfeld", "Petershausen",
                      "Röhrmoos", "Hallbergmoos", "Ismaning", "Bayrischzell",
                      "Unterföhring", "Daglfing", "Unterschleißheim",
                      "Heimstetten", "Tegernsee", "Lenggries",
                      "Aying", "Vaterstetten", "Baldham", "Steinebach",
                      "Weßling", "Deisenhofen", "Sauerlach", "Otterfing", 
                      "Kreuzstraße", "Ottobrunn", "Hohenbrunn", "Mittenwald",
                      "Oberschleißheim", "Eching", "Neufahrn", "Altomünster",
                      "Schwabhausen", "Kolbermoor", "Bad Aibling",
                      "Wasserburg am Inn", "Waldkraiburg", "Schrobenhausen",
                      "Garmisch-Partenkirchen", "Schliersee", "Gersthofen"]

def all_station(p):
        
    slist = s.stations(p, limit=30)
    pre_list = []
    for sl in slist:
        if p in sl["value"] :
            pre_list.append(sl["value"])
    
    pre_list = list(set(pre_list))
    return(pre_list)        
   
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
            
stations_iter = itertools.combinations(real_stations, 2)

fileobj = [real_stations, stations_iter]

with open("station", "wb") as sf:
    pickle.dump(fileobj, sf)

t.toc()