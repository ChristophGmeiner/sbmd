import schiene
import configparser
import os
import boto3
import itertools
import pickle



s = schiene.Schiene()

potential_stations = ["München", "Puchheim", "Germering", "Fürstenfeldbruck",
                      "Olching", "Gröbenzell", 
                      "Wolfratshausen", "Starnberg", "Gernlinden",
                      "Maisach", "Mammendorf", "Schöngeising", "Geltendorf",
                      "Buchenau", "Eichenau", "Heimeranplatz", "Hackherbrücke",
                      "Holzkirchen", "Ebersberg", "Grafing", "Haar", 
                      "Zorneding", "Freising", "München Flughafen", 
                      "Rosenheimer Platz", "Rosenheim", "Augsburg", 
                      "Ingolstadt", "Donauwörth", "Unterhachig", "Taufkirchen",
                      "Erding", "Dachau", "Herrsching", "Tutzng", "Feldafing",
                      "Mühldorf am Inn", "Deggendorf", "Landsberg", "Landshut",
                      "Nürnberg", "Grafrath", "Gräfelfing",
                      "Markt Schwaben", "Icking", "Kempten",
                      "Planegg", "Stockdorf", "Possenhofen", "Gauting",
                      "Gilching", "Türkenfeld", "Esting", "Petershausen",
                      "Röhrmoos", "Halbergmoos", "Ismaning", "Bayrischzell",
                      "Unterföhring", "Daglfing", 
                      "Heimstetten", "Poing", "Tegernsee", "Lenggriess",
                      "Aying", "Vaterstetten", "Baldham", "Steinebach",
                      "Weßling", "Deisenhofen", "Sauerlach", "Otterfing", 
                      "Kreuzstraße", "Ottobrunn", "Hohenbrunn",
                      "schleißheim", "Eching", "Neufahrn", "Altomünster",
                      "Schwabhausen", "Karlsfeld", "Kolbermoor", "Bad Aibling"]

real_stations = []
for p in potential_stations:
    slist = s.stations(p, limit=30)
    
    for sl in slist:
        #print(s["value"])
        if sl["value"] not in real_stations and p in sl["value"] :
            real_stations.append(sl["value"])
            
real_stations = [x for x in real_stations if x.find(",") == -1]
real_stations = [x for x in real_stations if x.find(";") == -1]
real_stations = [x for x in real_stations if x.find("Berlin") == -1]
real_stations = [x for x in real_stations if x.find("Attnang") == -1]
            
stations_iter = itertools.combinations(real_stations, 2)

fileobj = [real_stations, stations_iter]

with open("station", "wb") as sf:
    pickle.dump(fileobj, sf)
