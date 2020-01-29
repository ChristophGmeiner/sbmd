import schiene
import configparser
import os
import boto3
import itertools
import pickle



s = schiene.Schiene()

potential_stations = ["München", "Puchheim", "Germering", "Fürstenfeldbruck",
                      "Olching", "Gröbenzell", "Aubing", "Lochhausen",
                      "Langwied", "Wolfratshausen", "Starnberg", "Gernlinden",
                      "Maisach", "Mammendorf", "Schöngeising", "Geltendorf",
                      "Buchenau", "Eichenau", "Laim", "Hirschgarten",
                      "Donnersbergerbrücke", "Heimarnplatz", "Hackherbrücke",
                      "Holzkirchen", "Ebersberg", "Grafing", "Haar", 
                      "Zorneding", "Freising", "München Flughafen", 
                      "Rosenheimer Platz", "Rosenheim", "Augsburg", 
                      "Ingolstadt", "Donauwörth", "Unterhachig", "Taufkirchen",
                      "Erding", "Dachau", "Herrsching", "Tutzng", "Feldafing",
                      "Mühldorf", "Deggendorf", "Landsberg", "Landshut",
                      "Nürnberg", "Grafrath", "Gräfelfing", "Freiham",
                      "Moosach", "Markt Schwaben", "Icking", "Westkreuz", 
                      "Planegg", "Stockdorf", "Possenhofen", "Gauting",
                      "Gilching", "Türkenfeld", "Esting", "Petershausen",
                      "Röhrmoos", "Halbergmoos", "Ismaning", 
                      "Unterföhring", "Johanneskirchen", "Daglfing", 
                      "Feldkirchen", "Riem", "Heimstetten", "Poing", "Grub",
                      "Aying", "Vaterstetten", "Baldham", "Steinebach",
                      "Weßling", "Berg am Laim", "Leuchtenbergring", 
                      "Deisenhofen", "Sauerlach", "Otterfing", "Furth", 
                      "Kreuzstraße", "Ottobrunn", "Hohenbrunn",
                      "schleißheim", "Eching", "Neufahrn", "Altomünster",
                      "Schwabhausen", "Karlsfeld", "Allach", "menzing"]

real_stations = []
for p in potential_stations:
    slist = s.stations(p)
    
    for sl in slist:
        #print(s["value"])
        if sl["value"] not in real_stations:
            real_stations.append(sl["value"])
            
stations_iter = itertools.combinations(real_stations, 2)

fileobj = [real_stations, stations_iter]

with open("station", "wb") as sf:
    pickle.dump(fileobj, sf)
