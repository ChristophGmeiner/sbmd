import datetime
import configparser
import os
from pytictoc import TicToc
import pyowm
import boto3
import multiprocessing as mp

pool = mp.Pool(mp.cpu_count())

t = TicToc()
t.tic()

config = configparser.ConfigParser()
config.read("dwh.cfg")

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

s3key = config["AWS"]["KEY"]

with open("owm.txt", "r") as f:
    owmapi = f.readline()

owm = pyowm.OWM(owmapi) 

s3 = boto3.resource("s3")
bucket = s3.Bucket("sbmd3weather")

cities = ["München", "Puchheim", "Germering", "Fürstenfeldbruck", "Gersthofen",
          "Olching", "Gröbenzell", "Murnau", "Miesbach",
          "Wolfratshausen", "Starnberg", "Gernlinden", "Eichstätt",
          "Maisach", "Mammendorf", "Schöngeising", "Geltendorf",
          "Buchenau", "Eichenau", "Holzkirchen", "Ebersberg", "Grafing", 
          "Zorneding", "Freising", "Haar", "Wasserburg am Inn", "Mittenwald",
          "Rosenheim", "Augsburg", "Geretsried", "Waldkraiburg",
          "Ingolstadt", "Donauwörth", "Unterhaching", "Taufkirchen",
          "Erding", "Dachau", "Tutzing", "Feldafing", "Schrobenhausen",
          "Mühldorf am Inn", "Deggendorf", "Landsberg", "Landshut",
          "Nürnberg", "Grafrath", "Gräfelfing", "Garmisch-Partenkirchen",
          "Markt Schwaben", "Icking", "Kempten", "Schliersee",
          "Planegg", "Stockdorf", "Gauting",
          "Gilching", "Türkenfeld", "Petershausen",
          "Röhrmoos", "Hallbergmoos", "Ismaning", "Bayrischzell",
          "Unterföhring", "Daglfing", "Unterschleißheim",
          "Heimstetten", "Tegernsee", "Lenggries",
          "Aying", "Vaterstetten", "Baldham", "Steinebach",
          "Weßling", "Deisenhofen", "Sauerlach", "Otterfing", 
          "Kreuzstraße", "Ottobrunn", "Hohenbrunn",
          "Oberschleißheim", "Eching", "Neufahrn", "Altomünster",
          "Schwabhausen", "Karlsfeld", "Kolbermoor", "Bad Aibling"]

def load_weather(c):
    
        try:
    
            obs = owm.weather_at_place(c + ", DE")
            jf = obs.to_JSON()
            
            now = str(datetime.datetime.now()).replace("-", "_")\
                                       .replace(":", "_").replace(" ", "_")
            filename = c + "_" + now + "_" + ".json"
            
            with open ("test/" + filename, "w") as f:
                f.write(jf)
                
            bucket.upload_file("test/" + filename, filename)
            
        except Exception as e:
            print(c)
            print(e)
            
pool.map(load_weather, [c for c in cities])

pool.close()
    
t.toc() 