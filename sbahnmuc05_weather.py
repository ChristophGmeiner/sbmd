import datetime
import configparser
import os
from pytictoc import TicToc
import pyowm
import boto3
import multiprocessing as mp

def load_weather(c, s3=boto3.resource('s3'), 
                 bucket="/home/ec2-user/sbmd/dwh.cfg",
                 owmfile = "/home/ec2-user/sbmd/owm.txt",
                 credfile = "/home/ec2-user/sbmd/dwh.cfg"):
    
        try:
            
            config = configparser.ConfigParser()
            config.read(credfile)
            
            os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
            os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']
            
            #s3key = config["AWS"]["KEY"]
            
            with open(owmfile, "r") as f:
                owmapi = f.readline()

            owm = pyowm.OWM(owmapi) 
    
            obs = owm.weather_at_place(c + ", DE")
            jf = obs.to_JSON()
            
            now = str(datetime.datetime.now()).replace("-", "_")\
                                       .replace(":", "_").replace(" ", "_")
            filename = c + "_" + now + "_" + ".json"
            
            with open ("test/" + filename, "w") as f:
                f.write(jf)
                
            bucket = s3.Bucket("sbmd3weather2")
                
            bucket.upload_file("test/" + filename, filename)
            
        except Exception as e:
            print(c)
            print(e)

def main():

    pool = mp.Pool(mp.cpu_count())
    
    t = TicToc()
    t.tic()
    
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
    
    
                
    pool.map(load_weather, [c for c in cities])
    
    pool.close()
        
    t.toc() 
    
if __name__ == "__main__":
    main()