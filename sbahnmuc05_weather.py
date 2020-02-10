import datetime
import configparser
from pytictoc import TicToc
import pyowm
import boto3
import multiprocessing as mp

def load_weather(c, s3key, s3skey, 
                 owmfile = "/home/ec2-user/sbmd/owm.txt"):
    
        try:
            
            
            with open(owmfile, "r") as f:
                owmapi = f.readline()

            owmapi = owmapi.replace("\n", "")

            owm = pyowm.OWM(owmapi) 
    
            obs = owm.weather_at_place(c + ", DE")
            jf = obs.to_JSON()
            
            now = str(datetime.datetime.now()).replace("-", "_")\
                                       .replace(":", "_").replace(" ", "_")
            filename = c + "_" + now + "_" + ".json"
            
            with open (filename, "w") as f:
                f.write(jf)
                
            s3 = boto3.resource('s3',
                         aws_access_key_id=s3key,
                         aws_secret_access_key= s3skey)
                
            bucket = s3.Bucket("sbmd3weather2")
                
            bucket.upload_file(filename, filename)
            
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
    
    credfile = "/home/ec2-user/sbmd/dwh.cfg"
    config = configparser.ConfigParser()
    config.read(credfile)
            
    s3k = config['AWS']['KEY']
    s3ks = config['AWS']['SECRET']
                
    [pool.apply(load_weather, args=(c, s3k, s3ks)) for c in cities]
    
    pool.close()
        
    t.toc() 
    
if __name__ == "__main__":
    main()
