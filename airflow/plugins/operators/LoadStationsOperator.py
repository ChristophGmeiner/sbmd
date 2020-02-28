import schiene
import itertools
import pickle
import multiprocessing as mp
from itertools import chain
import numpy as np
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadStationsOperator(BaseOperator):
    
    ui_color = 'blue'
    
    @apply_defaults
    
    def __init__(self,
                aws_creds="",
                savepath="",
                station_list="",
                s_object="",
                s3_path="",
                *args, **kwargs):
        '''
        Operator for gathering station names from schiene object
        : aws_creds - Airflow AWS credentials for S3 bucket
        : savepath - local path to save station file
        : station_list - list of strings, i.e. station names
        : s_object - initiated schiene object
        '''
        super(LoadStationsOperator, self).__init__(*args, **kwargs)
        
        self.aws_creds=aws_creds
        self.savepath = savepath
        self.station_list = station_list
        self.s_object = s_object
        self.s3_path = s3_path
        
        def execeute(self, context):
            
            aws_hook = AwsHook(self.aws_creds)
                        
            def all_station(p, s_object=schiene.Schiene()):
                '''
                creates a list object containing all stations gathered from p
                p: String input on which the schiene instance checks for 
                   station names available
                '''
                    
                slist = s_object.stations(p, limit=30)
                pre_list = []
                for sl in slist:
                    if p in sl["value"] :
                        pre_list.append(sl["value"])
                
                pre_list = list(set(pre_list))
                return(pre_list)   
            
            pool = mp.Pool(mp.cpu_count())
            
            real_stations = pool.map(all_station, [p for p in 
                                                   self.station_list])
            real_stations = list(chain(*real_stations))
            
            pool.close()
            
            real_stations = [x for x in real_stations if x]            
            real_stations = [x for x in real_stations 
                             if x.find(",") == -1]
            real_stations = [x for x in real_stations 
                             if x.find(";") == -1]
            real_stations = [x for x in real_stations 
                             if x.find("Berlin") == -1]
            real_stations = [x for x in real_stations 
                             if x.find("Attnang") == -1]
            real_stations = [x for x in real_stations 
                             if x.find("Konstanz") == -1]
            real_stations = [x for x in real_stations 
                             if x.find("Kindsbach") == -1]
            
            real_stations.remove("Taufkirchen an der Pram")
            real_stations.remove("Steinebach an der Wied Ort")
            real_stations.remove('Mittenwalde b Templin Dorf')
            real_stations.remove("Haarhausen")
            
            stations_iter = itertools.combinations(real_stations, 2)
    
            stations_iter_list = []
    
            for sta in stations_iter:
                stations_iter_list.append(sta)
            
            stations_iter_parts_list = np.array_split(stations_iter_list, 10)
            
            fileobj = [real_stations, stations_iter, stations_iter_parts_list]
            
            with open(self.savepath + "station", "wb") as sf:
                pickle.dump(fileobj, sf)
                
            s3 = aws_hook.get_client_type('s3')
            s3.meta.client.upload_file(self.savepath + "station", self.s3_path, 
                               "station")
            
            
                
            