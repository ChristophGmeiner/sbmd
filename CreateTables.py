class CreateTables:

    dropsql = "DROP TABLE IF EXISTS "
    createsql = "CREATE TABLE IF NOT EXISTS "
      
    table1 = "t_db01_live"
    table1s = "t_db01_stagings"
    table2 = "t_gmap01_live"
    table2s = "t_gmap01_stagings"
    table3 = "t_w01_live"
    table3s = "t_w01_stagings"
    
    tabs = [table1, table1s,
            table2, table2s,
            table3, table3s]
    
    drop_table1 = dropsql + table1 + "; "
    drop_table2 = dropsql + table2 + "; "
    drop_table3 = dropsql + table3 + "; "
    drop_table1s = dropsql + table1s + "; "
    drop_table2s = dropsql + table2s + "; "
    drop_table3s = dropsql + table3s + "; "
    
    create_table1 = createsql + table1 + """ (db_id INT IDENTITY(0, 1) 
                    PRIMARY KEY SORTKEY, 
                    start_loc VARCHAR NOT NULL, end_loc VARCHAR NOT NULL, 
                    timestamp TIMESTAMP NOT NULL, departure TIMESTAMP NOT NULL, 
                    arrival TIMESTAMP NOT NULL, transfers INT, 
                    total_time TIMESTAMP NOT NULL, products VARCHAR, \
                    price FLOAT, ontime BOOLEAN NOT NULL, 
                    cancelled BOOLEAN NOT NULL, total_delay INT NOT NULL, 
                    delay_departure INT NOT NULL, 
                    delay_arrival INT NOT NULL, dep_date DATE)
                    """
    
    create_table1s = createsql + table1s + """ ("start" VARCHAR, "end" VARCHAR,  
                    "timestamp" TIMESTAMP, departure VARCHAR, arrival VARCHAR, 
                    transfers INT, products VARCHAR, price FLOAT, 
                    ontime BOOLEAN, cancelled BOOLEAN, total_delay INT, 
                    delay_delay_departure INT, delay_delay_arrival INT, 
                    "date" DATE)
                    """
     
    create_table2 = createsql + table2 + """ (gmap_id INT IDENTITY(0, 1) 
                    PRIMARY KEY SORTKEY, 
                    "start" VARCHAR NOT NULL, "end_loc" VARCHAR NOT NULL, 
                    timestamp TIMESTAMP NOT NULL, start_loc_lat FLOAT, 
                    start_loc_lng FLOAT, start_adress VARCHAR, 
                    end_loc_lat FLOAT, end_loc_lng FLOAT, end_adress VARCHAR, 
                    distance_text VARCHAR, distance_value FLOAT, 
                    duration_text VARCHAR, duration_value FLOAT, 
                    duration_traffic_text VARCHAR, 
                    duration_traffic_value FLOAT)
                    """
    
    create_table2s = createsql + table2s + """ (stat1 VARCHAR, "stat2" VARCHAR, 
                    "timestamp" TIMESTAMP, start_location_lat FLOAT, 
                    start_location_lng FLOAT, start_address VARCHAR, 
                    end_location_lat FLOAT, end_location_lng FLOAT, 
                    end_address VARCHAR, distance_text VARCHAR, 
                    distance_value FLOAT, duration_text VARCHAR, 
                    duration_value FLOAT, duration_traffic_text VARCHAR, 
                    duration_traffic_value FLOAT)
                    """
    
    create_table3 = createsql + table3 + """ (weather_id INT IDENTITY(0, 1) 
                   PRIMARY KEY SORTKEY, 
                   loc_name VARCHAR NOT NULL, loc_id FLOAT NULL, 
                   clouds FLOAT, detailed_status VARCHAR, dewpoint VARCHAR, 
                   heat_index VARCHAR, humidex VARCHAR, humidity FLOAT, 
                   press FLOAT, sea_level VARCHAR, rain_1h FLOAT, 
                   rain_3h FLOAT, reference_time FLOAT NOT NULL, 
                   snow_1h FLOAT, snow_3h FLOAT, status VARCHAR, 
                   sunrise_time FLOAT, sunset_time FLOAT, 
                   temperature FLOAT, temperature_kf FLOAT, 
                   temperature_max FLOAT, temperature_min FLOAT, 
                   visibility_distance FLOAT, weather_code INT, 
                   weather_iconname VARCHAR, wind_deg FLOAT, wind_gust FLOAT, 
                   wind_speed FLOAT, reception_time FLOAT NOT NULL)
                   """
                   
    create_table3s = createsql + table3s + """ (location_name VARCHAR, 
                     location_id FLOAT, weather_clouds FLOAT,  
                     weather_detailed_status VARCHAR, weather_dewpoint VARCHAR, 
                     weather_heat_index VARCHAR,  weather_humidex VARCHAR,  
                     weather_humidity FLOAT, weather_pressure_press FLOAT, 
                     weather_pressure_sea_level VARCHAR, weather_rain_1h FLOAT, 
                     weather_rain_3h FLOAT, weather_reference_time FLOAT, 
                     weather_snow_1h FLOAT, weather_snow_3h FLOAT, 
                     weather_status VARCHAR, weather_sunrise_time FLOAT, 
                     weather_sunset_time FLOAT, 
                     weather_temperature_temperature FLOAT, 
                     weather_temperature_temperature_kf FLOAT, 
                     weather_temperature_temp_max FLOAT, 
                     weather_temperature_temp_min FLOAT, 
                     weather_visibility_distance FLOAT, 
                     weather_weather_code INT, 
                     weather_weather_iconname VARCHAR, weather_wind_deg FLOAT, 
                     weather_wind_gust FLOAT, weather_wind_speed FLOAT, 
                     weather_reception_time FLOAT NOT NULL)
                   """

    
    createtable_queries = [create_table1, create_table1s,
                           create_table2, create_table2s, 
                           create_table3, create_table3s]
    droptable_queries = [drop_table1, drop_table1s,
                         drop_table2, drop_table2s,
                         drop_table3, drop_table3s]



