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
    
    create_table1s = createsql + table1s + """ (details VARCHAR, 
                    departure VARCHAR, arrival VARCHAR,	transfers INT, 
                    time VARCHAR, products VARCHAR, 	price VARCHAR,
                    ontime BOOLEAN,	 canceled BOOLEAN, "date" DATE, 
                    _id VARCHAR, "timestamp" TIMESTAMP, 	total_delay FLOAT, 
                    "start" VARCHAR, "end" VARCHAR, 
                    delay_delay_departure FLOAT,
                    delay_delay_arrival FLOAT)
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
    
    create_table2s = createsql + table2s + """ (end_address VARCHAR,
                    	start_address VARCHAR, traffic_speed_entry VARCHAR,
                    via_waypoint VARCHAR, stat1 VARCHAR, stat2 VARCHAR, 
                    "timestamp" TIMESTAMP, distance_text VARCHAR , 
                    distance_value FLOAT, duration_text VARCHAR, 
                    duration_value FLOAT, duration_in_traffic_text VARCHAR, 
                    duration_in_traffic_value FLOAT, end_location_lat VARCHAR,
                    end_location_lng VARCHAR, start_location_lat VARCHAR, 
                    start_location_lng VARCHAR)
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
                   
    create_table3s = createsql + table3s + """ (location_id BIGINT, 
                  location_coordinates_lat VARCHAR, 
                  location_coordinates_lon VARCHAR, location_country VARCHAR,
                  location_name VARCHAR, weather_clouds FLOAT, 
                  weather_detailed_status VARCHAR, weather_dewpoint VARCHAR,
                  weather_heat_index VARCHAR, weather_humidex VARCHAR, 
                  weather_humidity FLOAT, weather_pressure_press FLOAT, 
                  weather_pressure_sea_level VARCHAR, weather_rain_1h VARCHAR,
                  weather_rain_3h VARCHAR, weather_reference_time BIGINT, 
                  weather_snow_1h VARCHAR, weather_snow_3h VARCHAR, 
                  weather_status VARCHAR, weather_sunrise_time BIGINT,
                  weather_sunset_time BIGINT, weather_temperature_temp VARCHAR,
                  weather_temperature_temp_kf VARCHAR, 
                  weather_temperature_temp_max VARCHAR, 
                  weather_temperature_temp_min VARCHAR,
                  weather_visibility_distance FLOAT, 
                  weather_weather_code INT, 	weather_weather_icon_name VARCHAR,
                  weather_wind_deg VARCHAR,	weather_wind_gust VARCHAR,
                  weather_wind_speed VARCHAR, reception_time BIGINT)
                   """

    
    createtable_queries = [create_table1, create_table1s,
                           create_table2, create_table2s, 
                           create_table3, create_table3s]
    droptable_queries = [drop_table1, drop_table1s,
                         drop_table2, drop_table2s,
                         drop_table3, drop_table3s]



