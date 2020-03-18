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
    
    drop_table1 = dropsql + table1 + " CASCADE; "
    drop_table2 = dropsql + table2 + " CASCADE; "
    drop_table3 = dropsql + table3 + " CASCADE; "
    drop_table1s = dropsql + table1s + " CASCADE; "
    drop_table2s = dropsql + table2s + " CASCADE; "
    drop_table3s = dropsql + table3s + " CASCADE; "
    
    create_table1 = createsql + table1 + """ (db_id INT IDENTITY(0, 1) 
                    PRIMARY KEY SORTKEY, 
                    start_loc VARCHAR NOT NULL, end_loc VARCHAR NOT NULL, 
                    timestamp TIMESTAMP NOT NULL, departure TIMESTAMP NOT NULL, 
                    arrival TIMESTAMP NOT NULL, transfers INT, 
                    total_time VARCHAR NOT NULL, products VARCHAR, \
                    price VARCHAR, ontime BOOLEAN NOT NULL, 
                    cancelled BOOLEAN NOT NULL, total_delay INT NOT NULL, 
                    delay_departure INT NOT NULL, 
                    delay_arrival INT NOT NULL, dep_date DATE)
                    """
    
    create_table1s = createsql + table1s + """ (details VARCHAR, 
                    departure VARCHAR, arrival VARCHAR,	transfers VARCHAR, 
                    time VARCHAR, products VARCHAR, 	price VARCHAR,
                    ontime VARCHAR,	 canceled VARCHAR, "date" VARCHAR, 
                    _id VARCHAR, "timestamp" VARCHAR, total_delay VARCHAR, 
                    "start" VARCHAR, "end" VARCHAR, 
                    delay_delay_departure VARCHAR,
                    delay_delay_arrival VARCHAR)
                    """
     
    create_table2 = createsql + table2 + """ (gmap_id INT IDENTITY(0, 1) 
                    PRIMARY KEY SORTKEY, 
                    start_loc VARCHAR NOT NULL, end_loc VARCHAR NOT NULL, 
                    timestamp TIMESTAMP NOT NULL, start_loc_lat VARCHAR, 
                    start_loc_lng VARCHAR, start_adress VARCHAR, 
                    end_loc_lat VARCHAR, end_loc_lng VARCHAR, 
                    end_adress VARCHAR, distance_text VARCHAR, 
                    distance_value FLOAT, duration_text VARCHAR, 
                    duration_value FLOAT, duration_traffic_text VARCHAR, 
                    duration_traffic_value FLOAT)
                    """
    
    create_table2s = createsql + table2s + """ (end_address VARCHAR,
                    	start_address VARCHAR, traffic_speed_entry VARCHAR,
                    via_waypoint VARCHAR, stat1 VARCHAR, stat2 VARCHAR, 
                    "timestamp" VARCHAR, distance_text VARCHAR , 
                    distance_value VARCHAR, duration_text VARCHAR, 
                    duration_value VARCHAR, duration_in_traffic_text VARCHAR, 
                    duration_in_traffic_value VARCHAR, 
                    end_location_lat VARCHAR, end_location_lng VARCHAR, 
                    start_location_lat VARCHAR, start_location_lng VARCHAR)
                    """
    
    create_table3 = createsql + table3 + """ (weather_id INT IDENTITY(0, 1) 
                   PRIMARY KEY SORTKEY, 
                   loc_name VARCHAR NOT NULL, loc_id BIGINT NULL, 
                   clouds INT, detailed_status VARCHAR, dewpoint VARCHAR, 
                   heat_index VARCHAR, humidex VARCHAR, humidity INT, 
                   press INT, sea_level INT, rain_1h FLOAT, 
                   rain_3h FLOAT, reference_time TIMESTAMP NOT NULL, 
                   snow_1h FLOAT, snow_3h FLOAT, status VARCHAR, 
                   sunrise_time TIMESTAMP, sunset_time TIMESTAMP, 
                   temperature FLOAT, temperature_kf FLOAT, 
                   temperature_max FLOAT, temperature_min FLOAT, 
                   visibility_distance INT, weather_code INT, 
                   weather_iconname VARCHAR, wind_deg FLOAT, wind_gust FLOAT, 
                   wind_speed FLOAT, reception_time TIMESTAMP NOT NULL)
                   """
                   
    create_table3s = createsql + table3s + """ (location_id VARCHAR, 
                  location_coordinates_lat VARCHAR, 
                  location_coordinates_lon VARCHAR, location_country VARCHAR,
                  location_name VARCHAR, weather_clouds VARCHAR, 
                  weather_detailed_status VARCHAR, weather_dewpoint VARCHAR,
                  weather_heat_index VARCHAR, weather_humidex VARCHAR, 
                  weather_humidity VARCHAR, weather_pressure_press VARCHAR, 
                  weather_pressure_sea_level VARCHAR, weather_rain_1h VARCHAR,
                  weather_rain_3h VARCHAR, weather_reference_time VARCHAR, 
                  weather_snow_1h VARCHAR, weather_snow_3h VARCHAR, 
                  weather_status VARCHAR, weather_sunrise_time VARCHAR,
                  weather_sunset_time VARCHAR, 
                  weather_temperature_temp VARCHAR,
                  weather_temperature_temp_kf VARCHAR, 
                  weather_temperature_temp_max VARCHAR, 
                  weather_temperature_temp_min VARCHAR,
                  weather_visibility_distance VARCHAR, 
                  weather_weather_code VARCHAR, 
                  weather_weather_icon_name VARCHAR,
                  weather_wind_deg VARCHAR,	weather_wind_gust VARCHAR,
                  weather_wind_speed VARCHAR, reception_time VARCHAR)
                   """

    
    createtable_queries = [create_table1, create_table1s,
                           create_table2, create_table2s, 
                           create_table3, create_table3s]
    droptable_queries = [drop_table1s, drop_table1,
                         drop_table2s, drop_table2,
                         drop_table3s, drop_table3]



