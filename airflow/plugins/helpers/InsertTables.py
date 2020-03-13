class InsertTables:

    delsql1 = """
              DELETE FROM t_db01_stagings
              WHERE details = 'details';
    
              DELETE FROM t_db01_live
                WHERE departure || '_' || start_loc || '_' || end_loc
                 IN
                 (SELECT DISTINCT
                 CAST("date" || ' ' || departure AS TIMESTAMP) || '_' || "start" || '_' || "end"
                 FROM t_db01_stagings);
              """
    
    delsql2 = """
                DELETE FROM t_gmap01_stagings
                WHERE end_address = 'end_address';
    
                DELETE FROM t_gmap01_live
                WHERE CAST("timestamp" AS TIMESTAMP) || '_' || start_loc 
                || '_' || end_loc
                IN
                    (SELECT DISTINCT
                        CAST("timestamp" AS TIMESTAMP) || '_' || stat1 || '_' 
                        || stat2
                    FROM t_gmap01_stagings);
              """
    
    delsql3 = """
              DELETE FROM t_w01_stagings
              WHERE location_id = 'location_id';
            
              DELETE FROM t_w01_live
              WHERE reception_time || '_' || loc_name IN
                  (SELECT DISTINCT 
                     reception_time || '_' || location_name
              FROM
              t_w01_stagings);
              """
    inssql1 = """
                INSERT INTO t_db01_live(
                    start_loc, 
                    end_loc, 
                    "timestamp", 
                    departure, 
                    arrival, 
                    transfers, 
                    total_time, 
                    products, 
                    price, 
                    ontime, 
                    cancelled, 
                    total_delay, 
                    delay_departure, 
                    delay_arrival,
                    dep_date)
                SELECT DISTINCT
                    "start",
                    "end",
                    CAST("timestamp" AS TIMESTAMP),
                    CAST("date" || ' ' || departure AS TIMESTAMP) AS departure,
                    CAST("date" || ' ' || arrival AS TIMESTAMP) AS arrival,
                    CAST(transfers AS INT),
                    CAST("date" || ' ' || arrival AS TIMESTAMP) -  CAST("date" || ' ' || departure AS TIMESTAMP),
                    products,
                    price,
                    CASE WHEN ontime = 'True' THEN 1 ELSE 0 END, 
                    CASE WHEN canceled = 'True' THEN 1 ELSE 0 END,
                    CAST(total_delay AS INT),
                    CAST(delay_delay_departure AS INT),
                    CAST(delay_delay_arrival AS INT),
                    CAST("date" AS DATE)
                FROM
                t_db01_stagings;
             """
    
    inssql2 = """
              INSERT INTO t_gmap01_live(
                    start_loc, 
                    end_loc, 
                    "timestamp", 
                    start_loc_lat, 
                    start_loc_lng, 
                    start_adress, 
                    end_loc_lat, 
                    end_loc_lng, 
                    end_adress, 
                    distance_text, 
                    distance_value, 
                    duration_text, 
                    duration_value, 
                    duration_traffic_text, 
                    duration_traffic_value)
                SELECT DISTINCT
                    stat1,
                    stat2,
                    CAST("timestamp" AS TIMESTAMP),
                    start_location_lat,
                    start_location_lng,
                    start_address,
                    end_location_lat,
                    end_location_lat,
                    end_address,
                    distance_text,
                    CAST(distance_value AS INT),
                    duration_text,
                    CAST(duration_value AS INT),
                    duration_in_traffic_text, 
                    CAST(duration_in_traffic_value AS INT)
                FROM t_gmap01_stagings
                WHERE "timestamp" IS NOT NULL;
                """
    
    inssql3 = """
                INSERT INTO t_w01_live(
                     loc_name, 
                     loc_id, 
                     clouds, 
                     detailed_status, 
                     dewpoint, 
                     heat_index, 
                     humidex, 
                     humidity, 
                     press, 
                     sea_level, 
                     rain_1h, 
                     rain_3h, 
                     reference_time, 
                     snow_1h, 
                     snow_3h, 
                     status, 
                     sunrise_time, 
                     sunset_time, 
                     temperature, 
                     temperature_kf, 
                     temperature_max, 
                     temperature_min, 
                     visibility_distance, 
                     weather_code, 
                     weather_iconname, 
                     wind_deg, 
                     wind_gust, 
                     wind_speed, 
                     reception_time) 
                SELECT DISTINCT
                        location_name,
                        CAST(location_id AS BIGINT), 
                        CASE WHEN weather_clouds = ' ' THEN NULL ELSE CAST(weather_clouds AS INT) END, 
                        weather_detailed_status, 
                        weather_dewpoint, 
                        weather_heat_index, 
                        weather_humidex, 
                        CASE WHEN weather_humidity = ' ' THEN NULL ELSE CAST(weather_humidity AS INT) END,
                        CASE WHEN weather_pressure_press = ' ' THEN NULL ELSE CAST(weather_pressure_press AS INT) END, 
                        CASE WHEN weather_pressure_sea_level = ' ' THEN NULL ELSE CAST(weather_pressure_sea_level AS INT) END,
                        CASE WHEN weather_rain_1h = '' THEN NULL ELSE CAST(weather_rain_1h AS FLOAT) END,
                        CASE WHEN weather_rain_3h = '' THEN NULL ELSE CAST(weather_rain_3h AS FLOAT) END,
                        TIMESTAMP 'epoch' + weather_reference_time/1 * INTERVAL '1 second', 
                        CASE WHEN weather_snow_1h = '' THEN NULL ELSE CAST(weather_snow_1h AS FLOAT) END,
                        CASE WHEN weather_snow_3h = '' THEN NULL ELSE CAST(weather_snow_3h AS FLOAT) END,
                        weather_status, 
                        TIMESTAMP 'epoch' + weather_sunrise_time/1 * INTERVAL '1 second',
                        TIMESTAMP 'epoch' + weather_sunset_time/1 * INTERVAL '1 second', 
                        CASE WHEN weather_temperature_temp = '' THEN NULL ELSE CAST(weather_temperature_temp AS FLOAT) END,
                        CASE WHEN weather_temperature_temp_kf = '' THEN NULL ELSE CAST(weather_temperature_temp_kf AS FLOAT) END, 
                        CASE WHEN weather_temperature_temp_max = '' THEN NULL ELSE CAST(weather_temperature_temp_max AS FLOAT) END, 
                        CASE WHEN weather_temperature_temp_min = '' THEN NULL ELSE CAST(weather_temperature_temp_min AS FLOAT) END, 
                        CASE WHEN weather_visibility_distance = ' ' THEN NULL ELSE CAST(weather_visibility_distance AS INT) END, 
                        CASE WHEN weather_weather_code = ' ' THEN 0 ELSE CAST(weather_weather_code AS INT) END, 
                        weather_weather_icon_name, 
                        CASE WHEN weather_wind_deg = '' THEN NULL ELSE CAST(weather_wind_deg AS FLOAT) END, 
                        CASE WHEN weather_wind_gust = '' THEN NULL ELSE CAST(weather_wind_gust AS FLOAT) END,
                        CASE WHEN weather_wind_speed = '' THEN NULL ELSE CAST(weather_wind_speed AS FLOAT) END, 
                        TIMESTAMP 'epoch' + reception_time/1 * INTERVAL '1 second'
                        FROM t_w01_stagings;
                """
    
    delsql_list = [delsql1, delsql2, delsql3]
    inssql_list = [inssql1, inssql2, inssql3]