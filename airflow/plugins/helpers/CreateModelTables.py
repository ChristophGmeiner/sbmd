class CreateModelTables:

    dropsql = "DROP TABLE IF EXISTS "
    createsql = "CREATE TABLE IF NOT EXISTS "
    inssql = "INSERT INTO "
    truncsql = "TRUNCATE TABLE "
      
    table1 = "t_w02_weather_codes"
    table2 = "t05_dim_weather"
    table3 = "t04_dim_part"
    
    tabs = [table1, table2, table3]
    
    drop_table1 = dropsql + table1 + " CASCADE; "
    drop_table2 = dropsql + table2 + " CASCADE; "
    drop_table3 = dropsql + table3 + " CASCADE; "

    create_table1 = createsql + table1 + """ (
                    weather_code INT NOT NULL SORTKEY,
                    status VARCHAR NOT NULL,
                    detailed_status VARCHAR NOT NULL)
                    DISTSTYLE ALL;
                    """
    
    insert_table1 = inssql + table1 + """ 
                    (weather_code,
                    status,
                    detailed_status)
                    SELECT DISTINCT
                    weather_code,
                    status,
                    detailed_status
                    FROM
                    t_w01_live;
                    """
    
    create_table2 = createsql + table2 + """ (
                        w_id INT SORTKEY DISTKEY,
                        loc_name VARCHAR,
                        city VARCHAR,
                        dept_hour TIMESTAMP,
                        sunrise_time TIMESTAMP,
                        sunset_time TIMESTAMP,
                        clouds INT,
                        rain_1h FLOAT,
                        rain_3h FLOAT,
                        snow_1h FLOAT,
                        snow_3h FLOAT,
                        wind_deg FLOAT,
                        wind_gust FLOAT,
                        wind_speed FLOAT,
                        humidity INT,
                        press INT,
                        sea_level INT,
                        temperature FLOAT,
                        temperature_kf FLOAT,
                        temperature_min FLOAT,
                        temperature_max FLOAT,
                        visibility_distance FLOAT,
                        weather_status VARCHAR,
                        weather_status_detail VARCHAR);
                    """

    insert_table2 = inssql + table2 + """
                    (w_id,
                    loc_name,
                    city, 
                    dept_hour,
                    sunrise_time,
                    sunset_time,
                    clouds, 
                    rain_1h, 
                    rain_3h,
                    snow_1h,
                    snow_3h,
                    wind_deg,
                    wind_gust,
                    wind_speed,
                    humidity,
                    press,
                    sea_level,
                    temperature,
                    temperature_kf,
                    temperature_min,
                    temperature_max,
                    visibility_distance,
                    weather_status,
                    weather_status_detail)
                    SELECT 
                    v.weather_id,
                    v.loc_name,
                    v.city, 
                    TO_TIMESTAMP(v.dept_hour, 'YYYY-MM-DD HH'),
                    v.sunrise_time,
                    v.sunset_time,
                    v.clouds, 
                    v.rain_1h, 
                    v.rain_3h,
                    v.snow_1h,
                    v.snow_3h,
                    v.wind_deg,
                    v.wind_gust,
                    v.wind_speed,
                    v.humidity,
                    v.press,
                    v.sea_level,
                    v.temperature,
                    v.temperature_kf,
                    v.temperature_min,
                    v.temperature_max,
                    v.visibility_distance,
                    w.status,
                    w.detailed_status
                    FROM
                    v02_weather_dim v
                    LEFT JOIN t_w02_weather_codes w
                    ON w.weather_code = v.weather_code
                    WHERE v.weather_id > (
                        SELECT MAX(w_id) FROM t05_dim_weather)
                    ;
                    """
       
    trunc_table3 = truncsql + table3 + ";"
        
    create_table3 = createsql + table3 + """ (
                    part_id INT NOT NULL SORTKEY,
                    part_name VARCHAR NOT NULL);
                    """
    insert_table3 = inssql + table3 + """ 
                    (
                    part_id, 
                    part_name)
                    VALUES(
                    2, 
                    'gmap');
                    """
                    
    insert_table3b = inssql + table3 + """ 
                    (
                    part_id, 
                    part_name)
                    VALUES(
                    1, 
                    'train');
                    """
    
    createtable_queries = [create_table1, create_table2,
                           create_table3]
    droptable_queries = [drop_table1, drop_table2,
                         drop_table3]
    
    wc_queries = [drop_table1, create_table1, insert_table1]
    wc_queries = " ".join(wc_queries)
    
    t05_queries = [drop_table2, create_table2, insert_table2]
    t05_queries = " ".join(t05_queries)
    





