class CreateModelTables:

    dropsql = "DROP TABLE IF EXISTS "
    createsql = "CREATE TABLE IF NOT EXISTS "
    inssql = "INSERT INTO "
      
    table1 = "t_w02_weather_codes'
    table2 = "t_dim05_weather"
    
    tabs = [table1, table2]
    
    drop_table1 = dropsql + table1 + " CASCADE; "
    drop_table2 = dropsql + table2 + " CASCADE; "

    create_table1 = createsql + table1 + """ (
                    weather_code INT NOT NULL SORTKEY,
                    status VARCHAR NOT NULL,
                    detailed_status VARCHAR NOT NULL)
                    DISTSTYLE ALL;
                    """
    
    create_table2 = createsql + table2 + """ (
                        w_id INT IDENTITY(0, 1) SORTKEY DISTKEY,
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
   
    
    createtable_queries = [create_table1]
    droptable_queries = [drop_table1]




