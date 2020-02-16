delsql1 = """
          DELETE FROM t_db01_live
            WHERE departure || '_' || start_loc || '_' || end_loc
             IN
             (SELECT DISTINCT
             TO_TIMESTAMP(departure, 'HH24:MI')::TIME || '_' || "start" || '_' || "end"
             FROM t_db01_stagings)
          """

delsql2 = """
            DELETE FROM t_gmap01_live
            WHERE "timestamp" || '_' || start_loc || '_' || end_loc
            IN
                (SELECT DISTINCT
                    "timestamp" || '_' || stat1 || '_' || stat2
                FROM t_gmap01_stagings)
          """

delsql3 = """
          DELETE FROM t_w01_live
          WHERE reception_time || '_' || loc_name IN
              (SELECT DISTINCT 
                 reception_time || '_' || location_name
          FROM
          t_w01_stagings)
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
            TO_TIMESTAMP(departure, 'HH24:MI')::TIME AS departure,
            TO_TIMESTAMP(arrival, 'HH24:MI')::TIME AS arrival,
            transfers,
            TO_TIMESTAMP(arrival, 'HH24:MI')::TIME - TO_TIMESTAMP(departure, 'HH24:MI')::TIME,
            products,
            CAST(price AS FLOAT),
            ontime,
            canceled,
            total_delay,
            delay_delay_departure,
            delay_delay_arrival,
            CAST("date" AS DATE)
        FROM
        t_db01_stagings
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
            distance_traffic_text, 
            distance_traffic_value)
        SELECT DISTINCT
            stat1,
            stat2,
            CAST("timestamp" AS TIMESTAMP),
            start_location_lat,
            start_location_lng,
            start_address,
            end_location_lat,
            end_location_lng,
            end_address,
            distance_text,
            distance_value,
            duration_text,
            duration_value,
            duration_in_traffic_text, 
            duration_in_traffic_value
        FROM t_gmap01_stagings
        WHERE "timestamp" IS NOT NULL
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
            SELECT
                location_name,
                location_id, 
                weather_clouds, 
                weather_detailed_status, 
                weather_dewpoint, 
                weather_heat_index, 
                weather_humidex, 
                weather_humidity, 
                weather_pressure_press, 
                weather_pressure_sea_level, 
                weather_rain_1h, 
                weather_rain_3h, 
                weather_reference_time, 
                weather_snow_1h, 
                weather_snow_3h, 
                weather_status, 
                weather_sunrise_time, 
                weather_sunset_time, 
                weather_temperature_temp, 
                weather_temperature_temp_kf, 
                weather_temperature_temp_max, 
                weather_temperature_temp_min, 
                weather_visibility_distance, 
                weather_weather_code, 
                weather_weather_icon_name, 
                weather_wind_deg, 
                weather_wind_gust, 
                weather_wind_speed, 
                reception_time
                FROM t_w01_stagings
            """

delsql_list = [delsql1, delsql2, delsql3]
inssql_list = [inssql1, inssql2, inssql3]