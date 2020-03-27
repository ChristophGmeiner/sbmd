class CreateViews:

    dropsql = "DROP VIEW IF EXISTS "
    createsql = "CREATE VIEW IF NOT EXISTS "
      
    view1 = "v01_gmap_fact"
    view2 = "v02_weather_dim"
    view3 = "v03_train_fact"
    
    views = [view1, view2, view3]
    
    drop_view1 = dropsql + view1 + ";"
    drop_view2 = dropsql + view2 + ";"
    drop_view3 = dropsql + view3 + ";"
    
    create_view1 = createsql + view1 + """ 
                     AS
                    SELECT 
                    g.start_loc || '_' || g.end_loc AS conn_detail,
                    TO_CHAR("timestamp"::DATETIME, 'yyyy-mm-dd HH24') AS dept_hour,
                    2 AS part,
                    AVG(duration_value) AS reg_duration_sec,
                    AVG(duration_traffic_value) AS real_duration_sec,
                    AVG(duration_traffic_value) - AVG(duration_value) AS delay_sec
                    FROM t_gmap01_live g
                    GROUP BY 
                    g.start_loc || '_' || g.end_loc,
                    TO_CHAR("timestamp"::DATETIME, 'yyyy-mm-dd HH24');
                    """
    
    create_view2 = createsql + view2 + """
                    AS
                    SELECT 
                    AVG(w.weather_id) AS weather_id,
                    w.loc_name,
                    c.city_name AS city,
                    TO_CHAR(w.reference_time::DATETIME, 'yyyy-mm-dd HH24') AS dept_hour,
                    MIN(w.sunrise_time) AS sunrise_time,
                    MAX(w.sunset_time) AS sunset_time,
                    AVG(w.clouds) AS clouds,
                    AVG(w.rain_1h) AS rain_1h,
                    AVG(w.rain_3h) AS rain_3h,
                    AVG(w.snow_1h) AS snow_1h,
                    AVG(w.snow_3h) AS snow_3h,
                    AVG(w.wind_deg) AS wind_deg,
                    AVG(w.wind_gust) AS wind_gust,
                    AVG(wind_speed) AS wind_speed,
                    AVG(w.humidity) AS humidity,
                    AVG(w.press) AS press,
                    AVG(w.sea_level) AS sea_level,
                    ROUND(AVG(w.temperature), 2) AS temperature,
                    ROUND(AVG(w.temperature_kf), 2) AS temperature_kf,
                    ROUND(MIN(w.temperature_min), 2) AS temperature_min,
                    ROUND(MAX(w.temperature_max), 2) AS temperature_max,
                    AVG(w.visibility_distance) AS visibility_distance,
                    ROUND(AVG(weather_code), 0) AS weather_code
                    FROM t_w01_live w
                    LEFT JOIN zz_w_cities c
                    ON 
                    c.start_loc = w.loc_name
                    GROUP BY 
                    w.loc_name,
                    c.city_name,
                    TO_CHAR(w.reference_time::DATETIME, 'yyyy-mm-dd HH24');
                    """
    
    create_view3 = createsql + view3 + """ 
                        AS
                        SELECT
                        1 AS part_id, 
                        db.start_loc || '_' || db.end_loc AS conn_detail_name,
                        TO_CHAR(db.departure, 'YYYY-MM-DD HH24') AS dept_hour,
                        w.weather_id AS w_id,
                        AVG(total_time) AS duration_sec,
                        AVG(total_delay) * 60 AS delay_sec
                        FROM
                        t_db01_live db
                        LEFT JOIN zz_db_cities cs
                        ON cs.start_loc = db.start_loc
                        LEFT JOIN v02_weather_dim w
                        ON w.city = cs.city_name
                        AND w.dept_hour = TO_CHAR(db.departure, 'YYYY-MM-DD HH24') 
                        GROUP BY
                        db.start_loc || '_' || db.end_loc,
                        TO_CHAR(db.departure, 'YYYY-MM-DD HH24'),
                        w.weather_id;
                    """
    
    createview_queries = [create_view1, create_view2]
    dropview_queries = [drop_view1, drop_view2]




