class DataModel:

    delsql1 = """
            DROP TABLE IF EXISTS t03_dim_conn;
            CREATE TABLE t03_dim_conn (
            conn_id INT IDENTITY(0, 1) PRIMARY KEY SORTKEY,
            conn_detail_name VARCHAR NOT NULL,
            stat1 VARCHAR NOT NULL,
            stat2 VARCHAR NOT NULL,
            conn_city_name VARCHAR NOT NULL,
            stat1_city VARCHAR NOT NULL,
            stat2_city VARCHAR NOT NULL)
            DISTSTYLE ALL;
            """
    
    inssql1 = """
                INSERT INTO t03_dim_conn (
                conn_detail_name,
                stat1,
                stat2,
                conn_city_name,
                stat1_city,
                stat2_city)
                SELECT DISTINCT
                vt.conn_detail_name,
                substring(vt.conn_detail_name, 1, CHARINDEX('_', vt.conn_detail_name) - 1) AS stat1,
                substring(vt.conn_detail_name, CHARINDEX('_', vt.conn_detail_name) + 1, 50) AS stat2,
                dbcs.city_name || '_' || dbce.city_name AS conn_city,
                dbcs.city_name AS stat1_city,
                dbce.city_name AS stat2_city
                FROM 
                v03_train_fact vt
                LEFT JOIN zz_db_cities dbcs
                on dbcs.start_loc = substring(vt.conn_detail_name, 1, CHARINDEX('_', vt.conn_detail_name) - 1)
                LEFT JOIN zz_db_cities dbce
                on dbce.start_loc = substring(vt.conn_detail_name, CHARINDEX('_', vt.conn_detail_name) + 1, 50)
                WHERE (dbcs.city_name || '_' || dbce.city_name) IS NOT NULL;
                """
    
    delsql2 = """
            DROP TABLE IF EXISTS t02_dim_time;
            CREATE TABLE t02_dim_time (
            time_id INT IDENTITY(0, 1) SORTKEY DISTKEY,
            time_key TIMESTAMP NOT NULL, 
            "year" INT NOT NULL,
            "month" INT NOT NULL,
            "week" INT NOT NULL,
            "day" INT NOT NULL,
            "weekday" INT NOT NULL,
            is_weekend BOOLEAN NOT NULL,
            "hour" INT NOT NULL);
            """
    
    inssql2 = """
            INSERT INTO t02_dim_time (
            time_key,
            "year",
            "month",
            "week",
            "day",
            "weekday",
            is_weekend,
            "hour")
            SELECT DISTINCT
            TO_TIMESTAMP(vt.dept_hour, 'YYYY-MM-DD HH24') AS time_key,
            EXTRACT(year FROM TO_TIMESTAMP(vt.dept_hour, 'YYYY-MM-DD HH24')) AS "year",
            EXTRACT(month FROM TO_TIMESTAMP(vt.dept_hour, 'YYYY-MM-DD HH24')) AS "month",
            EXTRACT(week FROM TO_TIMESTAMP(vt.dept_hour, 'YYYY-MM-DD HH24')) AS "week",
            EXTRACT(day FROM TO_TIMESTAMP(vt.dept_hour, 'YYYY-MM-DD HH24')) AS "day",
            EXTRACT(weekday FROM TO_TIMESTAMP(vt.dept_hour, 'YYYY-MM-DD HH24')) AS "weekday",
            CASE WHEN EXTRACT(weekday FROM TO_TIMESTAMP(vt.dept_hour, 'YYYY-MM-DD HH24')) IN (0, 6) THEN False ELSE True END AS is_weekend,
            EXTRACT(hour FROM TO_TIMESTAMP(vt.dept_hour, 'YYYY-MM-DD HH24')) AS "hour"
            FROM v03_train_fact vt;
            """
    
    delsql3 = """
            DROP TABLE IF EXISTS t01_delay_fact;
            CREATE TABLE t01_delay_fact (
            f_id INT IDENTITY(0, 1) SORTKEY,
            conn_id INT NOT NULL,
            time_id INT NOT NULL,
            part_id INT NOT NULL,
            w_id INT NOT NULL DISTKEY,
            duration_sec FLOAT NOT NULL,
            delay_sec FLOAT NOT NULL);
                """
    
    inssql3 = """
                INSERT INTO t01_delay_fact (
                conn_id,
                time_id,
                part_id,
                w_id,
                duration_sec,
                delay_sec)
                SELECT 
                dc.conn_id,
                dt.time_id,
                vt.part_id,
                vt.w_id,
                vt.duration_sec,
                vt.delay_sec
                FROM
                v03_train_fact vt
                LEFT JOIN
                t03_dim_conn dc
                ON dc.conn_detail_name = vt.conn_detail_name
                LEFT JOIN t02_dim_time dt
                ON dt.time_key = TO_TIMESTAMP(vt.dept_hour, 'YYYY-MM-DD HH24')
                WHERE vt.w_id IS NOT NULL
                AND dc.conn_id IS NOT NULL;
    """
    
    datamodelc = [delsql1, inssql1, delsql2, inssql2, delsql3, inssql3]
    comm = " ".join(datamodelc)
