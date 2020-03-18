class CreatezzTables:

    dropsql = "DROP TABLE IF EXISTS "
    createsql = "CREATE TABLE IF NOT EXISTS "
    inssql = "INSERT INTO "
    truncsql = "TRUNCATE TABLE "
      
    table1 = "zz_w_cities"
    table2 = "zz_gmap_cities"
    table3 = "zz_db_cities"
       
    tabs = [table1, table2, table3]
    
    drop_table1 = dropsql + table1 + " CASCADE; "
    drop_table2 = dropsql + table2 + " CASCADE; "
    drop_table3 = dropsql + table3 + " CASCADE;"
    
    trunc_table1 = truncsql + table1 + ";"
    trunc_table2 = truncsql + table2 + ";"
    trunc_table3 = truncsql + table3 + ";"
        
    create_table1 = createsql + table1 + """ (
                    start_loc VARCHAR NOT NULL SORTKEY,
                    city_name VARCHAR NOT NULL)
                    DISTSTYLE ALL;
                    """
    insert_table1 = inssql + table1 + """ 
                    SELECT DISTINCT
                    start_loc,
                    SUBSTRING(start_loc, 1, POSITION('-' IN start_loc) - 1)
                    FROM
                    t_gmap01_live
                    WHERE start_loc LIKE '%-%'
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    start_loc
                    FROM
                    t_gmap01_live
                    WHERE start_loc NOT LIKE '%-%';
                    
                    DELETE FROM zz_w_cities
                    WHERE city_name IN (
                    'Wessling',
                    'Unterfoehring',
                    'Unterfohring'
                    'Tuerkenfeld',
                    'Turkenfeld'
                    'Schoengeising',
                    'Schongeising',
                    'Roehrmoos',
                    'Rohrmoos',
                    'Oberschleissheim'
                    'Muehldorf am Inn',
                    'Muhldorf am Inn',
                    'Groebenzell',
                    'Grobenzell',
                    'Graefelfing',
                    'Grafelfing',
                    'Fuerstenfeldbruck',
                    'Furstenfeldbruck',
                    'Eichstaett',
                    'Eichstatt',
                    'Donauwoerth',
                    'Donauworth',
                    'Altomuenster'
                    'Altomunster'
                    );

                    UPDATE zz_w_cities
                    SET city_name = 'Unterschleißheim'
                    WHERE city_name = 'Unterschleissheim';

                    UPDATE zz_w_cities
                    SET city_name = 'München'
                    WHERE city_name = 'Munich';

                    UPDATE zz_w_cities
                    SET city_name = 'Nürnberg'
                    WHERE city_name = 'Nuremberg';

                    UPDATE zz_w_cities
                    SET city_name = 'Kempten'
                    WHERE city_name LIKE 'Kempten%';
                    """
    
    create_table2 = createsql + table2 + """ (
                    start_loc VARCHAR NOT NULL SORTKEY,
                    city_name VARCHAR NOT NULL)
                    DISTSTYLE ALL;
                    """
    
    insert_table2 = inssql + table2 + """ 
                    INSERT INTO zz_w_cities
                    SELECT DISTINCT
                    loc_name,
                    loc_name
                    FROM t_w01_live
                    WHERE loc_name NOT LIKE 'Landkre%'
                    ORDER BY 1;
                    """
    
    create_table3 = createsql + table3 + """ (
                    start_loc,
                    start_loc
                    FROM 
                    t_db01_live
                    WHERE start_loc NOT LIKE '%-%' AND start_loc NOT LIKE '% %' AND start_LOC IS NOT NULL
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    SUBSTRING(start_loc, CHARINDEX(', ', start_loc) + 1, 20)
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE '%, %'
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    SUBSTRING(start_loc, 1, CHARINDEX('-', start_loc) - 1)
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE '%-%' AND start_loc NOT LIKE 'Garmisch%' 
                    AND start_loc NOT IN (SELECT start_loc FROM zz_db_cities)
                    UNION 
                    SELECT DISTINCT
                    start_loc,
                    'München'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'München %' 
                    UNION 
                    SELECT DISTINCT
                    start_loc,
                    'Nürnberg'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Nürnberg %' 
                    UNION 
                    SELECT DISTINCT
                    start_loc,
                    'Augsburg'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Augsburg %' 
                    UNION 
                    SELECT DISTINCT
                    start_loc,
                    'Ingolstadt'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Ingolstadt %' 
                    UNION 
                    SELECT DISTINCT
                    start_loc,
                    'Bad Aibling'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Bad Aibling%' 
                    UNION 
                    SELECT DISTINCT
                    start_loc,
                    'Dachau'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Dachau %' 
                    UNION 
                    SELECT DISTINCT
                    start_loc,
                    'Deggendorf'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Deggendorf %' 
                    UNION 
                    SELECT DISTINCT
                    start_loc,
                    'Garmisch-Partenkirchen'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Garmisch%' 
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    'Eichstätt'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Eichstätt%' 
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    'Grafing'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Grafing %' 
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    'Murnau'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Murnau %' 
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    'Rosenheim'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Rosenheim %'
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    'Starnberg'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Starnberg %' 
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    'Wolfratshausen'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE '%b Wolfratshausen%' 
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    'Markt Schwaben'
                    FROM 
                    t_db01_live
                    WHERE start_loc = 'Markt Schwaben'
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    'Mühldorf am Inn'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE '%Mühldorf am Inn %'
                    UNION
                    SELECT DISTINCT
                    start_loc,
                    'Steinebach'
                    FROM 
                    t_db01_live
                    WHERE start_loc LIKE 'Steinebach %' 
                    """
    
    createtable_queries = [create_table1, create_table2,
                           create_table2]
    droptable_queries = [drop_table1, drop_table2,
                         drop_table3]
    ins_queries = [insert_table1, insert_table2,
                   insert_table3]
    trunc_queries = [trunc_table1, trunc_table2,
                     trunc_table3]




