dropsql = "DROP TABLE IF EXISTS "
createsql = "CREATE TABLE IF NOT EXISTS "
createuniqueindsql = "CREATE UNIQUE INDEX "

table1 = "t_db01_live"
table2 = "t_gmap01_live"
table3 = "t_w01_live"

drop_table1 = dropsql + table1
drop_table2 = dropsql + table2
drop_table3 = dropsql + table3

create_table1 = createsql + table1 + " (db_id SERIAL PRIMARY KEY, \
                start_loc VARCHAR NOT NULL, end_loc VARCHAR NOT NULL, \
                timestamp TIMESTAMP NOT NULL, departure TIME NOT NULL, \
                arrival TIME NOT NULL, transfers INT, \
                total_time INTERVAL NOT NULL, products VARCHAR, \
                price FLOAT, ontime BOOLEAN NOT NULL, \
                cancelled BOOLEAN NOT NULL, total_delay INT NOT NULL, \
                delay_departure INT NOT NULL, \
                delay_arrival INT NOT NULL, dep_date DATE)"
create_table1_index = createuniqueindsql + table1 + "_Index ON " + table1 + \
                      " (dep_date, departure, start_loc, end_loc)"

create_table2 = createsql + table2 + " (gmap_id SERIAL PRIMARY KEY, \
                start_loc VARCHAR NOT NULL, end_loc VARCHAR NOT NULL, \
                timestamp TIMESTAMP NOT NULL, start_loc_lat FLOAT, \
                start_loc_lng FLOAT, start_adress VARCHAR, end_loc_lat FLOAT, \
                end_loc_lng FLOAT, end_adress VARCHAR, distance_text VARCHAR, \
                distance_value FLOAT, duration_text VARCHAR, \
                duration_value FLOAT, distance_traffic_text VARCHAR, \
                distance_traffic_value FLOAT)"
create_table2_index = createuniqueindsql + table2 + "_Index ON " + table2 + \
                      " (timestamp, start_loc, end_loc)"


create_table3 = createsql + table3 + " (weather_id SERIAL PRIMARY KEY, \
               loc_name VARCHAR NOT NULL, loc_id FLOAT NULL, \
               clouds FLOAT, detailed_status VARCHAR, dewpoint VARCHAR, \
               heat_index VARCHAR, humidex VARCHAR, humidity FLOAT, \
               press FLOAT, sea_level VARCHAR, rain_1h FLOAT, rain_3h FLOAT, \
               reference_time FLOAT NOT NULL, snow_1h FLOAT, snow_3h FLOAT, \
               status VARCHAR, sunrise_time FLOAT, sunset_time FLOAT, \
               temperature FLOAT, temperature_kf FLOAT, \
               temperature_max FLOAT, temperature_min FLOAT, \
               visibility_distance FLOAT, weather_code INT, \
               weather_iconname VARCHAR, wind_deg FLOAT, wind_gust FLOAT, \
               wind_speed FLOAT, reception_time FLOAT NOT NULL)"
create_table3_index = createuniqueindsql + table3 + "_Index ON " + table3 + \
                      " (reception_time, loc_name)"

createtable_queries = [create_table1, create_table1_index,
                       create_table2, create_table2_index, 
                       create_table3, create_table3_index]
droptable_queries = [drop_table1, drop_table2, drop_table3]


