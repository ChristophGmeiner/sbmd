import configparser
import psycopg2
from CreateTables import CreateTables


def drop_tables(cur, conn):
    '''Drops all tables in DWH
       INPUT:
           cur: A DB cursor object(has to be AWS Redshift based)
           conn: A connection object to an AWS Redsahift cluster
    '''
    for t, query in zip(CreateTables.tabs, CreateTables.droptable_queries):
        cur.execute(query)
        print("{} dropped!".format(t))
        conn.commit()


def create_tables(cur, conn):
    '''Creates all necessary tables in DWH
       INPUT:
           cur: A DB cursor object(has to be AWS Redshift based)
           conn: A connection object to an AWS Redsahift cluster
    '''
    for t, query in zip(CreateTables.tabs, CreateTables.createtable_queries):
        cur.execute(query)
        print("{} created!".format(t))
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    host = config["Red"]["DWH_HOST"]
    dbn = config["Red"]["DWH_DB"]
    user = config["Red"]["DWH_DB_USER"]
    pw = config["Red"]["DWH_DB_PASSWORD"]
    port = config["Red"]["DWH_PORT"]
    conn = psycopg2.connect(f"host={host}\
                            dbname={dbn} \
                            user={user} password={pw} port={port}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()