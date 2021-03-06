import psycopg2
import configparser
from InsertTables import delsql_list, inssql_list
import logging

def del_tables(cur, conn):
    '''drops all necessary tables before creating new ones, based on 
    sql_queries.py module
    INPUT:
    cur: A postgres / psycopg2 cursor object for fulfilling the drop tasks
    conn: A postgres / psycopg2 connection concerning the cur
    '''
    for query in delsql_list:
        cur.execute(query)
        conn.commit()
        
def ins_tables(cur, conn):
    '''creates all necessary tables, based on sql_queries.py module
    INPUT:
    cur: A postgres / psycopg2 cursor object for fulfilling the creation tasks
    conn: A postgres / psycopg2 connection concerning the cur
    '''
    for query in inssql_list:
        cur.execute(query)
        conn.commit()


def main(cfg = "/home/ubuntu/dwh.cfg"):
    
    config = configparser.ConfigParser()
    config.read(cfg)
    
    rdspw = config["RDS"]["PW"]
    
    conn = psycopg2.connect(
        host="sbmd.cfv4eklkdk8x.eu-central-1.rds.amazonaws.com", 
        dbname="sbmd1", port=5432, user="sbmdmaster", password=rdspw)
    
    cur = conn.cursor()
    
    try:
    
        del_tables(cur, conn)
        logging.info("Succesfull deleted staged keys!")
        
        ins_tables(cur, conn)
        logging.info("Succesfull inserted staged keys!")
    
        conn.close()
        
    except Exception as e:
        logging.error(e)
        logging.error("Staging to live transfer failed!")


if __name__ == "__main__":
    main()