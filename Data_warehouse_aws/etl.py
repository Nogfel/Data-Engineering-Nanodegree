import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_HOST=config.get('CLUSTER', 'HOST')
DWH_DB_NAME=config.get('CLUSTER', 'DB_NAME')
DB_UDWH_SER=config.get('CLUSTER', 'DB_USER')
DB_PASSWDWH_ORD=config.get('CLUSTER', 'DB_PASSWORD')
DWH_DB_PORT=int(config.get('CLUSTER', 'DB_PORT'))

def load_staging_tables(cur, conn):
    '''Execute the queries related to load data into the staging tables'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''Execute the queries related to load data into tables used for analysis puposes'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_HOST,DWH_DB_NAME, DB_UDWH_SER, DB_PASSWDWH_ORD, DWH_DB_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()