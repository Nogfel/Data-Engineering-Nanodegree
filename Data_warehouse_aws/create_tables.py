import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_HOST=config.get('CLUSTER', 'HOST')
DWH_DB_NAME=config.get('CLUSTER', 'DB_NAME')
DB_UDWH_SER=config.get('CLUSTER', 'DB_USER')
DB_PASSWDWH_ORD=config.get('CLUSTER', 'DB_PASSWORD')
DWH_DB_PORT=int(config.get('CLUSTER', 'DB_PORT'))


def drop_tables(cur, conn):
    '''Drop tables in case they exist'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''Create tables in the database'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_HOST,DWH_DB_NAME, DB_UDWH_SER, DB_PASSWDWH_ORD, DWH_DB_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()