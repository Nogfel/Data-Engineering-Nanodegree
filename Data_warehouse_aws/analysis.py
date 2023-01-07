import configparser
import psycopg2
from sql_queries import analytical_queries


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_HOST=config.get('CLUSTER', 'HOST')
DWH_DB_NAME=config.get('CLUSTER', 'DB_NAME')
DB_UDWH_SER=config.get('CLUSTER', 'DB_USER')
DB_PASSWDWH_ORD=config.get('CLUSTER', 'DB_PASSWORD')
DWH_DB_PORT=int(config.get('CLUSTER', 'DB_PORT'))


def analysis(cur, conn):
    for query in analytical_queries:
        cur.execute(query)
        data = cur.fetchall()
        for row in data:
            print(row)
            
        # conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_HOST,DWH_DB_NAME, DB_UDWH_SER, DB_PASSWDWH_ORD, DWH_DB_PORT))
    cur = conn.cursor()
    
    analysis(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()