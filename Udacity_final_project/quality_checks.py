checks = ("""
            SELECT
                {id},
                COUNT({id})
            FROM {table}
            GROUP BY 1
            ORDER BY 2 DESC
""")
          
table_to_check = {
    "dim_country":"id",
    "dim_state":"id",
    "dim_modal":"id_modal",
    "dim_visa_motive":"visa_motive_id",
    "dim_port":"port_modal_id",
    "dim_imigrant":"imigrant_id",
    "fact_imigration":"id"

}

def connect_redshift():
    import psycopg2
    import configparser

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_HOST=config.get('CLUSTER', 'HOST')
    DWH_DB_NAME=config.get('CLUSTER', 'DB_NAME')
    DB_UDWH_SER=config.get('CLUSTER', 'DB_USER')
    DB_PASSWDWH_ORD=config.get('CLUSTER', 'DB_PASSWORD')
    DWH_DB_PORT=int(config.get('CLUSTER', 'DB_PORT'))
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_HOST,DWH_DB_NAME, DB_UDWH_SER, DB_PASSWDWH_ORD, DWH_DB_PORT))
    conn.autocommit = True
    cur = conn.cursor()

    return cur

def run_checks(cur, query, table, id):
    cur.execute(query)
    response = cur.fetchall()

    for item in response:
        duplicates = item[1]
        break

    if duplicates == 1:
        print('Table {table} has no duplicates on primary key column ({id}).'.format(table=table, id=id))
    else:
        raise KeyError("There are duplicates values on primary key {id} column for {table} table".format(id=id, table=table))


def main():
    for table, id in table_to_check.items():
        run_checks(
            cur=connect_redshift(),
            query=checks.format(id=id, table=table),
            table=table,
            id=id
        )

if __name__ == "__main__":
    main()
